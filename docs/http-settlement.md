# Rozliczenia HTTP/SSE i nieznane usage

Dotyczy Chat, Messages i Responses, także odpowiedzi JSON. Audio receipt-v1
pozostaje oddzielnym protokołem z integer samples/16000; ten journal nie
przelicza tokenów na czas ani nie zmienia taryf.

## Kolejność i granice gwarancji

1. Po auth/parser/admission, **przed upstream POST**, durable attempt przechowuje
   request UUID, historycznego ownera, backend alias/URL/device, endpoint i czas
   przyjęcia. Bez credential, promptu, transkryptu lub response body.
2. Backend usage musi dać dokładnie prompt/completion produktów wybranego modelu,
   z nieujemnymi całkowitymi tokenami. Proof i jego pierwszy observed_at są trwałe
   przed financial write. Conflicting proof/source/owner nie jest nadpisywany.
3. SQLite5: cały zestaw financial rows + marker w jednym BEGIN IMMEDIATE/FULL.
   Mongo: jeden stabilny BSON ObjectId, majority+journal, duplicate verify,
   a potem marker. Historyczna data finance to pierwszy observed usage, nie retry.
4. Dopiero po settlement wychodzi terminal SSE/EOF lub gotowy JSON. Partials są
   nadal streamowane, nie buforujemy całego outputu ani nie udajemy streamingu.
5. Confirmed proof odtwarza się bez browsera, klucza użytkownika, kontaktu z backendem
   lub ponownego requestu modelowego. Brak wcześniej billed finance to conflict,
   nie prawo do odtworzenia opłaty po cichu.

To **nie** jest dowód znajomości usage, którego backend nie zdążył przekazać
trwale. SIGKILL po partialach może pozostawić `prepared`; transport/usage error
bez proof pozostawia `unknown`. Obie sytuacje są nierozliczonym zobowiązaniem,
nie zerowym usage i nie automatycznym upoważnieniem do obciążenia.

Known upstream4xx jest `rejected` bez completed usage. Upstream5xx, nieznany
wynik połączenia lub malformed usage nie są automatycznie tym samym co rejection.
Late cleanup nie może nadpisać confirmed/billed. Nadal istnieje nieusuwalne okno
między zdalnym wykonaniem a lokalnym proof; automatyczne jego rozstrzygnięcie
wymaga durable receipt/query po stronie konkretnego providera. Nie obiecujemy
tego dla dowolnego backendu OpenAI-compatible.

## Restart, alarm i inspekcja

Istniejący bounded recovery worker sprawdza oba journale co5s. HTTP page≤100,
cycle deadline15s, fair cursor; osobny bounded existence query utrzymuje unhealthy
przy zobowiązaniu także poza bieżącą stroną. Confirmed rozlicza się raz;
prepared/unknown pozostają do operator reconciliation. Emitowany jest alarm
bez promptu/secret, a `app['http_attempt_recovery_healthy']` jest false dopóki są
retained obligations. Integracja flagi z konkretnym deploymentowym monitoringiem
nie jest automatycznym dowodem gotowego alertingu/DR.

```bash
# Tylko nazwa referencji. URI/hasło żyje w chronionym env, nigdy w argv/log/repo.
python -m llmproxy.attempts --db-env LLMPROXY_DB_URI
# Jawna operacja finansowa, wyłącznie dla już potwierdzonego durable proof:
python -m llmproxy.attempts --db-env LLMPROXY_DB_URI --recover-confirmed
```

Domyślnie CLI nie tworzy opłat; może zainicjalizować/migrować metadata DB i indeksy,
więc nie jest fizycznie read-only. Drukuje≤100IDs/state/model/product quantities,
bez owner credential/comment/body. Następną stronę wybiera `--after <last-id>`.
Nie ma flagi „uznaj unknown za zero/confirmed”. Nie kasuj wpisu, żeby wyzerować alarm.
Zgodny provider receipt/usage po request ID wymaga osobno zweryfikowanej ścieżki
operatora; brak takiego API oznacza dalsze unresolved, nie guessed backfill.

## Rollout i backup

SQLite5 jest add-only; zachowuje stare finance, receipt i conversion journals.
Stare binaria nie mogą otwierać v5. Mongo runtime role musi otrzymać actions dla
konkretnego `cgc.llmproxy_http_attempt` według `mongo-runtime-role.md` przed rolloutem.
Nie przyznawaj delete/update finance ani wildcard readWrite.

Backup musi obejmować finance, oba journale HTTP/conversion i receipt metadata
z jednego zgodnego punktu. Nie cofaj do starego snapshotu po nowym usage.
Test lokalnego spójnego backupu nie zastępuje off-volume restore/DR ani realnego
CGC consumer/pricing gate. Te same ograniczenia dotyczą zniszczenia obu ledgerów.
