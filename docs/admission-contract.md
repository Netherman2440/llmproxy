# Admission: credential ID nie jest billing ownerem

## Obecny kontrakt

To opis **istniejącej semantyki**, nie włączenie nowych limitów konta.

| Polityka | Klucz bucketu | Zakres |
|---|---|---|
| `[backends.NAME.rate_limit]` rpm/concurrency | API-key record ID + model alias | konkretny credential/model |
| `[rate_limit]` rpm/concurrency | API-key record ID | ten credential przez wszystkie modele bez override |
| `parsing.per_user` | API-key record ID | aktywny body parser; nazwa historyczna |
| `parsing.concurrency` | wspólny parser bucket | wszystkie credentials, tylko faza parse |

Pierwszeństwo: model → global policy.0wyłącza dany limit, brak wartości przechodzi
do globalnej. Tylko `rpm` i `concurrency` są zaimplementowane. Tabele odrzucają
unknown options, w tym `scope="account"`, `group_by_owner` oraz `prompt_tpd`.
`parsing` przyjmuje wyłącznie concurrency/per_user/timeout. Nieobsługiwane daily
quotas nie mogą przejść walidacji i udawać działającego zabezpieczenia.

Mongo auth zwraca `user['id'] = cgc.api_keys._id`; `_user_id` jest ownerem z
`rest_users`, a `_namespace` i `_org_id` to metadata ownera. SQLite używa odrębnego
UUID rekordu `api_key`. Dwa credentials jednego ownera mają **niezależne** RPM i
model-concurrency. Zmiana comment nie resetuje bucketu tego samego credential.
Billing nadal zapisuje właściwy historyczny owner/account: grouping finansowy
nie implikuje grouping admission. Nie zmieniono cen, taryf ani integer sample STT.

## Local a shared

- `admission.mode="local"` (default): counters jednego procesu. Restart resetuje
  RPM, SIGHUP nie gubi owned concurrency slots. Kilka niezależnych local replicas
  nie stanowi jednego deployment-wide limitu.
- Jawny `admission.mode="shared"`: te same DB/scope/policy, database leases z
  finite TTL, renew/watchdog i server time. Brak DB/utrata lease nie przełącza
  do local. Transient renew I/O może retry tylko w już potwierdzonym oknie:
  niepewna próba nie wydłuża watchdog, false ownership odcina natychmiast;
  acquire bez proof nadal failuje closed. Scope identyfikuje współpracującą usługę, **nie scala credentials
  tego samego account**. Restart repliki nie resetuje wspólnego okna RPM.
- Parser-global cap obejmuje tylko parse/admission body, nie wszystkie trwające
  backendy. Nie ma jednego globalnego backend capu dla wszystkich credentials.
  Backend/STT nadal potrzebuje własnych bezpiecznych global/GPU gates.
- File-conversion poll ma oddzielny bound, nie nalicza nowego RPM/model slotu.
  W background conversion model concurrency obejmuje upload, nie cały queued job.
- Realtime RPM dotyczy handshake; slot trwa przez sesję i krótki drain. Powyższa
  tożsamość credential i model/global precedence obowiązuje także WebSockety.

## Konsekwencje operacyjne i konfiguracja historyczna

Nie traktuj per-key limitu jako zabezpieczenia sumy ruchu jednego billing ownera.
Wydanie wielu kluczy może zwiększyć łączny ruch tego ownera. Admission nie jest
sprawdzeniem budżetu finansowego konta ani zagregowanym SLA/licznikiem osób.
Metryka `user` ma pseudonim credential: nie licz jej jako unikalnych ludzi.

Jeśli potrzebne są limity per-account/org, potrzebny jest jawny projekt biznesowy,
wiarygodny wspólny identyfikator, migracja policy/aktywnych leases oraz osobne
cross-key/cross-replica tests. Ten patch **nie** podejmuje tej decyzji za ownera.

Starszy config z ignorowanymi `scope`, `group_by_owner`, token-day lub innymi
nieobsługiwanymi polami teraz failuje startup/reload. To zamknięcie pozornej
konfiguracji, nie zmiana działających rpm/concurrency. Nie usuwaj po prostu
`scope="account"` i nie uruchamiaj traffic, jeżeli wymagasz account-wide ochrony:
aktualny runtime jej nie realizuje. SIGHUP przy błędnym nowym configu zachowuje
ostatnią poprawną konfigurację/owned slots; przejrzyj zamiar i uzyskaj świadomą
decyzję operatora. Poprawny istniejący per-key config działa bez zmiany wartości.

## Dowody

Conformance tests realnego resolvera/slot/parser: dwa credentials z tym samym
ownerem, global fallback przez modele, comment mutation bez resetu, parser cap
wspólny + per-key. Te testy **przeszły na dawnym zachowaniu**: nie nazywamy tego
RED→GREEN zmiany grouping. Osobny rzeczywisty RED wykazał przyjmowanie pozornych
account/daily options; minimalna walidacja je odrzuca, bez zmiany poprawnych
limitów. Shared two-process SQLite oraz real Mongo gates pozostają odrębnymi
proof points; dokumentacja nie zastępuje hosted CI ani wdrożenia wspólnego scope.
