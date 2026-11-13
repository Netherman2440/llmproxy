# Lokalny quickstart: SQLite i klucz przez referencję

To **single-process local development**, nie publiczny deployment ani klucz CGC.
Python3.12, istniejący checkout, dostęp do publicznego package index podczas
instalacji. Systemowy Debian może rozdzielać `python3.12-venv`/ensurepip:
bez niego `python3.12 -m venv` nie jest kompletną instalacją. Nie naprawiaj tego
bez pytania przez sudo/systemowe aktualizacje. Możesz zamiast pierwszej komendy
README użyć `uv venv --seed --python 3.12 .venv`, następnie tych samych pip/CLI
komend; również daje osobny venv, nie globalny install. Procedura z [README](../README.md#quick-start) instaluje hash-pinned
runtime i build backend, sam pakiet/CLI, tworzy nowy prywatny katalog, minimalny
config i osobny klucz ważny dobę. Nie używaj produkcyjnego URI ani prawdziwych
komentarzy/PII do ćwiczenia.

`requirements-build.lock` jest tu narzędziem instalacji ze źródeł. W produkcyjnym
image build tools pozostają w builder stage; lokalny venv nie dowodzi zgodności
ani bezpieczeństwa opublikowanego obrazu.

## Pierwszy uwierzytelniony request

W drugim terminalu użyj **ścieżki referencji** wypisanej przez README. Zachowaj
serwer w pierwszym terminalu. Nie kopiuj treści api-key do terminala/URL/argv.
Przykład wywołuje wyłącznie model-list: brak backendów w tym profilu oznacza
pustą listę, bez backend call i bez billing usage.

```sh
. .venv/bin/activate
python - /private/printed-state-directory/api-key <<'PY'
import json, pathlib, sys, urllib.request
key = pathlib.Path(sys.argv[1]).read_text().strip()
request = urllib.request.Request('http://127.0.0.1:8080/v1/models',
    headers={'Authorization': 'Bearer ' + key})
with urllib.request.urlopen(request, timeout=5) as response:
    value = json.load(response)
    assert response.status == 200 and value['data'] == []
    print('Authenticated model-list OK; no usage request sent')
PY
```

Anonimowe wywołanie powinno otrzymać401. `/metrics` jest tu wyłączone: nie używaj
API key jako scrape credential. Dodanie prawdziwego backendu wymaga autoryzacji
operatora, jawnego `type`, URL i limitów z
[pełnego template](../llmproxy/config.toml). Wtedy requesty generujące usage są
normalnie naliczane; sam ten przykład nie uzasadnia darmowego klienta/modelu.

## CLI, persistence i restart

- `python -m llmproxy --create-config -c /private/path/config.toml`: create-only
  template0600, bez startu serwera/DB i bez nadpisywania istniejącego pliku.
- `python -m llmproxy.ctl -c ... user create --secret-file /private/new/api-key`:
  create-only0600, fsync przed DB grant. Plik nie jest automatycznie usuwany przy
  niepewnym wyniku bazy. Sprawdź stan operatora przed ponownym mintem; nie wyrzucaj
  jedynej referencji do klucza, który mógł zostać przyjęty.
- Bez `--secret-file` istnieje historyczne plaintext stdout. Nie używaj tego
  wariantu w sesjach agenta/CI/transkryptach ani nie przekierowuj przypadkowo logów
  z raw credential. Nowa ścieżka jest dostępna przez zainstalowane `llmproxyctl`
  oraz `python -m llmproxy.ctl`.
- Shell z serwerem musi zachować ten sam `LLMPROXY_DB_URI` (absolute SQLite path)
  i `-c` przy restarcie. CTRL-C zatrzymuje proces; ponowne wywołanie nie mintuje
  klucza i nie kasuje SQLite. SQLite ma automatyczne additive migrations; nie
  inicjalizuj nowej wersji samym `schema.sql`.
- Katalog i api-key są poza repo. Nie dodawaj ich do gita, publicznego HTTP ani
  artefaktów CI. Nie kasuj prywatnego state po prawdziwym usage: może zawierać
  niezsettlowane zobowiązania. Cleanup tego **nowego**, bez-backendowego ćwiczenia
  po zatrzymaniu procesu nie jest instrukcją retencji finansowej.

## Co ten profil nie gwarantuje

Brak TLS/public ingress, RBAC operatorów, multi-replica admission, cross-key
account limits, customer billing/tariffs, immutable-image scan, standalone Mongo
transaction equivalence, off-volume DR i E2E model/GPU acceptance. Trwały lokalny
plik nie jest off-volume backupem. Do Mongo użyj
[minimalnego runtime role](mongo-runtime-role.md); do monitoringu oddzielnego
[scrape credential](metrics-security.md). Source-only quickstart nie wdraża
wcześniejszych zmian w działającym demo.
