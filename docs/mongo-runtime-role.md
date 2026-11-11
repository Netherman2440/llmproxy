# Mongo runtime role — auth, finance i journale

Runtime nie powinien używać `root`, `dbOwner` ani `readWrite` na całym clusterze.
Wersjonowany kontrakt to `llmproxy.mongo_roles.runtime_privileges`; helper tylko
zwraca metadata i **nie provisionuje** roli, użytkownika czy hasła.

```bash
# Metadata, bez credentials; wybierz tylko profile faktycznie włączone.
python -m llmproxy.mongo_roles --receipt-v1 --shared-admission > runtime-privileges.json
```

Bez flag drukuje rolę bazową. `--database` domyślnie `cgc`; inna nazwa jest
przydatna wyłącznie dla świadomego środowiska/testu z odpowiednim routingiem.
Produkcyjny adapter używa `cgc`; samo wygenerowanie innego spec nie zmienia
runtime DB routingu. Wildcards i niepoprawne nazwy są odrzucane.

| Collection w cgc | Runtime actions | Profil |
|---|---|---|
| api_keys |find| wszystkie — lookup SHA256 key/expiry |
| rest_users |find| wszystkie — historyczny owner/org/namespace/tier |
| billing_record |find,insert| wszystkie — zapis i weryfikacja idempotentnego retry |
| llmproxy_conversion_delivery |find,insert,update,createIndex| wszystkie — startup recovery retained debt |
| llmproxy_http_attempt |find,insert,update,createIndex| wszystkie — durable HTTP/SSE usage/recovery |
| llmproxy_accounting_session |find,insert,update,createIndex| receipt-v1 |
| llmproxy_accounting_receipt |find,insert,update,createIndex| receipt-v1 |
| admission_bucket |find,insert,update,createIndex| shared admission |

Nie ma `remove`, update financial history, drop, write auth tables ani admin
users/roles. createIndex obejmuje wyłącznie konkretne nowe journal/lease
collections: obecny adapter idempotentnie provisionuje swoje indeksy przy użyciu
profilu. Jeśli organizacja wymaga wyodrębnienia DDL, potrzebuje osobnego migration
workflow i zmiany adaptera — samo odebranie createIndex zepsuje startup.

Delivery recovery działa nawet po usunięciu conversion backend z bieżącej
konfiguracji, więc bazowa rola nadal obejmuje retained delivery ledger. Wyłączenie
backendu nie jest upoważnieniem do zapomnienia wcześniejszego zobowiązania.
HTTP/SSE journal również należy do baseline: zanim upstream przyjmie request,
proxy utrwala owner/source, potem confirmed usage i dopiero finance. Wymaga to
jawnego rozszerzenia dotychczasowej roli o `llmproxy_http_attempt` przed rolloutem.
Nie używamy starego `billing.events_oneoff`: authority to `cgc.billing_record`.

## Provisioning administracyjny

Najpierw przejrzyj JSON spec, namespace/DB, profile i plan backup/restore. Zastosuj
jako administrator, nie credentialem aplikacji. Poniższy mongosh fragment wymaga
chronionej referencji do metadata pliku i interaktywnego password prompt:

```javascript
const fs = require('fs');
const specPath = process.env.LLMPROXY_ROLE_SPEC;
if (!specPath) throw new Error('LLMPROXY_ROLE_SPEC metadata path required');
const privileges = JSON.parse(fs.readFileSync(specPath, 'utf8'));
if (!privileges.length || privileges.some(p => p.resource.db !== 'cgc' || !p.resource.collection))
  throw new Error('Review explicit cgc collection scope before provisioning');
const auth = db.getSiblingDB('admin');
auth.createRole({role: 'llmproxyRuntime', privileges, roles: []});
auth.createUser({user: 'llmproxyRuntime', pwd: passwordPrompt(),
  roles: [{role: 'llmproxyRuntime', db: 'admin'}]});
```

To **create-only** przykład, nie idempotentne nadpisywanie roli. Istniejąca rola
wymaga osobnego review/diff i świadomego update; nie wykonuj go w ciemno.
URI z `authSource=admin` trafia do chronionego `LLMPROXY_DB_URI`/credential helper,
nie do argv, repo, ConfigMap, obrazu ani stdout. Nie udostępniaj DB publicznie.

Odbiór obejmuje prawdziwe auth lookups obu kolekcji, legacy/receipt/delivery
financial writes, cold indexes, lease renew/release, idempotent retries i próbę
niedozwolonego update/delete/drop/auth-write/foreign-DB/admin action.
Samo wygenerowanie JSON nie jest dowodem działającego ACL. Dowody konkretnego
isolated probe są w specs/001-post-audit-hardening/evidence/; realny deployment
musi potwierdzić swoją wersję Mongo, role, TLS, storage i consumer/pricing.

Mongo runtime user nie jest kontem do backup/restore, kasowania historii lub
naprawienia unknown legacy claim. Te działania wymagają osobnej authority.
Użytkownik może mieć wbudowane prawo odczytu własnych danych konta — to nie to
samo co uprawnienie do zarządzania albo czytania cudzych użytkowników.
