# Prywatne Mongo release gates

## Który pipeline co sprawdza

GitLab `mongo` jest blokującym jobem przed `publish`, nie pojedynczym auth smoke.
Uruchamia kolejno, bez `|| true`, skip ani optymalizacji assertions:

| Probe | Istotne dowody |
|---|---|
| mongo_probe | auth/bad auth, replica-set primary, BSON ObjectId, UTC expiry/cache, Decimal128, transaction rollback |
| mongo_admission_probe | owner fencing, expired owner nie odżywa, real two-process contention/one winner |
| mongo_accounting_probe | receipt identity/sample Decimal128, lost reply, two processes/one bill, process crash po insert, pending recovery/ACK outbox |
| mongo_delivery_probe | conversion proof/unknown, lost reply, two processes/one bill, crash po insert, history/source conflict |
| mongo_role_probe | cold minimal role/indices, HTTP obligations/lost reply/two runtime writers/crash, missing finance conflict, negative ACLs |

Pierwszy failure przerywa job. Publication potrzebuje image+scan+mongo i pozostaje
manual/protected/private tag. Brak sekretu lub `PYTHONOPTIMIZE` nie daje zielonego
skipa: job failuje przed próbą. Image stage może wcześniej zbudować **niepublikowany**
artifact; nie oznacza to przejścia gate Mongo.

GitHub workflow sprawdza unit/image/scan i **niczego nie publikuje**. Nie twierdzimy,
że sam ten workflow zastępuje prywatne Mongo release acceptance. Tych materiałów
nie wypychamy do obecnego publicznego origin bez świadomej zgody/private destination.

## Credentials i izolacja

- Tylko isolated service `audit-stt-mongo`, logical profile `ci-audit` (lub istniejący
  prywatny CGC audit profile `izajac`), RS`auditstt`; inne hosty/namespaces są
  odrzucane **przed połączeniem**. Conversion/role guards działają także bez asserts.
- GitLab dostaje synthetic service secret przez masked/protected
  `AUDIT_MONGO_PASSWORD`; YAML zawiera wyłącznie referencję. Nie używaj customer URI,
  produkcyjnego admina, raw tokenu w komendzie ani `set -x`/dump env/compose config.
- Service tworzy własny losowy keyfile0600, root auth służy tylko provisionowaniu
  syntetycznych fixtures i negatywnym ACLs. Minimal runtime role nie dostaje
  admin/auth writes ani financial update/delete/drop.
- Records i nonce-owned DB/user/role są usuwane przez **własny** probe cleanup.
  To nie instrukcja retencji/purge klienta i nie prawo usuwania financial rows
  z prawdziwego datasetu. Testowe fault injections/process exit dotyczą tylko
  fixture identities.

## Co trzeba osobno wykazać

Same statyczne guardrail tests i poprawny YAML nie są dowodem hosted CI run.
Prywatny CGC job może wykonać dokładne5probe commands odczytane z YAML z profilem
`ci-audit`; raport musi nazwać to **equivalent execution**, nie udawać GitLab UI.
Cold GitLab service provisioning/runner routing, manual tag protections i actual
release publication potrzebują własnego pipeline proof na prywatnym celu.
Standalone Mongo nie dziedziczy gwarancji RS/transakcji; arbitrary-provider unknown
usage, trwała utrata obu ledgers, off-volume DR, deploy roles/alerts i GPU gates
pozostają oddzielnymi warunkami. Testy nie autoryzują customer charges ani rollout.
