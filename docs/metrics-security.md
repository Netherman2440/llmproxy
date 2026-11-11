# Prywatny scrape metrics

`/metrics` wymaga osobnego `Authorization: Bearer` credential. Zwykły LLM API
key, cookie i query parameters nie autoryzują scrape. Nie ma publicznego fallbacku.
Brak sekretu, nieprawidłowy sekret lub nagłówek:401 z `WWW-Authenticate`.
`metrics.enabled=false` nie rejestruje endpointu.

## Konfiguracja

```toml
[metrics]
enabled = true
path = "/metrics"
token_env = "LLMPROXY_METRICS_TOKEN"
```

`token_env` to nazwa env, nie wartość. Literalne `token`/`secret` w tej sekcji są
odrzucane. Credential ma32..256 drukowalnych znaków ASCII bez whitespace;
wygeneruj go kryptograficznie w zarządzanym secret store, nie jako nazwę osoby.
Nie używaj ponownie user API key ani service credential backendu.

Proces proxy otrzymuje sekret przez `LLMPROXY_METRICS_TOKEN` (albo env wskazane
w `token_env`). Compose przekazuje wyłącznie referencję do env hosta; nie zapisuj
wartości w repo ani nie drukuj rozwiniętego `docker compose config`.

Standalone Prometheus ma osobny chroniony mount z tą samą wartością:

```yaml
authorization:
  type: Bearer
  credentials_file: /run/secrets/llmproxy_metrics_token
```

Pełny przykład: [prometheus-scrape.yml](../examples/prometheus-scrape.yml).
Skonfiguruj TLS lub rzeczywiście prywatną sieć. Sam Bearer przez publiczne HTTP
nie jest ochroną transportu. Nie umieszczaj sekretu w URL, command arguments,
front-end assets, ConfigMap ani pliku scrape config śledzonym przez git.

## Kubernetes / Prometheus Operator

Sample ServiceMonitor i PodMonitor mają `authorization.credentials` wskazujące
Secret `llmproxy-metrics`, key `token`. Secret musi istnieć w namespace monitora.
Proxy Deployment otrzymuje tę samą wartość przez własny secret reference:

```yaml
env:
  - name: LLMPROXY_METRICS_TOKEN
    valueFrom:
      secretKeyRef:
        name: llmproxy-metrics
        key: token
```

To fragment konfiguracji, nie gotowy manifest Deployment ani polecenie deployu.
Jeśli monitor i proxy są w innych namespaces, provisionuj osobne namespace-scoped
referencje w secret store. Uzgodnij RBAC, selector i dostępność pola authorization
w zainstalowanej wersji CRD. Nie dopisuj wartości do sample YAML.

## Pseudonimy, migracja i rotacja

Label `user` to `key_` + SHA256 domeny `llmproxy:key:` i stabilnego API-key ID.
Nie używa komentarza, nazwiska, emaila, raw id, secretu ani auth cache wartości.
Ten sam key zachowuje label po zmianie komentarza/secretu; różne klucze tego
samego człowieka mają różne labels. To metryka **per-key**, nie per-person.

To pseudonimizacja, nie anonimizacja: operator znający id może odtworzyć hash,
a legacy niskoentropijne id są podatne na słownik. Dane nadal wymagają dostępu
ograniczonego do operatorów. Standardowe UUID/ObjectId mają wysoką entropię.

Nowe workery tworzą wyłącznie nowe pseudonymous series. Stary worker utrzymuje
w pamięci wcześniej utworzone labels do restartu; historyczne serie w TSDB nie
znikają automatycznie. Zaplanuj rolling restart i zatwierdzoną retencję/access
historycznych danych — agent nie kasuje ich sam. Dashboards grupujące po `user`
zachowują nazwy labels, lecz zobaczą pseudonimy zamiast komentarzy.

Zmiana path, enabled lub token_env wymaga restartu. Zmieniona wartość w env
procesu jest używana od kolejnego requestu; w normalnym orchestratorze aktualizacja
secret-backed env wymaga restartu poda. Koordynuj credential po obu stronach;
stary credential ma po rotacji zwracać401, nie wpadać w API-key fallback.

Kontrola odbioru: anonymous i LLM user credential→401, poprawny dedicated
credential→200/Prometheus content, brak PII/credentials w exposition, target `up`.
Samo przejście CPU/HTTP testów nie potwierdza RBAC/CRD/TLS konkretnego deploymentu.
