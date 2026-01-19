## Tray OAuth (Backend) — Operação

### Variáveis de ambiente
- **Obrigatórias**:
  - `TRAY_CONSUMER_KEY`
  - `TRAY_CONSUMER_SECRET`
  - `TRAY_API_BASE` (ou `TRAY_API_ADDRESS`) — ex: `https://www.newstorerj.com.br/web_api`
- **Bootstrap (somente 1x / quando reautorizar)**:
  - `TRAY_CODE` (pode expirar; é único por loja)
- **Alternativo**:
  - `TRAY_REFRESH_TOKEN` (opcional; se não tiver, o sistema usa o token persistido no KV/DB)

### Como obter `TRAY_CODE` (1x)
Depois de instalar/reauthorize o app na Tray, a callback URL recebe `code` e `api_address`.
Use o endpoint do backend para capturar e persistir automaticamente:

- `GET /tray/callback/auth?code=...&api_address=...`

Ele persiste `tray_api_base` e gera tokens imediatamente.

### Como validar OAuth
- `GET /api/tray/health`

Retorna:
- `ok`
- `authMode` (`cache` | `refresh` | `bootstrap`)
- `apiBase`
- `expAccessAt`
- `hasRefreshKV`
- `lastError`

### Quando remover `TRAY_CODE`
Assim que o `refresh_token` estiver persistido no KV/DB (key `tray_refresh_token`) e o refresh estiver OK,
você pode **remover `TRAY_CODE`** da env — o dia a dia deve funcionar via refresh.

### Logs esperados (Render)
- **Bootstrap OK (primeira vez):**
  - `[tray.auth] env ... hasCode=true ...`
  - `[tray.auth] bootstrap start ...`
  - `[tray.auth] bootstrap ok expAccess=... expRefresh=...`
- **Refresh OK (rotina):**
  - `[tray.auth] env ... hasRefreshKV=true ...`
  - `[tray.auth] refresh start ...`
  - `[tray.auth] refresh ok ...`
- **CODE inválido/expirado (401/1099):**
  - `[tray.auth] bootstrap fail status=401 ... error_code=1099 causes=...`
  - `[tray.auth] CODE invalid/expired -> reauthorize app in Tray and generate a new code`
  - `AÇÃO: gere um novo TRAY_CODE ...`


