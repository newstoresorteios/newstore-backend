# Resumo das Correções da Integração Vindi

## Problemas Resolvidos

### 1. ✅ URL Inválida Causando Erro de Parse
**Problema**: `VINDI_API_BASE_URL` inválida causava erro "Failed to parse URL from .../customers?query=..."

**Solução**:
- Criado helper `normalizeBaseUrl()` em `vindi.js`, `vindi_public.js` e `vindi_payment_methods.js`
- Valida se a URL começa com `http://` ou `https://`
- Se inválida, loga warning e usa fallback
- Remove trailing slashes automaticamente

### 2. ✅ 401 da Vindi Causando Logout no Frontend
**Problema**: Erro 401 da Vindi (API key inválida) era repassado como 401, causando logout no frontend

**Solução**:
- `vindiRequest()` marca erros com `error.provider = "VINDI"`
- `mapVindiError()` verifica `error.provider === "VINDI"` antes de mapear
- Mapeamento de status:
  - 401/403 da Vindi → 502 com code `VINDI_AUTH_ERROR`
  - 422 da Vindi → 400 com code `VINDI_VALIDATION_ERROR`
  - 5xx da Vindi → 502 com code `VINDI_UPSTREAM_ERROR`
  - Outros 4xx → 502 com code `VINDI_CLIENT_ERROR`
- JWT expirado continua retornando 401 normalmente (não afetado)

### 3. ✅ Fluxo Customer -> Payment Profile
**Problema**: Fluxo não seguia a orientação oficial da Vindi

**Solução**:
- `ensureCustomer()` implementado corretamente:
  1. Busca por email: `GET /customers?query=email:<email>`
  2. Se não encontrar, cria: `POST /customers { name, email, code, registry_code? }`
- `createPaymentProfileWithCardData()` usa `customer_id` obrigatório
- Rota `/api/autopay/vindi/tokenize` segue o fluxo:
  1. Valida auth do usuário (JWT)
  2. Busca dados do usuário no DB se necessário
  3. `customerId = await ensureCustomer(...)`
  4. `paymentProfile = await createPaymentProfileWithCardData({ ..., customer_id: customerId })`
  5. Retorna `{ ok: true, customer_id, payment_profile_id, card_last4 }`

## Arquivos Modificados

### 1. `src/services/vindi.js`
- ✅ Adicionado `normalizeBaseUrl()` helper
- ✅ `VINDI_BASE` usa `normalizeBaseUrl()`
- ✅ Log diagnóstico no boot: `[vindi] VINDI_BASE configurado: ...`
- ✅ `vindiRequest()` marca erros com `error.provider = "VINDI"`
- ✅ Log de sucesso em cada request: `method + url + status`

### 2. `src/services/vindi_public.js`
- ✅ Adicionado `normalizeBaseUrl()` helper
- ✅ `VINDI_PUBLIC_BASE` usa `normalizeBaseUrl()`
- ✅ Log diagnóstico no boot: `[vindiPublic] VINDI_PUBLIC_BASE configurado: ...`

### 3. `src/services/vindi_payment_methods.js`
- ✅ Adicionado `normalizeBaseUrl()` helper
- ✅ `VINDI_BASE` usa `normalizeBaseUrl()`

### 4. `src/routes/autopay_vindi.js`
- ✅ `mapVindiError()` verifica `error.provider === "VINDI"`
- ✅ 422 da Vindi mapeado para 400 (conforme solicitado)
- ✅ Tratamento de erros em `/tokenize` e `/setup` usa `mapVindiError()`

### 5. `src/test_vindi_payload.js`
- ✅ Script atualizado para testar `ensureCustomer()` e `createPaymentProfileWithCardData()`
- ✅ Usa dados de teste (não válidos para cobrança real)
- ✅ Mostra resumo com customer_id e payment_profile_id

## Checklist de Variáveis de Ambiente (Render)

### Obrigatórias
- [ ] `VINDI_API_KEY` - Chave privada da Vindi (Private API)
- [ ] `VINDI_PUBLIC_KEY` - Chave pública da Vindi (Public API) - se usar tokenização pública

### Opcionais (para sandbox)
- [ ] `VINDI_API_BASE_URL` ou `VINDI_API_URL` - URL base da API privada
  - Produção: `https://app.vindi.com.br/api/v1` (padrão)
  - Sandbox: `https://sandbox-app.vindi.com.br/api/v1`
- [ ] `VINDI_PUBLIC_BASE_URL` ou `VINDI_PUBLIC_URL` - URL base da API pública
  - Produção: `https://app.vindi.com.br/api/v1` (padrão)
  - Sandbox: `https://sandbox-app.vindi.com.br/api/v1`

## Logs Diagnósticos

### No Boot
```
[vindi] VINDI_BASE configurado: https://app.vindi.com.br/api/v1
[vindiPublic] VINDI_PUBLIC_BASE configurado: https://app.vindi.com.br/api/v1
```

### Em Cada Request
```
[vindi] chamando Vindi API: GET https://app.vindi.com.br/api/v1/customers?query=email:...
[vindi] Vindi API sucesso: GET https://app.vindi.com.br/api/v1/customers?query=email:... { status: 200 }
```

### Em Caso de Erro
```
[vindi] Vindi API erro: POST https://app.vindi.com.br/api/v1/payment_profiles { status: 401, error_message: "...", errors_count: 1, provider: "VINDI" }
```

## Teste Rápido

```bash
# Configurar variáveis de ambiente
export VINDI_API_KEY="sua_chave_aqui"
export TEST_EMAIL="test@example.com"
export TEST_NAME="Teste Usuario"

# Executar teste
node src/test_vindi_payload.js
```

## Explicação Curta das Alterações

1. **Blindagem de URL**: Helper `normalizeBaseUrl()` valida e normaliza URLs, evitando erros de parse
2. **Tratamento de Erros**: Erros da Vindi são marcados com `provider: "VINDI"` e mapeados para códigos HTTP apropriados (nunca 401)
3. **Fluxo Correto**: Customer é criado/garantido primeiro, depois payment_profile é criado com `customer_id`
4. **Logs Diagnósticos**: Logs mostram URL configurada no boot e method+url+status em cada request
5. **Script de Teste**: Script atualizado para testar o fluxo completo

