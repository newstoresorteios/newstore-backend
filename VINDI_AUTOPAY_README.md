# Integração Vindi - Autopay

## Endpoints

### POST /api/autopay/vindi/tokenize
Tokeniza cartão via Vindi Public API e retorna `gateway_token`.

**Request:**
```json
{
  "holder_name": "João Silva",
  "card_number": "4111111111111111",
  "card_expiration": "12/2025",
  "card_cvv": "123",
  "payment_company_code": "visa",
  "document_number": "12345678901"
}
```

**Response (sucesso):**
```json
{
  "ok": true,
  "gateway_token": "abc123...xyz789",
  "brand": "visa",
  "last4": "1111"
}
```

**Response (erro):**
```json
{
  "ok": false,
  "code": "VINDI_VALIDATION_ERROR",
  "error_message": "card_number: deve ter 16 dígitos",
  "provider_status": 422,
  "errors": [
    {
      "field": "card_number",
      "message": "deve ter 16 dígitos"
    }
  ],
  "requestId": "abc-123-def-456"
}
```

### POST /api/autopay/vindi/setup
Configura autopay: garante customer e cria payment_profile com `gateway_token`.

**Request:**
```json
{
  "gateway_token": "abc123...xyz789",
  "holder_name": "João Silva",
  "doc_number": "12345678901",
  "numbers": [1, 5, 10],
  "active": true
}
```

**Response (sucesso):**
```json
{
  "ok": true,
  "active": true,
  "numbers": [1, 5, 10],
  "holder_name": "João Silva",
  "doc_number": "12345678901",
  "vindi": {
    "customer_id": "123456",
    "payment_profile_id": "789012",
    "last_four": "1111"
  },
  "card": {
    "brand": "visa",
    "last4": "1111",
    "has_card": true
  }
}
```

**Response (erro 422 - validação Vindi):**
```json
{
  "ok": false,
  "code": "VINDI_VALIDATION_ERROR",
  "error_message": "holder_name: deve ficar em branco",
  "provider_status": 422,
  "errors": [
    {
      "field": "holder_name",
      "message": "deve ficar em branco"
    }
  ],
  "requestId": "abc-123-def-456"
}
```

**Response (erro 401 - autenticação):**
```json
{
  "ok": false,
  "code": "VINDI_AUTH_ERROR",
  "error_message": "Chave da API inválida",
  "provider_status": 401,
  "requestId": "abc-123-def-456"
}
```

### GET /api/autopay/vindi/status
Retorna status do autopay do usuário.

**Response:**
```json
{
  "active": true,
  "has_vindi": true,
  "holder_name": "João Silva",
  "doc_number": "12345678901",
  "numbers": [1, 5, 10],
  "vindi": {
    "customer_id": "123456",
    "payment_profile_id": "789012",
    "last_four": "1111"
  },
  "card": {
    "brand": "visa",
    "last4": "1111",
    "has_card": true
  }
}
```

### POST /api/autopay/vindi/webhook
Webhook da Vindi para atualizar status de pagamentos (bills/charges).

**Request (Vindi):**
```json
{
  "type": "bill_paid",
  "data": {
    "bill": {
      "id": "123456",
      "status": "paid"
    },
    "charge": {
      "id": "789012",
      "status": "paid"
    }
  }
}
```

**Response:**
```json
{
  "ok": true,
  "message": "webhook processed",
  "requestId": "abc-123-def-456"
}
```

## Variáveis de Ambiente

### Obrigatórias

```env
# Base URL da API privada (produção)
VINDI_API_BASE_URL=https://app.vindi.com.br/api/v1

# Chave de API privada (obtida no painel Vindi: Configurações > API)
VINDI_API_KEY=<sua-chave-privada>

# Chave de API pública (obtida no painel Vindi: Configurações > API > Public Key)
VINDI_PUBLIC_KEY=<sua-chave-publica>
```

### Opcionais

```env
# Base URL da API pública (usa fallback se não configurada)
VINDI_PUBLIC_BASE_URL=https://app.vindi.com.br/api/v1

# Sandbox (usa URLs de sandbox se true)
VINDI_SANDBOX=false

# Gateway padrão (não usado quando gateway_token está presente)
VINDI_DEFAULT_GATEWAY=pagarme
```

### Sandbox

```env
VINDI_API_BASE_URL=https://sandbox-app.vindi.com.br/api/v1
VINDI_PUBLIC_BASE_URL=https://sandbox-app.vindi.com.br/api/v1
VINDI_SANDBOX=true
```

## Fluxo de Setup

1. **Tokenize** (`POST /api/autopay/vindi/tokenize`):
   - Frontend envia dados do cartão
   - Backend tokeniza via Vindi Public API
   - Retorna `gateway_token`

2. **Setup** (`POST /api/autopay/vindi/setup`):
   - Frontend envia `gateway_token` + preferências
   - Backend garante customer (GET/POST /customers)
   - Backend cria payment_profile (POST /payment_profiles) com body mínimo:
     ```json
     {
       "gateway_token": "...",
       "customer_id": "...",
       "payment_method_code": "credit_card"
     }
     ```
   - Backend persiste no banco (autopay_profiles)

## Fluxo de Recorrência (runAutopayForDraw)

Quando um sorteio é aberto (`POST /api/admin/draws/new` ou `POST /api/admin/draws/:id/open`):

1. Busca `autopay_profiles` ativos com `vindi_customer_id` e `vindi_payment_profile_id`
2. Para cada perfil:
   - Calcula números disponíveis (filtra livres)
   - Calcula `amount_cents = números_livres * preço_por_número`
   - Cria bill na Vindi com idempotency: `draw:{drawId}:user:{userId}`
   - Cobra a bill
   - Se aprovado: cria payment + reservation + atualiza numbers
   - Grava em `autopay_runs` (status: ok/error)

## Webhook da Vindi

Configure no painel Vindi:
- URL: `https://seu-dominio.com/api/autopay/vindi/webhook`
- Eventos: `bill_paid`, `bill_failed`, `charge_paid`, `charge_rejected`

O webhook atualiza:
- `autopay_runs.status` (ok/error)
- `payments.status` e `payments.vindi_status`

## Logs

Todos os logs incluem `requestId` para correlação:
- Aceita `x-request-id` do header do frontend
- Gera UUID se não fornecido

Logs estruturados mostram:
- Requisições à Vindi (method, url, body mascarado)
- Respostas da Vindi (status, erros)
- Dados sensíveis mascarados (cartão, CVV, documentos)

## Erros Comuns

### 422 "holder_name: deve ficar em branco"
**Causa:** Enviando `holder_name` ao criar payment_profile com `gateway_token`.

**Solução:** Corrigido - agora envia apenas `gateway_token`, `customer_id`, `payment_method_code`.

### 401 "Chave da API inválida"
**Causa:** `VINDI_API_KEY` incorreta ou `VINDI_API_BASE_URL` inválida.

**Solução:** Verifique as variáveis de ambiente no Render.

### 502 "Failed to parse URL"
**Causa:** `VINDI_API_BASE_URL` contém chave ao invés de URL.

**Solução:** Configure `VINDI_API_BASE_URL=https://app.vindi.com.br/api/v1`.

