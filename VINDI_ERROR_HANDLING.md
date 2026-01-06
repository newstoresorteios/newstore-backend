# Tratamento de Erros da Integração Vindi

## Problema Resolvido

Anteriormente, quando a Vindi retornava erro 401 (ex: "Chave da API inválida"), o backend repassava HTTP 401 para o frontend. Isso causava confusão, pois o frontend interpretava como "sessão expirada" (erro de JWT do nosso sistema), quando na verdade era um erro de autenticação com a Vindi.

## Solução Implementada

### Mapeamento de Erros da Vindi

O backend agora mapeia erros da Vindi para códigos HTTP apropriados:

| Status Vindi | HTTP Status | Código | Descrição |
|--------------|-------------|--------|-----------|
| 401, 403 | 502 | `VINDI_AUTH_ERROR` | Erro de autenticação da Vindi (chave inválida, URL errada) |
| 422 | 422 | `VINDI_VALIDATION_ERROR` | Erro de validação (dados inválidos) |
| 400 | 400 | `VINDI_BAD_REQUEST` | Bad Request |
| 5xx | 502 | `VINDI_UPSTREAM_ERROR` | Erro no servidor da Vindi |
| Outros 4xx | 502 | `VINDI_CLIENT_ERROR` | Outros erros do cliente |
| Sem status | 500 | `INTERNAL_ERROR` | Erro interno |

### Formato de Resposta de Erro

```json
{
  "error": "vindi_auth_error",
  "code": "VINDI_AUTH_ERROR",
  "message": "Falha de autenticação na Vindi (verifique VINDI_API_KEY/VINDI_API_BASE_URL).",
  "provider_status": 401,
  "details": [
    {
      "message": "Chave da API inválida",
      "parameter": "authorization"
    }
  ]
}
```

### Garantias

1. **JWT expirado continua retornando 401**: O middleware `requireAuth` retorna 401 normalmente quando o JWT está inválido/expirado. Isso é do nosso sistema e não é afetado pelo mapeamento de erros da Vindi.

2. **Logs não expõem dados sensíveis**: Números de cartão e CVV são mascarados nos logs (apenas últimos 4 dígitos são mostrados).

3. **Mensagens de erro amigáveis**: Erros da Vindi são capturados e formatados em mensagens curtas (limite 300 caracteres) para facilitar o debug.

## Variáveis de Ambiente Necessárias

### Obrigatórias

- `VINDI_PUBLIC_KEY`: Chave pública da Vindi (Public API)
- `VINDI_API_KEY`: Chave privada da Vindi (Private API)

### Opcionais (para sandbox)

- `VINDI_PUBLIC_BASE_URL` ou `VINDI_PUBLIC_URL`: URL base da API pública
  - Produção: `https://app.vindi.com.br/api/v1` (padrão)
  - Sandbox: `https://sandbox-app.vindi.com.br/api/v1`

- `VINDI_API_BASE_URL` ou `VINDI_API_URL`: URL base da API privada
  - Produção: `https://app.vindi.com.br/api/v1` (padrão)
  - Sandbox: `https://sandbox-app.vindi.com.br/api/v1`

## Exemplo de Uso

### Erro de Autenticação (401 da Vindi)

**Request:**
```http
POST /api/autopay/vindi/tokenize
Authorization: Bearer <jwt_token>
Content-Type: application/json

{
  "holder_name": "João Silva",
  "card_number": "4111111111111111",
  "card_expiration": "12/25",
  "card_cvv": "123"
}
```

**Response (502 Bad Gateway):**
```json
{
  "error": "vindi_auth_error",
  "code": "VINDI_AUTH_ERROR",
  "message": "Falha de autenticação na Vindi (verifique VINDI_API_KEY/VINDI_API_BASE_URL).",
  "provider_status": 401,
  "details": [
    {
      "message": "Chave da API inválida"
    }
  ]
}
```

### Erro de Validação (422 da Vindi)

**Response (422 Unprocessable Entity):**
```json
{
  "error": "vindi_validation_error",
  "code": "VINDI_VALIDATION_ERROR",
  "message": "card_number: Número de cartão inválido",
  "provider_status": 422,
  "details": [
    {
      "message": "Número de cartão inválido",
      "parameter": "card_number"
    }
  ]
}
```

## Checklist de Configuração

- [ ] `VINDI_PUBLIC_KEY` configurado (obrigatório)
- [ ] `VINDI_API_KEY` configurado (obrigatório)
- [ ] `VINDI_PUBLIC_BASE_URL` ou `VINDI_PUBLIC_URL` configurado (opcional, para sandbox)
- [ ] `VINDI_API_BASE_URL` ou `VINDI_API_URL` configurado (opcional, para sandbox)
- [ ] URLs correspondem ao ambiente (produção vs sandbox)
- [ ] Chaves correspondem ao ambiente (produção vs sandbox)

