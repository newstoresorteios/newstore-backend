# Migração do Autopay: Mercado Pago → Vindi

Este documento descreve a migração completa do sistema de autopay do Mercado Pago para a Vindi.

## 📋 Visão Geral

A migração mantém compatibilidade com perfis antigos do Mercado Pago enquanto adiciona suporte completo à Vindi como novo provider principal.

## 🚀 Instalação e Configuração

### 1. Variáveis de Ambiente

Adicione as seguintes variáveis ao seu `.env`:

```bash
# Vindi
VINDI_API_BASE_URL=https://app.vindi.com.br/api/v1
VINDI_API_KEY=uXYN-Nh3uqwoNUaTs2eqwigoUic6qZvx0Gttg3d-8Ro
VINDI_WEBHOOK_SECRET=__COLE_UM_SECRET_AQUI__
VINDI_DEFAULT_PAYMENT_METHOD=credit_card
VINDI_DEFAULT_GATEWAY=pagarme
```

### 2. Migração do Banco de Dados

Execute a migration SQL:

```bash
psql $DATABASE_URL -f src/migrations/001_add_vindi_columns.sql
```

Ou execute manualmente as queries do arquivo `src/migrations/001_add_vindi_columns.sql`.

A migration adiciona:
- Colunas Vindi em `autopay_profiles` (vindi_customer_id, vindi_payment_profile_id, etc.)
- Coluna `provider` em `payments` e `autopay_runs`
- Colunas Vindi em `payments` (vindi_bill_id, vindi_charge_id, vindi_status)
- Índices para performance

## 📡 Novas Rotas

### Setup Autopay Vindi

**POST** `/api/autopay/vindi/setup`

Configura autopay com Vindi para o usuário logado.

**Body:**
```json
{
  "gateway_token": "token_gerado_no_frontend",
  "holder_name": "Nome do Titular",
  "doc_number": "12345678900",
  "numbers": [1, 2, 3],
  "active": true
}
```

**Resposta:**
```json
{
  "ok": true,
  "active": true,
  "numbers": [1, 2, 3],
  "vindi": {
    "customer_id": "123",
    "payment_profile_id": "456",
    "last_four": "1234"
  },
  "card": {
    "last4": "1234",
    "has_card": true
  }
}
```

### Status Autopay Vindi

**GET** `/api/autopay/vindi/status`

Retorna status do autopay Vindi do usuário logado.

**Resposta:**
```json
{
  "active": true,
  "has_vindi": true,
  "numbers": [1, 2, 3],
  "vindi": {
    "customer_id": "123",
    "payment_profile_id": "456",
    "last_four": "1234",
    "status": "active"
  },
  "card": {
    "last4": "1234",
    "has_card": true
  }
}
```

### Cancelar Autopay Vindi

**POST** `/api/autopay/vindi/cancel`

Cancela o autopay Vindi (remove payment_profile, mantém customer).

### Webhook Vindi

**POST** `/api/payments/vindi/webhook`

Endpoint para receber eventos da Vindi (bill_paid, charge_rejected, etc.).

Configure na dashboard da Vindi apontando para: `https://seu-dominio.com/api/payments/vindi/webhook`

## 🔄 Fluxo de Funcionamento

### 1. Setup (Frontend → Backend)

1. Frontend gera `gateway_token` usando Vindi Public API
2. Frontend envia `gateway_token` + dados do titular para `/api/autopay/vindi/setup`
3. Backend:
   - Cria/garante customer na Vindi
   - Cria payment_profile usando `gateway_token`
   - Salva IDs no banco de dados

### 2. Cobrança Automática (Quando sorteio abre)

1. `autopayRunner.js` detecta sorteio aberto
2. Para cada perfil ativo:
   - **Vindi primeiro**: Se tiver `vindi_payment_profile_id`, usa Vindi
   - **MP fallback**: Se não tiver Vindi mas tiver `mp_card_id`, usa MP
3. Cria bill na Vindi (ou cobra no MP)
4. Se aprovado:
   - Reserva números
   - Cria payment com `provider='vindi'` ou `provider='mercadopago'`
   - Marca números como sold
5. Se falhar após cobrança:
   - Executa refund na Vindi (se aplicável)
   - Registra erro

### 3. Webhook (Vindi → Backend)

1. Vindi envia evento (bill_paid, charge_rejected, etc.)
2. Backend processa e atualiza `payments.vindi_status`
3. Se necessário, reconcilia números/reservas

## 🔐 Segurança

- ✅ **Nunca aceita PAN/CVV no backend** - apenas `gateway_token`
- ✅ **Logs não expõem segredos** - API keys nunca são logadas
- ✅ **Basic Auth RFC2617** - formato correto `API_KEY:` base64
- ✅ **Validação de webhook** - suporte a secret (se Vindi fornecer)

## 🧪 Testes

### Teste Manual de Setup

```bash
# 1. Obter token de autenticação
TOKEN=$(curl -X POST http://localhost:4000/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"test@example.com","password":"123456"}' | jq -r .token)

# 2. Setup autopay Vindi (substitua gateway_token por um token real)
curl -X POST http://localhost:4000/api/autopay/vindi/setup \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "gateway_token": "token_do_frontend",
    "holder_name": "João Silva",
    "doc_number": "12345678900",
    "numbers": [1, 2, 3],
    "active": true
  }'
```

### Teste de Runner

```bash
# Criar sorteio e rodar autopay
curl -X POST http://localhost:4000/api/admin/draws/new \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"product_name":"Produto Teste"}'
```

## 📊 Compatibilidade

- ✅ **Perfis MP antigos continuam funcionando** - fallback automático
- ✅ **Rotas antigas mantidas** - `/api/me/autopay` ainda funciona
- ✅ **Migração gradual** - usuários podem migrar quando quiserem

## 🐛 Troubleshooting

### Erro: "VINDI_API_KEY não configurado"
- Verifique se `VINDI_API_KEY` está no `.env`
- Reinicie o servidor após adicionar

### Erro: "payment_profile_failed"
- Verifique se `gateway_token` é válido
- Confirme que gateway está configurado na Vindi

### Webhook não recebe eventos
- Verifique URL configurada na dashboard Vindi
- Confirme que endpoint está acessível publicamente
- Verifique logs do servidor

## 📝 Notas Importantes

1. **Gateway Token**: Deve ser gerado no frontend usando Vindi Public API. Backend nunca recebe dados sensíveis do cartão.

2. **Refund**: Implementado automaticamente se reserva falhar após cobrança bem-sucedida.

3. **Idempotência**: Webhook handler verifica se payment existe antes de processar.

4. **Provider Detection**: `autopayRunner` detecta automaticamente qual provider usar baseado nos dados do perfil.

## 🔗 Referências

- [Vindi API Documentation](https://developers.vindi.com.br/reference)
- [Vindi Public API (Frontend)](https://developers.vindi.com.br/docs/public-api)

