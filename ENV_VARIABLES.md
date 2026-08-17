# Variáveis de Ambiente - Backend Lancaster

Este documento lista as variáveis de ambiente necessárias para configurar o backend.

## Variáveis Obrigatórias para Vindi

### VINDI_PUBLIC_KEY
- **Descrição**: Chave pública da Vindi para tokenização de cartões (Public API)
- **Obrigatória**: Sim (para autopay Vindi)
- **Uso**: Tokenização de cartões via `/api/autopay/vindi/tokenize`
- **Exemplo**: `VINDI_PUBLIC_KEY=your_vindi_public_key_here`

### VINDI_API_KEY
- **Descrição**: Chave privada da Vindi para operações administrativas (Private API)
- **Obrigatória**: Sim (para autopay Vindi completo)
- **Uso**: Criação de customers, payment_profiles, bills, etc.
- **Exemplo**: `VINDI_API_KEY=your_vindi_api_key_here`

## Variáveis Opcionais para Vindi

### VINDI_PUBLIC_BASE_URL ou VINDI_PUBLIC_URL
- **Descrição**: URL base da API pública da Vindi
- **Padrão**: `https://app.vindi.com.br/api/v1`
- **Uso**: Para sandbox ou ambientes customizados
- **Exemplo**: `VINDI_PUBLIC_BASE_URL=https://sandbox-app.vindi.com.br/api/v1`
- **Nota**: Aceita tanto `VINDI_PUBLIC_BASE_URL` quanto `VINDI_PUBLIC_URL` (ambos funcionam)

### VINDI_API_BASE_URL ou VINDI_API_URL
- **Descrição**: URL base da API privada da Vindi
- **Padrão**: `https://app.vindi.com.br/api/v1`
- **Uso**: Para sandbox ou ambientes customizados
- **Exemplo**: `VINDI_API_BASE_URL=https://sandbox-app.vindi.com.br/api/v1`
- **Nota**: Aceita tanto `VINDI_API_BASE_URL` quanto `VINDI_API_URL` (ambos funcionam)

### VINDI_DEFAULT_PAYMENT_METHOD
- **Descrição**: Método de pagamento padrão
- **Padrão**: `credit_card`
- **Exemplo**: `VINDI_DEFAULT_PAYMENT_METHOD=credit_card`

### VINDI_DEFAULT_GATEWAY
- **Descrição**: Gateway de pagamento padrão
- **Padrão**: `pagarme`
- **Exemplo**: `VINDI_DEFAULT_GATEWAY=pagarme`

## Pré-autorização de números cativos

### CAPTIVE_PREAUTH_AUTO_APPROVE_ON_EXPIRY_ENABLED
- **Descrição**: Controla a decisão aplicada a pré-autorizações `pending` quando a janela expira.
- **Padrão**: `false`
- **Com `false`**: Preserva o comportamento legado; a autorização vira `expired`, a reserva é liberada e não há cobrança.
- **Com `true`**: Autoriza automaticamente apenas registros ainda `pending`, agrupa por sorteio e usuário e chama o runner financeiro para cobrar o cartão cadastrado.
- **Exemplo**: `CAPTIVE_PREAUTH_AUTO_APPROVE_ON_EXPIRY_ENABLED=true`

### CAPTIVE_PREAUTH_EXPIRY_SCAN_ENABLED
- **Descrição**: Liga ou desliga somente a execução periódica do scanner de vencimentos.
- **Padrão**: `true`
- **Exemplo**: `CAPTIVE_PREAUTH_EXPIRY_SCAN_ENABLED=true`

### CAPTIVE_PREAUTH_EXPIRY_SCAN_INTERVAL_MS
- **Descrição**: Intervalo entre execuções do scanner, em milissegundos.
- **Padrão**: `300000`
- **Exemplo**: `CAPTIVE_PREAUTH_EXPIRY_SCAN_INTERVAL_MS=300000`

### CAPTIVE_PREAUTH_EXPIRES_HOURS
- **Descrição**: Janela para o cliente aprovar ou recusar a pré-autorização.
- **Padrão**: `12`
- **Exemplo**: `CAPTIVE_PREAUTH_EXPIRES_HOURS=12`

### CAPTIVE_PREAUTH_CHARGE_ON_AUTHORIZE_ENABLED
- **Descrição**: Permite que uma autorização confirmada siga imediatamente para o runner financeiro.
- **Padrão**: `false` no código; deve permanecer `true` no ambiente que usa cobrança imediata.
- **Exemplo**: `CAPTIVE_PREAUTH_CHARGE_ON_AUTHORIZE_ENABLED=true`

## Outras Variáveis Importantes

### PORT
- **Descrição**: Porta do servidor
- **Padrão**: `4000`
- **Exemplo**: `PORT=4000`

### DATABASE_URL
- **Descrição**: URL de conexão com o banco de dados PostgreSQL
- **Obrigatória**: Sim
- **Exemplo**: `DATABASE_URL=postgresql://user:password@localhost:5432/dbname`

### JWT_SECRET
- **Descrição**: Chave secreta para assinatura de tokens JWT
- **Obrigatória**: Sim
- **Exemplo**: `JWT_SECRET=your_jwt_secret_key_here`

### CORS_ORIGIN
- **Descrição**: Origens permitidas para CORS (separadas por vírgula)
- **Opcional**: Sim (usa allowlist padrão se não configurado)
- **Exemplo**: `CORS_ORIGIN=http://localhost:3000,https://yourdomain.com`

## Exemplo de Arquivo .env

```bash
# Porta do servidor
PORT=4000

# Banco de Dados
DATABASE_URL=postgresql://user:password@localhost:5432/dbname

# Vindi - Obrigatórias
VINDI_PUBLIC_KEY=your_vindi_public_key_here
VINDI_API_KEY=your_vindi_api_key_here

# Vindi - Opcionais (para sandbox)
# VINDI_PUBLIC_BASE_URL=https://sandbox-app.vindi.com.br/api/v1
# VINDI_API_BASE_URL=https://sandbox-app.vindi.com.br/api/v1

# Autenticação
JWT_SECRET=your_jwt_secret_key_here

# CORS
# CORS_ORIGIN=http://localhost:3000
```

## Notas Importantes

1. **Sandbox vs Produção**: As chaves da Vindi são diferentes entre sandbox e produção. Certifique-se de usar as URLs corretas (`VINDI_PUBLIC_BASE_URL` e `VINDI_API_BASE_URL`) quando usar sandbox.

2. **Segurança**: Nunca commite arquivos `.env` no repositório. Use `.env.example` ou este documento como referência.

3. **Erros 401**: Se receber erro 401 "Chave da API inválida", verifique:
   - Se a chave está correta
   - Se a base URL corresponde ao ambiente (sandbox vs produção)
   - Se a chave pública está sendo usada para Public API e a privada para Private API

