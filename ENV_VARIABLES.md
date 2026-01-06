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

### VINDI_PUBLIC_BASE_URL
- **Descrição**: URL base da API pública da Vindi
- **Padrão**: `https://app.vindi.com.br/api/v1`
- **Uso**: Para sandbox ou ambientes customizados
- **Exemplo**: `VINDI_PUBLIC_BASE_URL=https://sandbox-app.vindi.com.br/api/v1`

### VINDI_API_BASE_URL
- **Descrição**: URL base da API privada da Vindi
- **Padrão**: `https://app.vindi.com.br/api/v1`
- **Uso**: Para sandbox ou ambientes customizados
- **Exemplo**: `VINDI_API_BASE_URL=https://sandbox-app.vindi.com.br/api/v1`

### VINDI_DEFAULT_PAYMENT_METHOD
- **Descrição**: Método de pagamento padrão
- **Padrão**: `credit_card`
- **Exemplo**: `VINDI_DEFAULT_PAYMENT_METHOD=credit_card`

### VINDI_DEFAULT_GATEWAY
- **Descrição**: Gateway de pagamento padrão
- **Padrão**: `pagarme`
- **Exemplo**: `VINDI_DEFAULT_GATEWAY=pagarme`

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

