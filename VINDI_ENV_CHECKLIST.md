# Checklist de Variáveis de Ambiente Vindi

## Produção

```env
# Base URL da API privada (obrigatória)
# Deve começar com https://
VINDI_API_BASE_URL=https://app.vindi.com.br/api/v1

# Chave de API privada (obrigatória)
# Obtida no painel Vindi: Configurações > API
VINDI_API_KEY=<sua-chave-privada-aqui>

# Base URL da API pública (opcional, usa fallback se não configurada)
# Geralmente é a mesma da API privada
VINDI_PUBLIC_BASE_URL=https://app.vindi.com.br/api/v1

# Chave de API pública (obrigatória)
# Obtida no painel Vindi: Configurações > API > Public Key
# DIFERENTE da VINDI_API_KEY
VINDI_PUBLIC_KEY=<sua-chave-publica-aqui>
```

## Sandbox

```env
# Base URL da API privada (obrigatória)
# Deve começar com https://
VINDI_API_BASE_URL=https://sandbox-app.vindi.com.br/api/v1

# Chave de API privada (obrigatória)
# Obtida no painel Vindi Sandbox: Configurações > API
VINDI_API_KEY=<sua-chave-privada-sandbox-aqui>

# Base URL da API pública (opcional, usa fallback se não configurada)
VINDI_PUBLIC_BASE_URL=https://sandbox-app.vindi.com.br/api/v1

# Chave de API pública (obrigatória)
# Obtida no painel Vindi Sandbox: Configurações > API > Public Key
# DIFERENTE da VINDI_API_KEY
VINDI_PUBLIC_KEY=<sua-chave-publica-sandbox-aqui>

# Flag opcional para forçar ambiente sandbox
VINDI_SANDBOX=true
```

## Validações Automáticas

O backend valida automaticamente no boot:

✅ **VINDI_API_BASE_URL**:
- Se configurada e não começar com `http://` ou `https://`: WARNING e usa fallback
- Se não configurada: usa fallback baseado em `VINDI_SANDBOX` ou `NODE_ENV`

✅ **VINDI_PUBLIC_BASE_URL**:
- Se configurada e não começar com `http://` ou `https://`: WARNING e usa fallback
- Se não configurada: usa fallback baseado em `VINDI_SANDBOX` ou `NODE_ENV`

✅ **VINDI_API_KEY**:
- Não pode estar vazia (erro crítico - serviço não inicia)

✅ **VINDI_PUBLIC_KEY**:
- Não pode estar vazia (erro crítico - serviço não inicia)

✅ **Chaves não podem ser iguais**:
- Se `VINDI_API_KEY === VINDI_PUBLIC_KEY`: erro crítico - serviço não inicia

## Erros Comuns

❌ **VINDI_API_BASE_URL com valor de chave**:
```
VINDI_API_BASE_URL=abc123def456...  # ERRADO - não começa com http
```
✅ **Correto**:
```
VINDI_API_BASE_URL=https://app.vindi.com.br/api/v1
```

❌ **Chaves iguais**:
```
VINDI_API_KEY=abc123
VINDI_PUBLIC_KEY=abc123  # ERRADO - são iguais
```
✅ **Correto**:
```
VINDI_API_KEY=abc123...  # Chave privada
VINDI_PUBLIC_KEY=xyz789...  # Chave pública (diferente)
```

## Logs no Boot

O backend loga (sem expor valores completos):
```
[boot] ✅ Vindi Config validada (PRODUÇÃO):
  VINDI_API_BASE_URL: app.vindi.com.br
  VINDI_API_KEY: abcd...wxyz (64 caracteres)
  VINDI_PUBLIC_BASE_URL: app.vindi.com.br
  VINDI_PUBLIC_KEY: 1234...5678 (64 caracteres)
```

Se houver WARNING:
```
[boot] Vindi Config Warnings (PRODUÇÃO):
  ⚠️  VINDI_API_BASE_URL inválida (não começa com http): "abcd...wxyz". Usando fallback: https://app.vindi.com.br/api/v1
```

