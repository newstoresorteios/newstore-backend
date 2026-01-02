# Instruções de Deploy - Correção Vindi Tokenização

## Resumo das Alterações

Correções implementadas para eliminar erro 422 da Vindi relacionado a `payment_company_id`:

1. **`payment_company_id` nunca é enviado como null/undefined** - Apenas números válidos (> 0) são incluídos no payload
2. **`payment_company_code` sempre enviado quando disponível** - Prioriza frontend, senão detecta automaticamente
3. **`VINDI_API_KEY` não bloqueia tokenização** - Se não configurado, apenas loga warning e continua
4. **Logs melhorados** - Mostram claramente o que foi recebido e enviado
5. **Tratamento de erros 422** - Repassa `error_parameters` e `error_messages` para o frontend

## Variáveis de Ambiente Necessárias

### Obrigatórias:
- `VINDI_PUBLIC_KEY` - Chave pública para tokenização (endpoint `/public/payment_profiles`)

### Opcionais (recomendado):
- `VINDI_API_KEY` - Chave privada para consultar `/payment_methods` e resolver `payment_company_id`
  - Se não configurado, tokenização funciona normalmente usando apenas `payment_company_code`
  - Apenas loga warning (não bloqueia)

## Arquivos Modificados

1. `src/services/vindi_public.js`
   - Validação rigorosa de `payment_company_id` (só envia se número válido)
   - Logs melhorados com `payment_company_code_received` e `payment_company_code_sent`
   - Tratamento de erros 422 com contexto de `payment_company_id`

2. `src/routes/autopay_vindi.js`
   - Remove `payment_company_id` do payload se for null/undefined/0
   - Logs mostram claramente o que será enviado
   - Tratamento de erros com `error_parameters`

3. `src/services/vindi_payment_methods.js`
   - `VINDI_API_KEY` não bloqueia se não configurado
   - Retorna `null` graciosamente sem lançar erro

4. `src/test_vindi_payload.js` (novo)
   - Script de teste para validar comportamento do payload

## Deploy no Render

### Passo 1: Verificar Variáveis de Ambiente

No painel do Render, verifique se existe:
- ✅ `VINDI_PUBLIC_KEY` (obrigatório)
- ⚠️ `VINDI_API_KEY` (opcional, mas recomendado)

### Passo 2: Adicionar VINDI_API_KEY (se não existir)

1. Acesse o dashboard do Render
2. Vá em **Environment** → **Environment Variables**
3. Adicione:
   - **Key**: `VINDI_API_KEY`
   - **Value**: Sua chave privada da Vindi (API Key, não Public Key)
4. Salve

### Passo 3: Deploy

1. Faça commit das alterações
2. Push para o repositório
3. Render fará deploy automático
4. Ou faça deploy manual via dashboard

### Passo 4: Verificar Logs

Após o deploy, monitore os logs para confirmar:

```
✓ [autopay/vindi/tokenize] iniciando tokenização
  - payment_company_code_received: "elo"
  - payment_company_code_sent_to_vindi: "elo"
  - payment_company_id_sent: null (ou número se resolvido)

✓ [vindiPublic] chamando Vindi Public API - request final
  - payment_company_code_received: "elo"
  - payment_company_code_sent: "elo"
  - payment_company_id_sent: null (ou número se válido)
```

Se `VINDI_API_KEY` não estiver configurado, você verá:
```
⚠ [vindiPaymentMethods] VINDI_API_KEY não configurado - payment_company_id não será resolvido (tokenização pública continuará normalmente)
```

## Testes

### Teste Manual

1. Faça POST para `/api/autopay/vindi/tokenize` com:
```json
{
  "holder_name": "João Silva",
  "card_number": "6504123456789012",
  "card_expiration": "05/33",
  "card_cvv": "123",
  "payment_company_code": "elo"
}
```

2. Verifique nos logs:
   - `payment_company_code_sent_to_vindi: "elo"`
   - `payment_company_id_sent: null` (ou número se `VINDI_API_KEY` configurado)
   - Request não deve conter chave `payment_company_id` se for null

3. Resposta deve ser 200 com `gateway_token` (não mais 422)

### Teste Unitário

Execute o script de teste:
```bash
node src/test_vindi_payload.js
```

## Critérios de Aceite

✅ Request final enviado à Vindi **NÃO contém** `payment_company_id` quando não resolvido  
✅ Tokenização com `payment_company_code="elo"` **não retorna** erro "payment_company_id não pode ficar em branco"  
✅ Logs mostram claramente:
   - `payment_company_code_received` (frontend/backend)
   - `payment_company_code_sent`
   - `payment_company_id_sent` (apenas se existir)
   - Warning sobre `VINDI_API_KEY` (se não configurado)  
✅ Nenhum dado sensível exposto em logs (cartão mascarado: primeiros 4 + últimos 4)

## Rollback (se necessário)

Se houver problemas, reverta para commit anterior:
```bash
git revert HEAD
git push
```

Ou restaure variáveis de ambiente anteriores no Render.

## Suporte

Em caso de dúvidas ou problemas:
1. Verifique logs do Render
2. Confirme que `VINDI_PUBLIC_KEY` está configurado
3. Teste com cartão Elo (6504...) e `payment_company_code="elo"`
4. Verifique se o payload não contém `payment_company_id: null`

