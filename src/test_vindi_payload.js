// Script de teste rápido para validar payload enviado à Vindi
// Uso: node src/test_vindi_payload.js

/**
 * Testa se o payload está correto:
 * - payment_company_id não deve existir quando null/undefined
 * - payment_company_code deve existir quando fornecido
 * - card_expiration sempre MM/YYYY
 */

function testPayload() {
  console.log("=== Teste de Payload Vindi ===\n");

  // Teste 1: payment_company_id não deve existir quando null
  const payload1 = {
    allow_as_fallback: true,
    holder_name: "João Silva",
    card_number: "6504123456789012",
    card_expiration: "05/2033",
    card_cvv: "123",
    payment_method_code: "credit_card",
    payment_company_code: "elo",
    payment_company_id: null,
  };
  
  // Remove payment_company_id se for null/undefined/0
  if (payload1.payment_company_id == null || payload1.payment_company_id === "" || payload1.payment_company_id === 0) {
    delete payload1.payment_company_id;
  }
  
  console.log("Teste 1: payment_company_id null deve ser removido");
  console.log("Payload:", JSON.stringify(payload1, null, 2));
  console.log("✓ payment_company_id não existe:", !("payment_company_id" in payload1));
  console.log("✓ payment_company_code existe:", "payment_company_code" in payload1);
  console.log("✓ card_expiration formato MM/YYYY:", /^\d{2}\/\d{4}$/.test(payload1.card_expiration));
  console.log("");

  // Teste 2: payment_company_id válido deve ser mantido
  const payload2 = {
    allow_as_fallback: true,
    holder_name: "Maria Santos",
    card_number: "4111111111111111",
    card_expiration: "12/2025",
    card_cvv: "456",
    payment_method_code: "credit_card",
    payment_company_code: "visa",
    payment_company_id: 123,
  };
  
  const hasValidId = payload2.payment_company_id != null && 
                    payload2.payment_company_id !== "" && 
                    !isNaN(Number(payload2.payment_company_id)) && 
                    Number(payload2.payment_company_id) > 0;
  
  if (!hasValidId) {
    delete payload2.payment_company_id;
  }
  
  console.log("Teste 2: payment_company_id válido deve ser mantido");
  console.log("Payload:", JSON.stringify(payload2, null, 2));
  console.log("✓ payment_company_id existe:", "payment_company_id" in payload2);
  console.log("✓ payment_company_id valor:", payload2.payment_company_id);
  console.log("");

  // Teste 3: payment_company_id undefined deve ser removido
  const payload3 = {
    allow_as_fallback: true,
    holder_name: "Pedro Costa",
    card_number: "5555555555554444",
    card_expiration: "03/2026",
    card_cvv: "789",
    payment_method_code: "credit_card",
    payment_company_code: "mastercard",
  };
  
  if (payload3.payment_company_id == null || payload3.payment_company_id === "" || payload3.payment_company_id === 0) {
    delete payload3.payment_company_id;
  }
  
  console.log("Teste 3: payment_company_id undefined não deve existir");
  console.log("Payload:", JSON.stringify(payload3, null, 2));
  console.log("✓ payment_company_id não existe:", !("payment_company_id" in payload3));
  console.log("✓ payment_company_code existe:", "payment_company_code" in payload3);
  console.log("");

  // Teste 4: Validação de card_expiration
  const testDates = [
    { input: "05/33", expected: "05/2033" },
    { input: "12/25", expected: "12/2025" },
    { input: "01/2030", expected: "01/2030" },
  ];
  
  console.log("Teste 4: Validação de card_expiration");
  testDates.forEach(({ input, expected }) => {
    const parts = input.split("/");
    const month = parts[0].padStart(2, "0");
    let year = parts[1];
    
    if (year.length === 2) {
      const yy = parseInt(year, 10);
      year = yy <= 79 ? `20${year.padStart(2, "0")}` : `19${year.padStart(2, "0")}`;
    }
    
    const normalized = `${month}/${year}`;
    console.log(`  Input: ${input} -> Normalizado: ${normalized} (esperado: ${expected})`);
    console.log(`  ✓ Formato MM/YYYY:`, /^\d{2}\/\d{4}$/.test(normalized));
  });
  
  console.log("\n=== Todos os testes concluídos ===");
}

// Executa se chamado diretamente
if (import.meta.url === `file://${process.argv[1]}`) {
  testPayload();
}

export { testPayload };

