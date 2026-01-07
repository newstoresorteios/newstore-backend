// Teste rápido da integração Vindi
// Uso: node src/test_vindi_payload.js
import "dotenv/config";
import { ensureCustomer, createPaymentProfileWithCardData } from "./services/vindi.js";

const TEST_EMAIL = process.env.TEST_EMAIL || "test@example.com";
const TEST_NAME = process.env.TEST_NAME || "Teste Usuario";

async function testVindiIntegration() {
  console.log("=".repeat(60));
  console.log("Teste de Integração Vindi");
  console.log("=".repeat(60));
  console.log();

  // Verifica variáveis de ambiente
  if (!process.env.VINDI_API_KEY) {
    console.error("❌ VINDI_API_KEY não configurada");
    process.exit(1);
  }

  const vindiBase = process.env.VINDI_API_BASE_URL || process.env.VINDI_API_URL || "https://app.vindi.com.br/api/v1";
  console.log(`📡 VINDI_BASE: ${vindiBase}`);
  console.log();

  try {
    // Teste 1: ensureCustomer
    console.log("1️⃣  Testando ensureCustomer...");
    console.log(`   Email: ${TEST_EMAIL}`);
    console.log(`   Nome: ${TEST_NAME}`);
    
    const customer = await ensureCustomer({
      email: TEST_EMAIL,
      name: TEST_NAME,
      code: `test_${Date.now()}`,
    });
    
    console.log(`   ✅ Customer ID: ${customer.customerId}`);
    console.log();

    // Teste 2: createPaymentProfileWithCardData (com dados fake)
    console.log("2️⃣  Testando createPaymentProfileWithCardData...");
    console.log("   ⚠️  Usando dados de cartão de teste (não válidos para cobrança)");
    
    // Dados de cartão de teste (não válidos para cobrança real)
    const testCard = {
      customerId: customer.customerId,
      holderName: TEST_NAME,
      cardNumber: "4111111111111111", // Cartão de teste Visa
      cardExpiration: "12/25",
      cardCvv: "123",
      paymentCompanyCode: "visa",
    };
    
    const paymentProfile = await createPaymentProfileWithCardData(testCard);
    
    console.log(`   ✅ Payment Profile ID: ${paymentProfile.paymentProfileId}`);
    console.log(`   ✅ Last 4: ${paymentProfile.lastFour}`);
    console.log(`   ✅ Card Type: ${paymentProfile.cardType || "N/A"}`);
    console.log(`   ✅ Payment Company Code: ${paymentProfile.paymentCompanyCode || "N/A"}`);
    console.log();

    console.log("=".repeat(60));
    console.log("✅ Testes concluídos com sucesso!");
    console.log("=".repeat(60));
    console.log();
    console.log("Resumo:");
    console.log(`  - Customer ID: ${customer.customerId}`);
    console.log(`  - Payment Profile ID: ${paymentProfile.paymentProfileId}`);
    console.log();

  } catch (error) {
    console.error();
    console.error("=".repeat(60));
    console.error("❌ Erro no teste:");
    console.error("=".repeat(60));
    console.error(`   Status: ${error?.status || "N/A"}`);
    console.error(`   Provider: ${error?.provider || "N/A"}`);
    console.error(`   Mensagem: ${error?.message || "Erro desconhecido"}`);
    
    if (error?.response?.errors) {
      console.error("   Detalhes da Vindi:");
      error.response.errors.forEach((err, idx) => {
        console.error(`     ${idx + 1}. ${err.message || "Sem mensagem"}`);
        if (err.parameter) {
          console.error(`        Campo: ${err.parameter}`);
        }
      });
    }
    
    console.error();
    process.exit(1);
  }
}

// Executa o teste
testVindiIntegration().catch((error) => {
  console.error("Erro fatal:", error);
  process.exit(1);
});
