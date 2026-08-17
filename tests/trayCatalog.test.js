// tests/trayCatalog.test.js
// Normalizacao do catalogo Tray. Funcoes puras: preservam o dado factual.
import test from "node:test";
import assert from "node:assert/strict";

import {
  normalizeTrayProduct,
  normalizeTrayVariants,
  pickPrimaryImage,
  collectImages,
  derivePresentation,
} from "../src/services/trayCatalog.js";

const RAW_PRODUCT = {
  id: 123,
  name: "Citizen Promaster",
  description_small: "Relogio de mergulho",
  reference: "NY0129",
  brand: "Citizen",
  brand_id: 9,
  available: 1,
  available_in_store: 1,
  stock: 3,
  availability: "Disponivel - Envio em 1 dia util",
  has_variation: 0,
  price: "4599.00",
  modified: "2026-08-10 14:32:11",
  url: { http: "http://loja/produto-123", https: "https://loja/produto-123" },
  ProductImage: [
    { http: "http://cdn/img1.jpg", https: "https://cdn/img1.jpg" },
    { http: "http://cdn/img2.jpg", https: "https://cdn/img2.jpg" },
  ],
  ProductSettings: { when_stock_runs_out: "deactivate_product", order_days_availability: 0 },
};

test("preserva os campos factuais da Tray sem alterar valores", () => {
  const p = normalizeTrayProduct(RAW_PRODUCT);

  assert.equal(p.tray_product_id, "123");
  assert.equal(p.name, "Citizen Promaster");
  assert.equal(p.reference, "NY0129");
  assert.equal(p.brand, "Citizen");
  assert.equal(p.description_small, "Relogio de mergulho");

  // Factuais: nao convertidos, nao "corrigidos".
  assert.equal(p.tray_available, 1);
  assert.equal(p.tray_available_in_store, 1);
  assert.equal(p.stock, 3);
  assert.equal(p.availability_text, "Disponivel - Envio em 1 dia util");
  assert.equal(p.has_variation, false);
  assert.equal(p.when_stock_runs_out, "deactivate_product");
  assert.equal(p.order_days_availability, 0);
  assert.equal(p.tray_price, 4599);
  assert.equal(p.tray_product_url, "https://loja/produto-123");
});

test("id da Tray e sempre string (0 e um id valido de nada, mas 0 nao pode virar vazio)", () => {
  assert.equal(normalizeTrayProduct({ id: 456 }).tray_product_id, "456");
  assert.equal(normalizeTrayProduct({ id: "789" }).tray_product_id, "789");
  assert.equal(normalizeTrayProduct({}).tray_product_id, null);
});

test("has_variation 1 vira boolean true", () => {
  assert.equal(normalizeTrayProduct({ ...RAW_PRODUCT, has_variation: 1 }).has_variation, true);
  assert.equal(normalizeTrayProduct({ ...RAW_PRODUCT, has_variation: "1" }).has_variation, true);
  assert.equal(normalizeTrayProduct({ ...RAW_PRODUCT, has_variation: 0 }).has_variation, false);
});

test("imagem principal e deterministica: primeira ProductImage, preferindo https", () => {
  assert.equal(pickPrimaryImage(RAW_PRODUCT), "https://cdn/img1.jpg");

  assert.equal(
    pickPrimaryImage({ ProductImage: [{ http: "http://cdn/only-http.jpg" }] }),
    "http://cdn/only-http.jpg"
  );

  // Formato aninhado tambem e aceito.
  assert.equal(
    pickPrimaryImage({ ProductImage: [{ ProductImage: { https: "https://cdn/nested.jpg" } }] }),
    "https://cdn/nested.jpg"
  );
});

test("produto sem imagem nao inventa imagem", () => {
  assert.equal(pickPrimaryImage({}), null);
  assert.equal(pickPrimaryImage({ ProductImage: [] }), null);
  assert.deepEqual(collectImages({}), []);
  assert.equal(normalizeTrayProduct({ id: 1 }).image_url, null);
});

test("collectImages preserva todas as imagens retornadas, na ordem", () => {
  assert.deepEqual(collectImages(RAW_PRODUCT), ["https://cdn/img1.jpg", "https://cdn/img2.jpg"]);
});

test("disponibilidade NAO e apenas stock > 0", () => {
  // Tray diz disponivel, estoque zerado, mas a loja continua vendendo por encomenda.
  const encomenda = derivePresentation({
    tray_available: 1,
    tray_available_in_store: 1,
    stock: 0,
    when_stock_runs_out: "continue_selling_immediate",
  });
  assert.equal(encomenda.is_available, true);
  assert.equal(encomenda.reason, "available_on_demand");

  const prazoEstendido = derivePresentation({
    tray_available: 1,
    tray_available_in_store: 1,
    stock: 0,
    when_stock_runs_out: "sell_extended_lead_time",
  });
  assert.equal(prazoEstendido.is_available, true);
  assert.equal(prazoEstendido.reason, "available_extended_lead_time");
});

test("estoque zerado com prazo de entrega e venda sob encomenda, nao indisponivel", () => {
  // Loja real, produto 15526: stock 0, available 1, "Disponível em 45 dias úteis".
  // A Tray afirma que esta disponivel; a NewStore nao pode sobrepor isso.
  const out = derivePresentation({
    tray_available: 1,
    tray_available_in_store: 1,
    stock: 0,
    when_stock_runs_out: null,
    availability_days: 45,
  });
  assert.equal(out.is_available, true);
  assert.equal(out.reason, "available_extended_lead_time");
});

test("estoque zerado SEM prazo e sem regra continua indisponivel", () => {
  assert.deepEqual(
    derivePresentation({ tray_available: 1, tray_available_in_store: 1, stock: 0, availability_days: 0 }),
    { is_available: false, reason: "out_of_stock" }
  );
  assert.deepEqual(
    derivePresentation({ tray_available: 1, tray_available_in_store: 1, stock: 0 }),
    { is_available: false, reason: "out_of_stock" }
  );
});

test("listagem e detalhe concordam mesmo a listagem omitindo availability_days", () => {
  // Divergencia factual entre endpoints da Tray: GET /products nao devolve
  // availability_days; GET /products/:id devolve. O texto vem nos dois.
  const daListagem = derivePresentation({
    tray_available: 1,
    tray_available_in_store: 1,
    stock: 0,
    availability_days: null,
    availability_text: "Disponível em 45 dias úteis",
  });
  const doDetalhe = derivePresentation({
    tray_available: 1,
    tray_available_in_store: 1,
    stock: 0,
    availability_days: 45,
    availability_text: "Disponível em 45 dias úteis",
  });

  assert.deepEqual(daListagem, doDetalhe);
  assert.equal(daListagem.is_available, true);
  assert.equal(daListagem.reason, "available_extended_lead_time");
});

test("prazo de entrega NAO ressuscita produto que a Tray desligou", () => {
  assert.deepEqual(
    derivePresentation({ tray_available: 0, tray_available_in_store: 1, stock: 0, availability_days: 30 }),
    { is_available: false, reason: "unavailable_in_tray" }
  );
});

test("disponibilidade respeita o desligamento factual da Tray", () => {
  assert.deepEqual(
    derivePresentation({ tray_available: 0, tray_available_in_store: 1, stock: 10 }),
    { is_available: false, reason: "unavailable_in_tray" }
  );

  assert.deepEqual(
    derivePresentation({ tray_available: 1, tray_available_in_store: 0, stock: 10 }),
    { is_available: false, reason: "hidden_in_store" }
  );

  assert.deepEqual(
    derivePresentation({
      tray_available: 1,
      tray_available_in_store: 1,
      stock: 0,
      when_stock_runs_out: "deactivate_product",
    }),
    { is_available: false, reason: "out_of_stock" }
  );

  assert.deepEqual(
    derivePresentation({ tray_available: 1, tray_available_in_store: 1, stock: 5 }),
    { is_available: true, reason: "available" }
  );
});

test("produto com variacao e disponivel quando alguma variacao tem estoque", () => {
  const out = derivePresentation({
    tray_available: 1,
    tray_available_in_store: 1,
    stock: 0,
    has_variation: true,
    variants: [
      { id: 1, stock: 0, available: 1 },
      { id: 2, stock: 4, available: 1 },
    ],
  });
  assert.equal(out.is_available, true);
  assert.equal(out.reason, "available_in_variant");
});

test("variacoes preservam estoque e disponibilidade factuais de cada uma", () => {
  const variants = normalizeTrayVariants([
    { Variant: { id: 10, product_id: 123, reference: "TAM-40", price: "199.90", stock: 0, available: 1, minimum_stock: 0 } },
    { id: 11, product_id: 123, reference: "TAM-41", price: "199.90", stock: 7, available: 1 },
  ]);

  assert.equal(variants.length, 2);
  assert.deepEqual(variants[0], {
    variant_id: "10",
    tray_product_id: "123",
    reference: "TAM-40",
    ean: null,
    price: 199.9,
    stock: 0,
    minimum_stock: 0,
    tray_available: 1,
    values: [],
  });
  assert.equal(variants[1].stock, 7);
  assert.equal(variants[1].variant_id, "11");
});

test("variacoes trazem os valores (tamanho/cor) quando a Tray retorna", () => {
  const variants = normalizeTrayVariants([
    {
      Variant: {
        id: 20,
        product_id: 5,
        stock: 1,
        available: 1,
        VariantValue: [
          { VariantValue: { type: "Tamanho", value: "40" } },
          { type: "Cor", value: "Preto" },
        ],
      },
    },
  ]);

  assert.deepEqual(variants[0].values, [
    { type: "Tamanho", value: "40" },
    { type: "Cor", value: "Preto" },
  ]);
});

test("normalizeTrayVariants aceita entrada vazia sem quebrar", () => {
  assert.deepEqual(normalizeTrayVariants(null), []);
  assert.deepEqual(normalizeTrayVariants([]), []);
  assert.deepEqual(normalizeTrayVariants("nao e array"), []);
});

test("produto normalizado ja carrega a apresentacao derivada", () => {
  const p = normalizeTrayProduct(RAW_PRODUCT);
  assert.deepEqual(p.presentation, { is_available: true, reason: "available" });
});

test("data de modificacao da Tray vira ISO sem perder a informacao", () => {
  const p = normalizeTrayProduct(RAW_PRODUCT);
  assert.equal(p.tray_modified_at, "2026-08-10T14:32:11.000Z");
  assert.equal(normalizeTrayProduct({ id: 1 }).tray_modified_at, null);
  assert.equal(normalizeTrayProduct({ id: 1, modified: "0000-00-00 00:00:00" }).tray_modified_at, null);
});

test("payload real da loja: a Tray devolve os numeros como STRING", () => {
  // Capturado da loja real (produto 14570): stock 37, mas available "0".
  // Prova factual de que disponibilidade NAO e stock > 0.
  const raw = {
    id: "14570",
    name: "Bone Laco Basecap Cinza 691158",
    reference: "691158",
    brand: "Laco",
    price: "499.99",
    stock: "37",
    available: "0",
    available_in_store: "0",
    availability: "Disponível em 30 dias úteis",
    availability_days: "30",
    has_variation: "0",
    ProductSettings: { when_stock_runs_out: "", order_days_availability: "" },
  };

  const p = normalizeTrayProduct(raw);

  assert.equal(p.tray_product_id, "14570");
  assert.equal(p.stock, 37);
  assert.equal(p.tray_available, 0);
  assert.equal(p.tray_available_in_store, 0);
  assert.equal(p.availability_days, 30);
  assert.equal(p.availability_text, "Disponível em 30 dias úteis");
  assert.equal(p.has_variation, false);
  assert.equal(p.when_stock_runs_out, null, "a loja devolve string vazia, nao um valor");
  assert.equal(p.tray_price, 499.99);

  // Com estoque 37 e available 0, o produto NAO pode aparecer como disponivel.
  assert.deepEqual(p.presentation, { is_available: false, reason: "unavailable_in_tray" });
});

test("payload real de variacao: valores em string sao normalizados", () => {
  const variants = normalizeTrayVariants([{ Variant: { id: "1686", product_id: "15086", stock: "38", available: "1" } }]);
  assert.equal(variants[0].variant_id, "1686");
  assert.equal(variants[0].stock, 38);
  assert.equal(variants[0].tray_available, 1);
});

test("preco factual da Tray e numerico ou null, nunca NaN", () => {
  assert.equal(normalizeTrayProduct({ id: 1, price: "0,00" }).tray_price, null);
  assert.equal(normalizeTrayProduct({ id: 1, price: "" }).tray_price, null);
  assert.equal(normalizeTrayProduct({ id: 1, price: "12.34" }).tray_price, 12.34);
});
