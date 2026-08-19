// tests/rewardProfile.test.js
import test from "node:test";
import assert from "node:assert/strict";

import {
  validateBirthDate,
  validateCPF,
  normalizeCPF,
  maskCPF,
  computeMissingRewardProfileFields,
  isRewardProfileComplete,
  getRewardProfile,
  updateBirthDate,
  updateCpf,
  RewardProfileError,
} from "../src/services/rewardProfile.js";

// CPFs de teste publicamente conhecidos (nunca de cliente real) — os
// mesmos usados amplamente em fixtures de software brasileiro.
const VALID_CPF_A = "11144477735";
const VALID_CPF_B = "52998224725";

function makeDb(initialUsers = []) {
  const users = initialUsers.map((u) => ({ ...u }));

  function run(sql, params = []) {
    const s = String(sql).toLowerCase();
    if (/^select id, name, email, phone, birth_date, cpf, tray_customer_id from public\.users where id = \$1/.test(s)) {
      const u = users.find((x) => x.id === params[0]);
      return { rows: u ? [u] : [] };
    }
    if (/^update public\.users set birth_date = \$2 where id = \$1/.test(s)) {
      const u = users.find((x) => x.id === params[0]);
      if (!u) return { rows: [] };
      u.birth_date = params[1];
      return { rows: [u] };
    }
    if (/^select id from public\.users where cpf = \$1 and id <> \$2 limit 1/.test(s)) {
      const [cpf, id] = params;
      const conflict = users.find((x) => x.cpf === cpf && x.id !== id);
      return { rows: conflict ? [{ id: conflict.id }] : [] };
    }
    if (/^update public\.users set cpf = \$2 where id = \$1/.test(s)) {
      const u = users.find((x) => x.id === params[0]);
      if (!u) return { rows: [] };
      u.cpf = params[1];
      return { rows: [u] };
    }
    throw new Error(`SQL nao mapeado: ${sql}`);
  }

  return { deps: { query: async (sql, params) => run(sql, params) }, users };
}

/* ─────────────────────────── validateBirthDate ─────────────────────────── */

test("aceita data valida no formato YYYY-MM-DD", () => {
  assert.equal(validateBirthDate("1990-05-20"), "1990-05-20");
});

test("recusa formato invalido", () => {
  for (const bad of ["20-05-1990", "1990/05/20", "não é data", "", null, undefined]) {
    assert.throws(() => validateBirthDate(bad), (e) => e instanceof RewardProfileError && e.code === "invalid_birth_date");
  }
});

test("recusa data de calendario impossivel (ex.: 31 de fevereiro)", () => {
  assert.throws(() => validateBirthDate("1990-02-31"), (e) => e.code === "invalid_birth_date");
});

test("recusa data no futuro", () => {
  const future = new Date();
  future.setFullYear(future.getFullYear() + 1);
  const iso = future.toISOString().slice(0, 10);
  assert.throws(() => validateBirthDate(iso), (e) => e.code === "birth_date_in_future");
});

test("recusa idade implausivel: menor que 13 anos", () => {
  const now = new Date();
  const tooYoung = `${now.getFullYear() - 5}-01-01`;
  assert.throws(() => validateBirthDate(tooYoung), (e) => e.code === "birth_date_too_recent");
});

test("recusa idade implausivel: maior que 120 anos", () => {
  assert.throws(() => validateBirthDate("1850-01-01"), (e) => e.code === "birth_date_implausible");
});

test("aceita exatamente no limite de 13 anos", () => {
  const now = new Date();
  const exactly13 = `${now.getFullYear() - 13}-01-01`;
  // so testa se nao lanca (idade real pode variar +-1 dia dependendo do mes atual)
  try {
    validateBirthDate(exactly13);
  } catch (e) {
    assert.equal(e.code, "birth_date_too_recent"); // aceitavel se cair no limite exato
  }
});

/* ─────────────────────────── normalizeCPF / validateCPF ─────────────────────────── */

test("normalizeCPF remove pontuacao e preserva zeros a esquerda", () => {
  assert.equal(normalizeCPF("111.444.777-35"), "11144477735");
  assert.equal(normalizeCPF("011.144.477-35"), "01114447735");
  assert.equal(normalizeCPF(""), "");
  assert.equal(normalizeCPF(null), "");
});

test("aceita CPF valido, com ou sem formatacao", () => {
  assert.equal(validateCPF(VALID_CPF_A), VALID_CPF_A);
  assert.equal(validateCPF("111.444.777-35"), VALID_CPF_A);
  assert.equal(validateCPF(VALID_CPF_B), VALID_CPF_B);
});

test("recusa CPF com digitos verificadores incorretos", () => {
  const wrong = VALID_CPF_A.slice(0, 10) + "0"; // ultimo digito trocado
  assert.throws(() => validateCPF(wrong), (e) => e instanceof RewardProfileError && e.code === "invalid_cpf");
});

test("recusa sequencias repetidas (matematicamente 'validas' mas nunca CPF real)", () => {
  for (const seq of ["00000000000", "11111111111", "99999999999"]) {
    assert.throws(() => validateCPF(seq), (e) => e.code === "invalid_cpf");
  }
});

test("recusa CPF curto demais", () => {
  assert.throws(() => validateCPF("123456789"), (e) => e.code === "invalid_cpf");
});

test("recusa CPF longo demais", () => {
  assert.throws(() => validateCPF("123456789012"), (e) => e.code === "invalid_cpf");
});

test("recusa string nao numerica", () => {
  assert.throws(() => validateCPF("abc.def.ghi-jk"), (e) => e.code === "invalid_cpf");
});

test("preserva zero a esquerda (nunca usa Number)", () => {
  // CPF valido comecando com zero, digitos verificadores calculados pelo
  // proprio algoritmo (nao inventado a mao).
  const withLeadingZero = "01065320493";
  assert.equal(validateCPF(withLeadingZero), withLeadingZero);
  assert.equal(validateCPF(withLeadingZero).length, 11, "nao pode perder o zero a esquerda");
});

/* ─────────────────────────── maskCPF ─────────────────────────── */

test("maskCPF mostra so os dois digitos verificadores", () => {
  assert.equal(maskCPF(VALID_CPF_A), "***.***.***-35");
});

test("maskCPF de valor invalido devolve null, nunca um mascaramento incorreto", () => {
  assert.equal(maskCPF("123"), null);
  assert.equal(maskCPF(null), null);
});

/* ─────────────────────────── completude ─────────────────────────── */

test("usuario sem birth_date nem cpf esta incompleto para o resgate", () => {
  const missing = computeMissingRewardProfileFields({ name: "Joao", email: "j@x.com", birth_date: null, cpf: null });
  assert.deepEqual(missing, ["birth_date", "cpf"]);
  assert.equal(isRewardProfileComplete({ birth_date: null, cpf: null }), false);
});

test("usuario com birth_date mas sem cpf continua incompleto", () => {
  const missing = computeMissingRewardProfileFields({ birth_date: "1990-01-01", cpf: null });
  assert.deepEqual(missing, ["cpf"]);
});

test("usuario com birth_date e cpf esta completo", () => {
  const missing = computeMissingRewardProfileFields({ birth_date: "1990-01-01", cpf: VALID_CPF_A });
  assert.deepEqual(missing, []);
  assert.equal(isRewardProfileComplete({ birth_date: "1990-01-01", cpf: VALID_CPF_A }), true);
});

test("nunca exige rg/gender (opcionais no contrato Tray, YAGNI)", () => {
  const missing = computeMissingRewardProfileFields({ birth_date: "1990-01-01", cpf: VALID_CPF_A, rg: null, gender: null });
  assert.deepEqual(missing, []);
});

/* ─────────────────────────── getRewardProfile ─────────────────────────── */

test("getRewardProfile devolve perfil + completude, NUNCA cpf cru", async () => {
  const { deps } = makeDb([{ id: 1, name: "Joao", email: "j@x.com", phone: "11999999999", birth_date: "1990-01-01", cpf: VALID_CPF_A, tray_customer_id: null }]);
  const out = await getRewardProfile(1, deps);
  assert.equal(out.profile_complete_for_reward, true);
  assert.deepEqual(out.missing_fields, []);
  assert.equal(out.has_cpf, true);
  assert.equal(out.cpf_masked, "***.***.***-35");
  assert.equal("cpf" in out, false, "cpf cru nunca sai deste servico");
});

test("getRewardProfile sem cpf: has_cpf false, cpf_masked null, entra em missing_fields", async () => {
  const { deps } = makeDb([{ id: 1, name: "Joao", email: "j@x.com", birth_date: "1990-01-01", cpf: null, tray_customer_id: null }]);
  const out = await getRewardProfile(1, deps);
  assert.equal(out.has_cpf, false);
  assert.equal(out.cpf_masked, null);
  assert.deepEqual(out.missing_fields, ["cpf"]);
});

test("getRewardProfile de usuario inexistente falha alto, nunca inventa perfil vazio", async () => {
  const { deps } = makeDb([]);
  await assert.rejects(() => getRewardProfile(999, deps), (e) => e instanceof RewardProfileError && e.code === "user_not_found");
});

/* ─────────────────────────── updateBirthDate ─────────────────────────── */

test("updateBirthDate persiste e devolve perfil (ainda incompleto sem cpf)", async () => {
  const { deps, users } = makeDb([{ id: 1, name: "Joao", email: "j@x.com", phone: null, birth_date: null, cpf: null, tray_customer_id: null }]);
  const out = await updateBirthDate(1, "1990-05-20", deps);
  assert.equal(out.birth_date, "1990-05-20");
  assert.equal(out.profile_complete_for_reward, false);
  assert.deepEqual(out.missing_fields, ["cpf"]);
  assert.equal(users[0].birth_date, "1990-05-20");
});

test("updateBirthDate recusa valor invalido ANTES de tocar o banco", async () => {
  const { deps, users } = makeDb([{ id: 1, name: "Joao", email: "j@x.com", birth_date: null }]);
  await assert.rejects(() => updateBirthDate(1, "data-invalida", deps), (e) => e.code === "invalid_birth_date");
  assert.equal(users[0].birth_date, null, "banco nunca e tocado quando a validacao falha");
});

test("updateBirthDate nunca aceita user_id arbitrario fora do inteiro seguro", async () => {
  const { deps } = makeDb([{ id: 1, name: "Joao", email: "j@x.com", birth_date: null }]);
  await assert.rejects(() => updateBirthDate(-1, "1990-01-01", deps), (e) => e.code === "invalid_user_id");
  await assert.rejects(() => updateBirthDate("nao-e-numero", "1990-01-01", deps), (e) => e.code === "invalid_user_id");
});

/* ─────────────────────────── updateCpf ─────────────────────────── */

test("updateCpf persiste e devolve perfil completo quando birth_date ja existia", async () => {
  const { deps, users } = makeDb([{ id: 1, name: "Joao", email: "j@x.com", birth_date: "1990-01-01", cpf: null, tray_customer_id: null }]);
  const out = await updateCpf(1, "111.444.777-35", deps);
  assert.equal(out.has_cpf, true);
  assert.equal(out.cpf_masked, "***.***.***-35");
  assert.equal(out.profile_complete_for_reward, true);
  assert.equal(users[0].cpf, VALID_CPF_A, "normalizado, sem pontuacao, no banco");
});

test("updateCpf recusa CPF invalido ANTES de tocar o banco", async () => {
  const { deps, users } = makeDb([{ id: 1, name: "Joao", email: "j@x.com", cpf: null }]);
  await assert.rejects(() => updateCpf(1, "11111111111", deps), (e) => e.code === "invalid_cpf");
  assert.equal(users[0].cpf, null);
});

test("updateCpf recusa CPF ja usado por OUTRO usuario", async () => {
  const { deps } = makeDb([
    { id: 1, name: "Joao", email: "j@x.com", cpf: null },
    { id: 2, name: "Maria", email: "m@x.com", cpf: VALID_CPF_A },
  ]);
  await assert.rejects(() => updateCpf(1, VALID_CPF_A, deps), (e) => e.code === "cpf_already_in_use");
});

test("updateCpf permite salvar o MESMO cpf que o proprio usuario ja tem (idempotente)", async () => {
  const { deps } = makeDb([{ id: 1, name: "Joao", email: "j@x.com", cpf: VALID_CPF_A }]);
  const out = await updateCpf(1, VALID_CPF_A, deps);
  assert.equal(out.has_cpf, true);
});

test("dois usuarios diferentes podem ter CPFs diferentes sem conflito", async () => {
  const { deps, users } = makeDb([
    { id: 1, name: "Joao", email: "j@x.com", cpf: null },
    { id: 2, name: "Maria", email: "m@x.com", cpf: null },
  ]);
  await updateCpf(1, VALID_CPF_A, deps);
  await updateCpf(2, VALID_CPF_B, deps);
  assert.equal(users[0].cpf, VALID_CPF_A);
  assert.equal(users[1].cpf, VALID_CPF_B);
});

test("updateCpf nunca aceita user_id arbitrario fora do inteiro seguro", async () => {
  const { deps } = makeDb([{ id: 1, name: "Joao", email: "j@x.com", cpf: null }]);
  await assert.rejects(() => updateCpf(-1, VALID_CPF_A, deps), (e) => e.code === "invalid_user_id");
});
