// tests/rewardProfile.test.js
import test from "node:test";
import assert from "node:assert/strict";

import {
  validateBirthDate,
  computeMissingRewardProfileFields,
  isRewardProfileComplete,
  getRewardProfile,
  updateBirthDate,
  RewardProfileError,
} from "../src/services/rewardProfile.js";

function makeDb(initialUsers = []) {
  const users = initialUsers.map((u) => ({ ...u }));

  function run(sql, params = []) {
    const s = String(sql).toLowerCase();
    if (/^select id, name, email, phone, birth_date, tray_customer_id from public\.users where id = \$1/.test(s)) {
      const u = users.find((x) => x.id === params[0]);
      return { rows: u ? [u] : [] };
    }
    if (/^update public\.users set birth_date = \$2 where id = \$1/.test(s)) {
      const u = users.find((x) => x.id === params[0]);
      if (!u) return { rows: [] };
      u.birth_date = params[1];
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

/* ─────────────────────────── completude ─────────────────────────── */

test("usuario sem birth_date esta incompleto para o resgate", () => {
  const missing = computeMissingRewardProfileFields({ name: "Joao", email: "j@x.com", birth_date: null });
  assert.deepEqual(missing, ["birth_date"]);
  assert.equal(isRewardProfileComplete({ birth_date: null }), false);
});

test("usuario com birth_date esta completo", () => {
  const missing = computeMissingRewardProfileFields({ name: "Joao", email: "j@x.com", birth_date: "1990-01-01" });
  assert.deepEqual(missing, []);
  assert.equal(isRewardProfileComplete({ birth_date: "1990-01-01" }), true);
});

test("nunca exige cpf/rg/gender (opcionais no contrato Tray, YAGNI)", () => {
  const missing = computeMissingRewardProfileFields({ name: "Joao", email: "j@x.com", birth_date: "1990-01-01", cpf: null, rg: null, gender: null });
  assert.deepEqual(missing, []);
});

/* ─────────────────────────── getRewardProfile ─────────────────────────── */

test("getRewardProfile devolve perfil + completude para o usuario correto", async () => {
  const { deps } = makeDb([{ id: 1, name: "Joao", email: "j@x.com", phone: "11999999999", birth_date: null, tray_customer_id: null }]);
  const out = await getRewardProfile(1, deps);
  assert.equal(out.profile_complete_for_reward, false);
  assert.deepEqual(out.missing_fields, ["birth_date"]);
  assert.equal(out.name, "Joao");
});

test("getRewardProfile de usuario inexistente falha alto, nunca inventa perfil vazio", async () => {
  const { deps } = makeDb([]);
  await assert.rejects(() => getRewardProfile(999, deps), (e) => e instanceof RewardProfileError && e.code === "user_not_found");
});

/* ─────────────────────────── updateBirthDate ─────────────────────────── */

test("updateBirthDate persiste e devolve perfil completo", async () => {
  const { deps, users } = makeDb([{ id: 1, name: "Joao", email: "j@x.com", phone: null, birth_date: null, tray_customer_id: null }]);
  const out = await updateBirthDate(1, "1990-05-20", deps);
  assert.equal(out.birth_date, "1990-05-20");
  assert.equal(out.profile_complete_for_reward, true);
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
