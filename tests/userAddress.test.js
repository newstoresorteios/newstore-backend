// tests/userAddress.test.js
import test from "node:test";
import assert from "node:assert/strict";

import {
  listUserAddresses,
  createUserAddress,
  getUserAddress,
  deleteUserAddress,
  UserAddressError,
} from "../src/services/userAddress.js";

function makeDb() {
  const rows = [];
  let nextId = 1;

  function run(sql, params = []) {
    const s = String(sql).toLowerCase();

    if (/^select \* from public\.user_addresses where user_id = \$1 order/.test(s)) {
      return { rows: rows.filter((r) => r.user_id === params[0]) };
    }
    if (/^update public\.user_addresses set is_default = false/.test(s)) {
      rows.forEach((r) => { if (r.user_id === params[0]) r.is_default = false; });
      return { rows: [], rowCount: 0 };
    }
    if (/^insert into public\.user_addresses/.test(s)) {
      const [user_id, recipient_name, zipcode, street, number, complement, neighborhood, city, state, is_default] = params;
      const row = { id: String(nextId++), user_id, recipient_name, zipcode, street, number, complement, neighborhood, city, state, country: "BR", is_default, created_at: new Date().toISOString() };
      rows.push(row);
      return { rows: [row] };
    }
    if (/^select \* from public\.user_addresses where id = \$1 and user_id = \$2/.test(s)) {
      const r = rows.find((x) => x.id === String(params[0]) && x.user_id === params[1]);
      return { rows: r ? [r] : [] };
    }
    if (/^delete from public\.user_addresses where id = \$1 and user_id = \$2/.test(s)) {
      const idx = rows.findIndex((x) => x.id === String(params[0]) && x.user_id === params[1]);
      if (idx >= 0) { rows.splice(idx, 1); return { rowCount: 1 }; }
      return { rowCount: 0 };
    }
    throw new Error(`SQL nao mapeado: ${sql}`);
  }

  return { deps: { query: async (sql, params) => run(sql, params) }, rows };
}

const VALID = {
  recipient_name: "Joao Pedro",
  zipcode: "01304-001",
  street: "Rua Augusta",
  number: "123",
  neighborhood: "Consolacao",
  city: "Sao Paulo",
  state: "sp",
};

test("cria endereco com zipcode normalizado (so digitos)", async () => {
  const { deps } = makeDb();
  const a = await createUserAddress(1, VALID, deps);
  assert.equal(a.zipcode, "01304001");
  assert.equal(a.state, "SP");
  assert.equal(a.country, "BR");
});

test("zipcode invalido e recusado", async () => {
  const { deps } = makeDb();
  await assert.rejects(() => createUserAddress(1, { ...VALID, zipcode: "123" }, deps), (e) => e instanceof UserAddressError && e.code === "invalid_zipcode");
});

test("estado invalido e recusado", async () => {
  const { deps } = makeDb();
  await assert.rejects(() => createUserAddress(1, { ...VALID, state: "SPX" }, deps), (e) => e.code === "invalid_state");
});

test("campos obrigatorios ausentes sao recusados", async () => {
  const { deps } = makeDb();
  await assert.rejects(() => createUserAddress(1, { ...VALID, street: "" }, deps), (e) => e.code === "street_required");
});

test("marcar como padrao desmarca os outros do mesmo usuario", async () => {
  const { deps } = makeDb();
  const a = await createUserAddress(1, { ...VALID, is_default: true }, deps);
  const b = await createUserAddress(1, { ...VALID, number: "456", is_default: true }, deps);

  const list = await listUserAddresses(1, deps);
  assert.equal(list.find((x) => x.id === a.id).is_default, false);
  assert.equal(list.find((x) => x.id === b.id).is_default, true);
});

test("usuario nunca ve nem apaga endereco de outro", async () => {
  const { deps } = makeDb();
  const a = await createUserAddress(1, VALID, deps);

  assert.equal(await getUserAddress(2, a.id, deps), null);
  assert.equal(await deleteUserAddress(2, a.id, deps), false);

  const stillThere = await getUserAddress(1, a.id, deps);
  assert.ok(stillThere);
});

test("apagar o proprio endereco funciona", async () => {
  const { deps } = makeDb();
  const a = await createUserAddress(1, VALID, deps);
  assert.equal(await deleteUserAddress(1, a.id, deps), true);
  assert.equal(await getUserAddress(1, a.id, deps), null);
});
