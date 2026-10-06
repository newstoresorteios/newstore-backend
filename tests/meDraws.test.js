import assert from "node:assert/strict";
import test from "node:test";

process.env.JWT_SECRET = process.env.JWT_SECRET || "test-secret-me-draws";

const meDraws = await import("../src/routes/me_draws.js");

const normalizeSql = (sql) => String(sql).replace(/\s+/g, " ").trim();
const seq = (from, to) => Array.from({ length: to - from + 1 }, (_, i) => from + i);
const sets = (taken = [], reserved = [], mine = []) => ({
  taken: new Set(taken),
  reserved: new Set(reserved),
  mine: new Set(mine),
});

test("winner_name prioriza draws.winner_name, depois nome do usuario, sem expor e-mail", async () => {
  const calls = [];
  const queryFn = async (sql, params) => {
    calls.push({ sql: normalizeSql(sql), params });
    return { rows: [{ id: 133, winner_name: "Nome Persistido" }] };
  };

  assert.equal(typeof meDraws.loadDrawForBoard, "function");
  const row = await meDraws.loadDrawForBoard(133, queryFn);

  assert.equal(row.winner_name, "Nome Persistido");
  assert.deepEqual(calls[0].params, [133]);
  assert.match(
    calls[0].sql,
    /COALESCE\(NULLIF\(d\.winner_name, ''\), NULLIF\(u\.name, ''\)\) AS winner_name/i
  );
  assert.doesNotMatch(calls[0].sql, /u\.email/i);
});

test("loadDrawForBoard devolve null quando o sorteio nao existe", async () => {
  assert.equal(await meDraws.loadDrawForBoard(1, async () => ({ rows: [] })), null);
});

test("loadBoardNumbers le public.numbers do draw ordenado por n", async () => {
  const calls = [];
  const numbers = await meDraws.loadBoardNumbers(150, async (sql, params) => {
    calls.push({ sql: normalizeSql(sql), params });
    return { rows: [{ n: "2" }, { n: 0 }, { n: 1 }] };
  });
  assert.deepEqual(numbers, [2, 0, 1]);
  assert.deepEqual(calls[0].params, [150]);
  assert.match(calls[0].sql, /FROM public\.numbers WHERE draw_id = \$1 ORDER BY n ASC/);
});

test("board 0-99 mantem 100 posicoes com labels 00..99", () => {
  const board = meDraws.buildBoard({ numbers: seq(0, 99), ...sets(), winner: null });
  assert.equal(board.length, 100);
  assert.equal(board[0].label, "00");
  assert.equal(board[7].label, "07");
  assert.equal(board[99].label, "99");
  assert.deepEqual(Object.keys(board[0]).sort(), ["isMine", "isWinner", "label", "n", "state"]);
  assert.ok(board.every((item) => item.state === "available" && !item.isWinner && !item.isMine));
});

test("board com mais de 100 numeros usa labels de 3 digitos e nao assume 100 posicoes", () => {
  const board500 = meDraws.buildBoard({ numbers: seq(0, 499), ...sets(), winner: null });
  assert.equal(board500.length, 500);
  assert.equal(board500[0].label, "000");
  assert.equal(board500[1].label, "001");
  assert.equal(board500[499].label, "499");

  const board1000 = meDraws.buildBoard({ numbers: seq(0, 999), ...sets(), winner: null });
  assert.equal(board1000.length, 1000);
  assert.equal(board1000[999].label, "999");
});

test("board apenas representa os registros existentes, ordenados por n", () => {
  const board = meDraws.buildBoard({ numbers: [5, 0, 3], ...sets(), winner: null });
  assert.deepEqual(board.map((item) => item.n), [0, 3, 5]);
  assert.deepEqual(board.map((item) => item.label), ["00", "03", "05"]);
  assert.deepEqual(meDraws.buildBoard({ numbers: [], ...sets(), winner: 3 }), []);
});

test("winner_number = 0 e vencedor valido", () => {
  const board = meDraws.buildBoard({ numbers: seq(0, 99), ...sets(), winner: 0 });
  assert.equal(board[0].isWinner, true);
  assert.equal(board.filter((item) => item.isWinner).length, 1);
  const board500 = meDraws.buildBoard({ numbers: seq(0, 499), ...sets(), winner: "0" });
  assert.equal(board500[0].isWinner, true);
});

test("isWinner so marca o numero sorteado e nada quando nao ha vencedor", () => {
  const withWinner = meDraws.buildBoard({ numbers: seq(0, 99), ...sets(), winner: 16 });
  assert.deepEqual(withWinner.filter((item) => item.isWinner).map((item) => item.n), [16]);
  for (const winner of [null, undefined]) {
    const none = meDraws.buildBoard({ numbers: seq(0, 99), ...sets(), winner });
    assert.ok(none.every((item) => item.isWinner === false));
  }
});

test("isMine e state seguem a regra existente (mine/taken -> taken, reserved, available)", () => {
  const board = meDraws.buildBoard({
    numbers: seq(0, 9),
    ...sets([1, 2], [2, 3], [4]),
    winner: null,
  });
  const byN = Object.fromEntries(board.map((item) => [item.n, item]));
  assert.equal(byN[0].state, "available");
  assert.equal(byN[1].state, "taken");
  assert.equal(byN[2].state, "taken");
  assert.equal(byN[3].state, "reserved");
  assert.equal(byN[4].state, "taken");
  assert.equal(byN[4].isMine, true);
  assert.equal(byN[1].isMine, false);
});
