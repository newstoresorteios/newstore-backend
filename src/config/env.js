import dotenv from "dotenv";

dotenv.config();

// override:false (o padrao do dotenv) e proposital: uma env var ja definida
// no processo (export DATABASE_URL=... antes de rodar node) tem que vencer
// o .env.local, nunca o contrario. Um `override:true` aqui ja fez migrations
// de teste caírem em produção porque .env.local::DATABASE_URL sobrescrevia
// silenciosamente o que a shell tinha exportado.
if (process.env.NODE_ENV !== "production") {
  dotenv.config({ path: ".env.local" });
}
