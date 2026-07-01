import { Router } from "express";
import {
  authorizeCaptivePreauthByToken,
  declineCaptivePreauthByToken,
} from "../services/autopay/captivePreauthService.js";

const router = Router();

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function htmlResponse(res, title, message, status = 200) {
  return res.status(status).type("html").send(`<!doctype html>
<html lang="pt-BR">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width,initial-scale=1">
    <title>${escapeHtml(title)}</title>
    <style>
      body{margin:0;font-family:Inter,system-ui,-apple-system,Segoe UI,Arial,sans-serif;background:#050706;color:#f5f7f5;display:grid;min-height:100vh;place-items:center;padding:24px}
      main{max-width:560px;border:1px solid rgba(255,255,255,.14);background:#0d100e;border-radius:14px;padding:28px;box-shadow:0 18px 42px rgba(0,0,0,.32)}
      h1{font-size:24px;line-height:1.2;margin:0 0 12px}
      p{font-size:16px;line-height:1.55;color:rgba(245,247,245,.78);margin:0}
    </style>
  </head>
  <body>
    <main>
      <h1>${escapeHtml(title)}</h1>
      <p>${escapeHtml(message)}</p>
    </main>
  </body>
</html>`);
}

function messageForResult(action, result) {
  if (result?.code === "token_invalid") {
    return {
      status: 400,
      title: "Link inválido",
      message: "Não foi possível localizar esta autorização. Verifique se o link está completo.",
    };
  }
  if (result?.code === "token_expired") {
    return {
      status: 410,
      title: "Link expirado",
      message: "O prazo para responder esta autorização expirou.",
    };
  }
  if (result?.code === "already_decided") {
    const status = result?.status;
    const messages = {
      authorized: "Sua participação já havia sido autorizada.",
      declined: "Sua participação já havia sido recusada.",
      expired: "Este link já expirou.",
      charged: "Esta participação já foi processada.",
      failed: "Esta autorização já foi processada com falha.",
    };
    return {
      status: 200,
      title: "Resposta já registrada",
      message: messages[status] || "Esta decisão já foi registrada anteriormente.",
    };
  }
  if (action === "authorize") {
    return {
      status: 200,
      title: "Participação autorizada",
      message: "Sua participação foi autorizada para esta rodada. A confirmação foi registrada com sucesso.",
    };
  }
  return {
    status: 200,
    title: "Participação recusada",
    message: "Sua participação nesta rodada foi recusada. A decisão foi registrada com sucesso.",
  };
}

async function handleAuthorize(req, res) {
  try {
    const result = await authorizeCaptivePreauthByToken(req.query?.token);
    const message = messageForResult("authorize", result);
    return htmlResponse(res, message.title, message.message, message.status);
  } catch (error) {
    console.error("[captive-preauth] failed", {
      action: "authorize",
      message: error?.message || null,
      code: error?.code || null,
    });
    return htmlResponse(res, "Erro", "Não foi possível registrar sua resposta agora.", 500);
  }
}

async function handleDecline(req, res) {
  try {
    const result = await declineCaptivePreauthByToken(req.query?.token);
    const message = messageForResult("decline", result);
    return htmlResponse(res, message.title, message.message, message.status);
  } catch (error) {
    console.error("[captive-preauth] failed", {
      action: "decline",
      message: error?.message || null,
      code: error?.code || null,
    });
    return htmlResponse(res, "Erro", "Não foi possível registrar sua resposta agora.", 500);
  }
}

router.get("/authorize", handleAuthorize);
router.get("/decline", handleDecline);
router.get("/autorizar", handleAuthorize);
router.get("/recusar", handleDecline);

export default router;
