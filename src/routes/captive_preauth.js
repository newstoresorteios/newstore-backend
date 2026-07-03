import { Router } from "express";
import {
  authorizeCaptivePreauthByCode,
  authorizeCaptivePreauthPublic,
  authorizeCaptivePreauthByToken,
  declineCaptivePreauthByCode,
  declineCaptivePreauthPublic,
  declineCaptivePreauthByToken,
  lookupCaptivePreauthByCode,
  lookupCaptivePreauthPublic,
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

function readConfirmationCode(req) {
  return String(req.body?.confirmation_code || "").trim().toUpperCase();
}

function readPublicCredentials(req) {
  return {
    email: String(req.body?.email || "").trim().toLowerCase(),
    phone: String(req.body?.phone || "").replace(/\D/g, ""),
    authorizationId: String(req.body?.authorization_id || "").trim(),
  };
}

function jsonForCodeDecision(result) {
  if (result?.code === "invalid_confirmation_code") {
    return { status: 400, body: { ok: false, error: "invalid_confirmation_code" } };
  }
  if (result?.code === "duplicate_confirmation_code") {
    return { status: 409, body: { ok: false, error: "duplicate_confirmation_code" } };
  }
  if (result?.code === "confirmation_code_expired") {
    return {
      status: 410,
      body: {
        ok: true,
        status: "expired",
        authorization: result.authorization
          ? {
              status: "expired",
              captive_number: Number(result.authorization.captive_number),
            }
          : undefined,
      },
    };
  }
  if (result?.code === "already_decided") {
    return {
      status: 200,
      body: {
        ok: true,
        already_decided: true,
        status: result.status,
        message: "Esta decisão já foi registrada anteriormente.",
      },
    };
  }
  return {
    status: 200,
    body: {
      ok: true,
      status: result?.status,
    },
  };
}

function jsonForPublicDecision(result) {
  if (result?.code === "authorization_not_found") {
    return { status: 404, body: { ok: false, error: "authorization_not_found" } };
  }
  if (result?.code === "payment_failed" || result?.status === "failed") {
    return {
      status: 402,
      body: {
        ok: false,
        error: "payment_failed",
        status: "failed",
      },
    };
  }
  if (result?.code === "already_decided") {
    return {
      status: 200,
      body: {
        ok: true,
        already_decided: true,
        status: result.status,
        message: "Esta decisão já foi registrada anteriormente.",
      },
    };
  }
  if (result?.status === "expired") {
    return {
      status: 410,
      body: {
        ok: false,
        error: "authorization_expired",
        status: "expired",
      },
    };
  }
  return {
    status: 200,
    body: {
      ok: true,
      status: result?.status,
      charged: result?.charged === true,
    },
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

router.post("/code/lookup", async (req, res) => {
  try {
    const result = await lookupCaptivePreauthByCode(readConfirmationCode(req));
    if (!result.ok) {
      return res.status(result.error === "duplicate_confirmation_code" ? 409 : 400).json({
        ok: false,
        error: result.error || "invalid_confirmation_code",
      });
    }
    return res.json(result);
  } catch (error) {
    console.error("[captive-preauth] failed", {
      action: "confirmation_code_lookup",
      message: error?.message || null,
      code: error?.code || null,
    });
    return res.status(500).json({ ok: false, error: "confirmation_code_lookup_failed" });
  }
});

router.post("/code/authorize", async (req, res) => {
  try {
    const result = await authorizeCaptivePreauthByCode(readConfirmationCode(req));
    const response = jsonForCodeDecision(result);
    return res.status(response.status).json(response.body);
  } catch (error) {
    console.error("[captive-preauth] failed", {
      action: "confirmation_code_authorize",
      message: error?.message || null,
      code: error?.code || null,
    });
    return res.status(500).json({ ok: false, error: "confirmation_code_authorize_failed" });
  }
});

router.post("/code/decline", async (req, res) => {
  try {
    const result = await declineCaptivePreauthByCode(readConfirmationCode(req));
    const response = jsonForCodeDecision(result);
    return res.status(response.status).json(response.body);
  } catch (error) {
    console.error("[captive-preauth] failed", {
      action: "confirmation_code_decline",
      message: error?.message || null,
      code: error?.code || null,
    });
    return res.status(500).json({ ok: false, error: "confirmation_code_decline_failed" });
  }
});

router.post("/public/lookup", async (req, res) => {
  try {
    const { email, phone } = readPublicCredentials(req);
    const result = await lookupCaptivePreauthPublic({ email, phone });
    return res.json(result);
  } catch (error) {
    console.error("[captive-preauth] failed", {
      action: "public_lookup",
      message: error?.message || null,
      code: error?.code || null,
    });
    return res.status(500).json({ ok: false, error: "public_lookup_failed" });
  }
});

router.post("/public/authorize", async (req, res) => {
  try {
    const { email, phone, authorizationId } = readPublicCredentials(req);
    const result = await authorizeCaptivePreauthPublic({ email, phone, authorizationId });
    const response = jsonForPublicDecision(result);
    return res.status(response.status).json(response.body);
  } catch (error) {
    console.error("[captive-preauth] failed", {
      action: "public_authorize",
      message: error?.message || null,
      code: error?.code || null,
    });
    return res.status(500).json({ ok: false, error: "public_authorize_failed" });
  }
});

router.post("/public/decline", async (req, res) => {
  try {
    const { email, phone, authorizationId } = readPublicCredentials(req);
    const result = await declineCaptivePreauthPublic({ email, phone, authorizationId });
    const response = jsonForPublicDecision(result);
    return res.status(response.status).json(response.body);
  } catch (error) {
    console.error("[captive-preauth] failed", {
      action: "public_decline",
      message: error?.message || null,
      code: error?.code || null,
    });
    return res.status(500).json({ ok: false, error: "public_decline_failed" });
  }
});

router.get("/authorize", handleAuthorize);
router.get("/decline", handleDecline);
router.get("/autorizar", handleAuthorize);
router.get("/recusar", handleDecline);

export default router;
