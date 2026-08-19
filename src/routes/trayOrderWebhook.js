// src/routes/trayOrderWebhook.js
//
// Receptor do webhook de pedido da Tray (escopo `order`). Ver
// src/services/trayOrderWebhook.js para a logica de reconciliacao.
//
// BLOQUEADO POR CONFIGURACAO EXTERNA: o escopo `order` so comeca a chegar
// aqui depois que a Tray habilitar o webhook via chamado de suporte,
// informando esta URL. Ate la, a rota existe, esta testada, mas nunca
// recebe trafego real.
//
// Payload oficial: sempre application/x-www-form-urlencoded (nunca JSON) —
// por isso este router tem seu PROPRIO parser de urlencoded, escopado
// apenas a esta rota (o parser global do app so cobre JSON).

import express from "express";
import { handleTrayOrderWebhook, TrayWebhookError } from "../services/trayOrderWebhook.js";

const router = express.Router();

router.use(express.urlencoded({ extended: false }));

router.post("/order", async (req, res) => {
  // Boa pratica documentada pela Tray: responder rapido, nunca deixar a
  // notificacao pendurada — mas o unico trabalho aqui e 1 GET + no maximo
  // 1 escrita atomica, entao processamos antes de responder (sem fila).
  try {
    const result = await handleTrayOrderWebhook(req.body || {});
    console.log("[tray.webhook.order]", { ...result });
    return res.status(200).json({ ok: true, ...result });
  } catch (e) {
    if (e instanceof TrayWebhookError) {
      console.warn("[tray.webhook.order] rejected", { code: e.code });
      return res.status(e.status).json({ ok: false, error: e.code });
    }
    console.error("[tray.webhook.order] failed", { error: e?.message || String(e) });
    // Nao-200 -> a Tray reenvia com backoff (comportamento documentado).
    // Correto aqui: nao fingir sucesso quando a reconciliacao realmente falhou.
    return res.status(500).json({ ok: false, error: "webhook_processing_failed" });
  }
});

export default router;
