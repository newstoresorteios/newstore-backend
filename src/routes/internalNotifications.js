import express from "express";

const router = express.Router();

router.post("/events", (_req, res) => {
  return res.status(403).json({ ok: false, error: "push_engine_blocked" });
});

export default router;
