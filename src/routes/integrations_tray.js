import { Router } from "express";
import { trayAdapterClient } from "../services/trayAdapterClient.js";

const router = Router();

function errorResponse(error) {
  return {
    success: false,
    adapter_connected: false,
    products_accessible: false,
    error: error?.code || "tray_adapter_error",
  };
}

router.get("/test", async (_req, res) => {
  try {
    const result = await trayAdapterClient.searchProducts({ limit: 1 });
    return res.json({
      success: true,
      adapter_connected: true,
      products_accessible: Array.isArray(result?.products),
    });
  } catch (error) {
    const status = error?.status === 401 || error?.status === 403 ? 502 : 502;
    return res.status(status).json(errorResponse(error));
  }
});

export default router;
