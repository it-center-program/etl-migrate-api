import { Router } from "express";
import { runETL, testETL } from "../controllers/contactpoint.controller.js";
import { preloadRedisFromPostgres } from "../services/preloadRedis.js";
import { redis } from "../database/redisClient.js";

const router = Router();

router.get("/", function (req, res) {
  return res.send("Hello contactpoint!");
});
router.get("/run-etl", runETL);

router.get("/refresh-redis", async (req, res) => {
  try {
    const result = await preloadRedisFromPostgres();
    res.json({ ok: true, result });
  } catch (err) {
    console.error(err);
    res.status(500).json({ ok: false, error: err.message });
  }
});

router.get("/test-etl", testETL);

export default router;
