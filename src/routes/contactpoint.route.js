import { Router } from "express";
import { runETL } from "../controllers/contactpoint.controller.js";

const router = Router();

router.get("/", function (req, res) {
  return res.send("Hello contactpoint!");
});
router.get("/run-etl", runETL);

export default router;
