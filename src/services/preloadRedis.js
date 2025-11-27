// preloadRedis.js
import { query } from "../db.js";
import { redis } from "../database/redisClient.js";

export async function preloadRedisFromPostgres() {
  console.log("🔄 Preloading Redis from PostgreSQL ...");

  // ------------------------------------------------------
  // STEP 1: Clear Redis keys (etl:hn_codes + etl:phones:*)
  // ------------------------------------------------------

  console.log("🧹 Clearing Redis old data...");

  // ลบชุดหลัก
  await redis.del("etl:hn_codes");

  // ลบ etl:phones:* แบบ SCAN (ไม่ block Redis)
  let cursor = "0";
  do {
    const [newCursor, keys] = await redis.scan(cursor, {
      MATCH: "etl:phones:*",
      COUNT: 1000,
    });
    cursor = newCursor;

    if (keys.length > 0) {
      await redis.del(keys);
    }
  } while (cursor !== "0");

  console.log("✔ Redis cleared");

  // ------------------------------------------------------
  // STEP 2: Load data from Postgres → Redis (batch)
  // ------------------------------------------------------

  const batch = 5000;
  let lastId = 0;
  let loaded = 0;

  while (true) {
    const rows = await query(
      `SELECT id, hn_code, tel_no, tel_no2, tel_no3, tel_no4, tel_no5,
              tel_no6, tel_no7, tel_no8, tel_no9, tel_no10, note_other
       FROM etl_customer_crm
       WHERE id > $1
       ORDER BY id
       LIMIT $2`,
      [lastId, batch]
    ).then((r) => r.rows);

    if (rows.length === 0) break;

    const pipeline = redis.multi();

    for (const r of rows) {
      pipeline.sAdd("etl:hn_codes", r.hn_code);

      pipeline.hSet(`etl:phones:${r.hn_code}`, {
        tel_no: r.tel_no || "",
        tel_no2: r.tel_no2 || "",
        tel_no3: r.tel_no3 || "",
        tel_no4: r.tel_no4 || "",
        tel_no5: r.tel_no5 || "",
        tel_no6: r.tel_no6 || "",
        tel_no7: r.tel_no7 || "",
        tel_no8: r.tel_no8 || "",
        tel_no9: r.tel_no9 || "",
        tel_no10: r.tel_no10 || "",
        note_other: r.note_other || "",
      });

      lastId = r.id;
    }

    await pipeline.exec();

    loaded += rows.length;

    console.log(`Loaded ${loaded} records...`);
  }

  console.log("✅ Redis preload complete");
  return { total: loaded };
}
