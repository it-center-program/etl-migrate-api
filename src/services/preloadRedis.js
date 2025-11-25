// preloadRedis.js
import { query } from "../db.js";
import { redis } from "../database/redisClient.js";

export async function preloadRedisFromPostgres() {
  console.log("🔄 Preloading Redis from PostgreSQL ...");

  const total = await query("SELECT COUNT(*) AS c FROM etl_customer_crm").then(
    (r) => Number(r.rows[0].c)
  );

  if (total === 0) {
    console.log("⚠ ไม่มีข้อมูลใน Postgres");
    return { total: 0 };
  }

  const batch = 5000;
  let offset = 0;
  let loaded = 0;

  while (offset < total) {
    const rows = await query(
      `SELECT hn_code, tel_no, tel_no2, tel_no3, tel_no4, tel_no5,
              tel_no6, tel_no7, tel_no8, tel_no9, tel_no10, note_other
       FROM etl_customer_crm
       ORDER BY id
       LIMIT $1 OFFSET $2`,
      [batch, offset]
    ).then((r) => r.rows);

    for (const r of rows) {
      await redis.sAdd("etl:hn_codes", r.hn_code);

      await redis.hSet(`etl:phones:${r.hn_code}`, {
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
    }

    loaded += rows.length;
    offset += batch;

    console.log(`Loaded ${loaded}/${total} records into Redis...`);
  }

  console.log("✅ Redis preload complete");
  return { total: loaded };
}
