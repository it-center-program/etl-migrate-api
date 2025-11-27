// saveToPostgres.js
import { query } from "../db.js";
import { redis } from "../database/redisClient.js";
import { extractPhones, mergePhones } from "./groupContact.js";

/**
 * แปลง object ทุกค่าเป็น string ก่อนเก็บ Redis
 */
function stringifyValues(obj) {
  const res = {};
  for (const key in obj) {
    res[key] = obj[key] != null ? String(obj[key]) : "";
  }
  return res;
}

export async function saveToPostgres(rows, logId) {
  let insertCount = 0;
  let updateCount = 0;

  for (const row of rows) {
    const hn = row.hn_code;

    // 1) แยกเบอร์ใน row
    const newPhones = extractPhones(row.tel_no);

    // 2) เช็ค Redis ก่อน
    const exists = await redis.sIsMember("etl:hn_codes", hn);

    if (!exists) {
      // ---------- INSERT ----------
      const merged = mergePhones(null, newPhones);
      const redisData = stringifyValues(merged);

      await query(
        `
        INSERT INTO etl_customer_crm (
          recid, hn_code, firstname, lastname, fullname,
          tel_no, tel_no2, tel_no3, tel_no4, tel_no5,
          tel_no6, tel_no7, tel_no8, tel_no9, tel_no10,
          note_other, address, subdistrict, district, province,
          zipcode, full_address, customer_status, gender,
          birthdate, submit_data_status, submit_call_status,
          create_date, health_detail, comment, health, information, rectype
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,
                $16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,
                $27,$28,$29,$30,$31,$32,$33)
      `,
        [
          row.id,
          row.hn_code,
          row.firstname,
          row.lastname,
          row.fullname,

          merged.tel_no,
          merged.tel_no2,
          merged.tel_no3,
          merged.tel_no4,
          merged.tel_no5,
          merged.tel_no6,
          merged.tel_no7,
          merged.tel_no8,
          merged.tel_no9,
          merged.tel_no10,
          merged.note_other,

          row.address,
          row.subdistrict,
          row.district,
          row.province,
          row.zipcode,
          row.full_address,
          row.customer_status,
          row.gender,
          row.birthdate,
          row.submit_data_status,
          row.submit_call_status,
          row.create_date,
          row.health_detail,
          row.comment,
          row.health,
          row.information,
          "BIGDATA",
        ]
      );

      // update redis
      await redis.sAdd("etl:hn_codes", hn);
      await redis.hSet(`etl:phones:${hn}`, redisData);

      insertCount++;
      continue;
    }

    // ---------- UPDATE ----------
    const redisPhones = await redis.hGetAll(`etl:phones:${hn}`);
    const merged = mergePhones(redisPhones, newPhones);
    const redisData = stringifyValues(merged);

    await query(
      `
      UPDATE etl_customer_crm
      SET tel_no=$1, tel_no2=$2, tel_no3=$3, tel_no4=$4, tel_no5=$5,
          tel_no6=$6, tel_no7=$7, tel_no8=$8, tel_no9=$9, tel_no10=$10, note_other=$11
      WHERE hn_code=$12
    `,
      [
        merged.tel_no,
        merged.tel_no2,
        merged.tel_no3,
        merged.tel_no4,
        merged.tel_no5,
        merged.tel_no6,
        merged.tel_no7,
        merged.tel_no8,
        merged.tel_no9,
        merged.tel_no10,
        merged.note_other,
        hn,
      ]
    );

    // update redis cache
    await redis.hSet(`etl:phones:${hn}`, redisData);

    updateCount++;
  }

  return { insertCount, updateCount };
}

function objectToHSet(obj) {
  const arr = [];
  for (const [k, v] of Object.entries(obj)) {
    arr.push(k, v ?? "");
  }
  return arr;
}

export async function saveToPostgresBulk(rows) {
  // preload all existed HN from Redis
  const existingHNs = await redis.sMembers("etl:hn_codes");
  const existsMap = new Set(existingHNs);

  const tempHNs = new Set(); // HN ที่เป็น "new" ในรอบนี้ (first-seen -> insert)

  // -------------------------
  // 1) collect unique HN ที่ต้อง update (only those present in Redis initially)
  // -------------------------
  const updateHNSet = new Set();
  for (const row of rows) {
    const hn = row.hn_code;
    if (existsMap.has(hn)) updateHNSet.add(hn);
  }
  const updateHNList = Array.from(updateHNSet);

  // -------------------------
  // 2) preload old phones (Pipeline ครั้งเดียว) -> phoneData
  // -------------------------
  const phoneData = {};
  if (updateHNList.length) {
    const pipe = redis.multi();
    for (const hn of updateHNList) {
      pipe.hGetAll(`etl:phones:${hn}`);
    }
    const result = await pipe.exec();

    result.forEach((res, idx) => {
      const hn = updateHNList[idx];
      phoneData[hn] = res[1] || {}; // redis response: [err, value]
    });
  }

  // -------------------------
  // currentPhones จะเก็บสถานะล่าสุดสำหรับแต่ละ HN ใน batch นี้
  // เริ่มจาก phoneData (HN ที่มีใน Redis) — HN ใหม่จะถูกเพิ่มเมื่อเจอครั้งแรก
  // -------------------------
  const currentPhones = new Map(Object.entries(phoneData)); // hn -> oldPhones object

  const inserts = [];
  const updates = [];
  const redisHSetOps = [];

  for (const row of rows) {
    const hn = row.hn_code;
    const phones = extractPhones(row.tel_no);

    const existsInRedis = existsMap.has(hn);
    const seenAsNew = tempHNs.has(hn);

    // oldPhones logic:
    // - ถ้า hn มีใน redis ตั้งแต่ต้น -> ใช้ currentPhones[hn] (preloaded or updated)
    // - ถ้า hn เป็น new ที่เจอในรอบนี้แล้ว (seenAsNew) -> ใช้ currentPhones[hn] (merged จาก first-seen)
    // - ถ้า hn ใหม่และยังไม่เคยเจอในรอบนี้ -> oldPhones = null
    let oldPhones = null;
    if (existsInRedis) {
      oldPhones = currentPhones.get(hn) || null;
    } else if (seenAsNew) {
      oldPhones = currentPhones.get(hn) || null;
    } else {
      oldPhones = null;
    }
    // merge ใช้ฟังก์ชันที่คุณมี (ปรับให้ถูกต้องตามก่อนหน้า)
    const merged = mergePhones(oldPhones, phones);
    if (hn == "N073935") {
      console.log("---------------------------");
      console.log(hn, merged);
      console.log("---------------------------");
    }

    // หลัง merge ให้อัปเดต currentPhones เสมอ เพื่อให้ subsequent rows ใช้ค่าอัปเดตล่าสุด
    currentPhones.set(hn, merged);

    // ตัดสิน insert / update:
    if (!existsInRedis && !seenAsNew) {
      // first time seen in this batch and not in redis => insert
      inserts.push({ row, merged });
      tempHNs.add(hn); // mark as seen new
    } else {
      // either existed in redis originally OR we've already seen it in this batch
      updates.push({ row, merged });
    }

    redisHSetOps.push({ key: `etl:phones:${hn}`, value: merged });
  }

  // -------------------------
  // 4) bulk insert/update (simulated here with redis.sAdd for test)
  // -------------------------
  if (inserts.length) {
    await bulkInsertPostgres(inserts);
    await redis.sAdd(
      "etl:hn_codes",
      inserts.map((i) => i.row.hn_code)
    );
  }

  if (updates.length) {
    await bulkUpdatePostgres(updates);
  }

  // -------------------------
  // 5) write to redis in batches
  // -------------------------
  const batchSize = 1000;
  for (let i = 0; i < redisHSetOps.length; i += batchSize) {
    const pipe = redis.multi();
    for (const { key, value } of redisHSetOps.slice(i, i + batchSize)) {
      pipe.hSet(key, objectToHSet(value));
    }
    await pipe.exec();
  }

  return {
    insertCount: inserts.length,
    updateCount: updates.length,
  };
}

const COLUMNS = [
  "recid",
  "hn_code",
  "firstname",
  "lastname",
  "fullname",
  "tel_no",
  "tel_no2",
  "tel_no3",
  "tel_no4",
  "tel_no5",
  "tel_no6",
  "tel_no7",
  "tel_no8",
  "tel_no9",
  "tel_no10",
  "note_other",
  "address",
  "subdistrict",
  "district",
  "province",
  "zipcode",
  "full_address",
  "customer_status",
  "gender",
  "birthdate",
  "submit_data_status",
  "submit_call_status",
  "create_date",
  "health_detail",
  "comment",
  "health",
  "information",
  "rectype",
];

const CHUNK_SIZE = 1000; // ปรับตาม memory และ performance

export async function bulkInsertPostgres(rows) {
  for (let i = 0; i < rows.length; i += CHUNK_SIZE) {
    const chunk = rows.slice(i, i + CHUNK_SIZE);
    const values = [];
    const params = [];

    chunk.forEach(({ row, merged }, rowIdx) => {
      const placeholders = [];
      COLUMNS.forEach((col, colIdx) => {
        placeholders.push(`$${rowIdx * COLUMNS.length + colIdx + 1}`);
      });
      values.push(`(${placeholders.join(",")})`);

      // Push values ตาม order ของ COLUMNS
      params.push(
        row.id,
        row.hn_code,
        row.firstname ?? "",
        row.lastname ?? "",
        row.fullname ?? "",
        merged.tel_no ?? "",
        merged.tel_no2 ?? "",
        merged.tel_no3 ?? "",
        merged.tel_no4 ?? "",
        merged.tel_no5 ?? "",
        merged.tel_no6 ?? "",
        merged.tel_no7 ?? "",
        merged.tel_no8 ?? "",
        merged.tel_no9 ?? "",
        merged.tel_no10 ?? "",
        merged.note_other ?? "",
        row.address ?? "",
        row.subdistrict ?? "",
        row.district ?? "",
        row.province ?? "",
        row.zipcode ?? 0,
        row.full_address ?? "",
        row.customer_status ?? "",
        row.gender ?? "",
        row.birthdate ?? null,
        row.submit_data_status ?? "",
        row.submit_call_status ?? "",
        row.create_date ?? null,
        row.health_detail ?? "",
        row.comment ?? "",
        row.health ?? "",
        row.information ?? "",
        "BIGDATA"
      );
    });

    const sql = `
      INSERT INTO etl_customer_crm (${COLUMNS.join(",")})
      VALUES ${values.join(",")}
      ON CONFLICT (hn_code) DO NOTHING
    `;
    await query(sql, params);
  }
}

export async function bulkUpdatePostgres(rows) {
  for (let i = 0; i < rows.length; i += CHUNK_SIZE) {
    const chunk = rows.slice(i, i + CHUNK_SIZE);

    // PostgreSQL ไม่ support multi-row update ง่ายๆ ต้องทำเป็น individual update ใน transaction
    // เราจะใช้ Promise.all เพื่อ parallelize แต่ chunked
    const promises = chunk.map(({ row, merged }) =>
      query(
        `
        UPDATE etl_customer_crm
        SET tel_no=$1, tel_no2=$2, tel_no3=$3, tel_no4=$4, tel_no5=$5,
            tel_no6=$6, tel_no7=$7, tel_no8=$8, tel_no9=$9, tel_no10=$10,
            note_other=$11
        WHERE hn_code=$12
        `,
        [
          merged.tel_no ?? "",
          merged.tel_no2 ?? "",
          merged.tel_no3 ?? "",
          merged.tel_no4 ?? "",
          merged.tel_no5 ?? "",
          merged.tel_no6 ?? "",
          merged.tel_no7 ?? "",
          merged.tel_no8 ?? "",
          merged.tel_no9 ?? "",
          merged.tel_no10 ?? "",
          merged.note_other ?? "",
          row.hn_code,
        ]
      )
    );

    await Promise.all(promises);
  }
}

export async function saveToPostgresBulkTest(rows) {
  // preload all existed HN from Redis
  const existingHNs = await redis.sMembers("etl:hn_codes");
  const existsMap = new Set(existingHNs);

  const tempHNs = new Set(); // HN ที่เป็น "new" ในรอบนี้ (first-seen -> insert)

  // -------------------------
  // 1) collect unique HN ที่ต้อง update (only those present in Redis initially)
  // -------------------------
  const updateHNSet = new Set();
  for (const row of rows) {
    const hn = row.hn_code;
    if (existsMap.has(hn)) updateHNSet.add(hn);
  }
  const updateHNList = Array.from(updateHNSet);

  // -------------------------
  // 2) preload old phones (Pipeline ครั้งเดียว) -> phoneData
  // -------------------------
  const phoneData = {};
  if (updateHNList.length) {
    const pipe = redis.multi();
    for (const hn of updateHNList) {
      pipe.hGetAll(`etl:phones:${hn}`);
    }
    const result = await pipe.exec();

    result.forEach((res, idx) => {
      const hn = updateHNList[idx];
      phoneData[hn] = res[1] || {}; // redis response: [err, value]
    });
  }

  // -------------------------
  // currentPhones จะเก็บสถานะล่าสุดสำหรับแต่ละ HN ใน batch นี้
  // เริ่มจาก phoneData (HN ที่มีใน Redis) — HN ใหม่จะถูกเพิ่มเมื่อเจอครั้งแรก
  // -------------------------
  const currentPhones = new Map(Object.entries(phoneData)); // hn -> oldPhones object

  const inserts = [];
  const updates = [];
  const redisHSetOps = [];

  for (const row of rows) {
    const hn = row.hn_code;
    const phones = extractPhones(row.tel_no);

    const existsInRedis = existsMap.has(hn);
    const seenAsNew = tempHNs.has(hn);

    // oldPhones logic:
    // - ถ้า hn มีใน redis ตั้งแต่ต้น -> ใช้ currentPhones[hn] (preloaded or updated)
    // - ถ้า hn เป็น new ที่เจอในรอบนี้แล้ว (seenAsNew) -> ใช้ currentPhones[hn] (merged จาก first-seen)
    // - ถ้า hn ใหม่และยังไม่เคยเจอในรอบนี้ -> oldPhones = null
    let oldPhones = null;
    if (existsInRedis) {
      oldPhones = currentPhones.get(hn) || null;
    } else if (seenAsNew) {
      oldPhones = currentPhones.get(hn) || null;
    } else {
      oldPhones = null;
    }
    // merge ใช้ฟังก์ชันที่คุณมี (ปรับให้ถูกต้องตามก่อนหน้า)
    const merged = mergePhones(oldPhones, phones);
    console.log("---------------------------");
    console.log(hn, merged);
    console.log("---------------------------");

    // หลัง merge ให้อัปเดต currentPhones เสมอ เพื่อให้ subsequent rows ใช้ค่าอัปเดตล่าสุด
    currentPhones.set(hn, merged);

    // ตัดสิน insert / update:
    if (!existsInRedis && !seenAsNew) {
      // first time seen in this batch and not in redis => insert
      inserts.push({ row, merged });
      tempHNs.add(hn); // mark as seen new
    } else {
      // either existed in redis originally OR we've already seen it in this batch
      updates.push({ row, merged });
    }

    redisHSetOps.push({ key: `etl:phones:${hn}`, value: merged });
  }

  // -------------------------
  // 4) bulk insert/update (simulated here with redis.sAdd for test)
  // -------------------------
  if (inserts.length) {
    // await bulkInsertPostgres(inserts);
    await redis.sAdd(
      "etl:hn_codes",
      inserts.map((i) => i.row.hn_code)
    );
  }

  if (updates.length) {
    // await bulkUpdatePostgres(updates);
  }

  // -------------------------
  // 5) write to redis in batches
  // -------------------------
  const batchSize = 1000;
  for (let i = 0; i < redisHSetOps.length; i += batchSize) {
    const pipe = redis.multi();
    for (const { key, value } of redisHSetOps.slice(i, i + batchSize)) {
      pipe.hSet(key, objectToHSet(value));
    }
    await pipe.exec();
  }

  return {
    insertCount: inserts.length,
    updateCount: updates.length,
  };
}
