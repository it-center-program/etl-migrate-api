import pool from "../db.js";
import { fetchContact } from "../services/fetchContact.js";
import {
  saveToPostgres,
  saveToPostgresBulk,
  saveToPostgresBulkTest,
} from "../services/saveToPostgres.js";

async function getLastId() {
  const res = await pool.query(
    "SELECT last_id FROM migrate_log_customer WHERE status='success' ORDER BY id DESC LIMIT 1"
  );
  return res.rows[0]?.last_id || 0;
}
async function saveLogStart({ continueId, batchNo }) {
  const res = await pool.query(
    `INSERT INTO migrate_log_customer (continue_id , batch_no, status)
     VALUES ($1, $2, 'running') RETURNING id`,
    [continueId, batchNo]
  );
  // console.log(res.rows[0].id);
  return res.rows[0].id;
}

async function saveLogFinish({
  logId,
  newLastId,
  recordCount,
  insertCount,
  updateCount,
  status,
  errorMessage,
}) {
  await pool.query(
    `UPDATE migrate_log_customer
     SET last_id=$1, record_count=$2,insert_count=$3,update_count=$4, status=$5, error_message=$6, finished_at=NOW()
     WHERE id=$7`,
    [
      newLastId,
      recordCount,
      insertCount,
      updateCount,
      status,
      errorMessage,
      logId,
    ]
  );
}

export async function runETL(req, res) {
  const limit = parseInt(req.query.limit) || 1000;
  const startTime = Date.now();
  let logId;
  let lastId;
  const stepDurations = {};

  try {
    // STEP 1: Get last ID
    let t0 = Date.now();
    lastId = await getLastId();
    stepDurations.getLastId = Date.now() - t0;

    // STEP 2: batchNo ...
    t0 = Date.now();
    const batchRes = await pool.query(`
      SELECT COALESCE(MAX(batch_no), 0) + 1 AS batch_no
      FROM migrate_log_customer
      WHERE DATE(started_at) = CURRENT_DATE
    `);
    const batchNo = batchRes.rows[0].batch_no;
    stepDurations.getBatchNo = Date.now() - t0;

    // STEP 3: fetch data
    t0 = Date.now();
    const raw = await fetchContact(lastId, limit);
    const data = raw.data;
    const recordCount = data.length;
    stepDurations.fetchData = Date.now() - t0;

    // STEP 4: save log start
    t0 = Date.now();
    logId = await saveLogStart({ continueId: lastId, batchNo });
    stepDurations.saveLogStart = Date.now() - t0;

    if (recordCount === 0) {
      t0 = Date.now();
      await saveLogFinish({
        logId,
        newLastId: lastId,
        recordCount: 0,
        status: "success",
        errorMessage: null,
      });
      stepDurations.saveLogFinish = Date.now() - t0;
      return res.json({
        message: "All data synced",
        count: 0,
        durations: stepDurations,
      });
    }

    // ---------------------------
    // IMPORTANT: ทำ DELETE แบบ chunked โดยใช้ client ชั่วคราว
    // ---------------------------
    t0 = Date.now();
    const clientForDelete = await pool.connect();
    try {
      // เลือกฟังก์ชันที่ DB รองรับ
      try {
        await deleteChunked(clientForDelete, lastId, 10000); // ลบทีละ 10k เพื่อลด lock
      } catch (err) {
        // ถ้า delete ... limit ไม่รองรับ ให้ fallback แบบ ctid
        await deleteChunkedFallback(clientForDelete, lastId, 10000);
      }
    } finally {
      clientForDelete.release();
    }
    stepDurations.deleteOldRecords = Date.now() - t0;

    // ---------------------------
    // STEP: Save to Postgres (let the bulk function manage its own transactions)
    // ---------------------------
    t0 = Date.now();
    const saveRes = await saveToPostgresBulk(data); // ให้ฟังก์ชันนี้ open/commit connections ของตัวเอง
    stepDurations.saveToPostgres = Date.now() - t0;

    // STEP: finish log
    t0 = Date.now();
    const newLastId = data[recordCount - 1].id;
    await saveLogFinish({
      logId,
      newLastId,
      recordCount,
      insertCount: saveRes.insertCount,
      updateCount: saveRes.updateCount,
      status: "success",
      errorMessage: null,
    });
    stepDurations.saveLogFinish = Date.now() - t0;

    // total
    const totalDuration = Date.now() - startTime;
    console.log("totalDuration_ms", totalDuration);
    console.log("insertCount", saveRes.insertCount);
    console.log("updateCount", saveRes.updateCount);
    res.json({
      message: "Batch completed",
      batch_no: batchNo,
      count: recordCount,
      lastId: newLastId,
      insertCount: saveRes.insertCount,
      updateCount: saveRes.updateCount,
      totalDuration_ms: totalDuration,
      stepDurations,
    });
  } catch (err) {
    console.error("runETL error:", err);
    // ถ้ามี logId ให้บันทึกสถานะ error
    try {
      if (logId)
        await saveLogFinish({
          logId,
          newLastId: lastId,
          recordCount: 0,
          status: "error",
          errorMessage: err.message,
        });
    } catch (e) {
      console.error("failed to save error log:", e);
    }
    return res.status(500).json({ status: "error", message: err.message });
  }
}

// helper: delete แบบ chunked โดยใช้ client ของ pool (และ commit ทุก chunk)
async function deleteChunked(poolClient, lastId, chunkSize = 50000) {
  // ใช้ loop ลบเป็น batch เล็ก ๆ เพื่อไม่ให้ transaction ใหญ่เกิน
  let totalDeleted = 0;
  while (true) {
    // ลบทีละ chunk ภายใน transaction สั้น ๆ
    await poolClient.query("BEGIN");
    try {
      const res = await poolClient.query(
        `DELETE FROM etl_customer_crm
         WHERE recid > $1 AND rectype='BIGDATA'
         LIMIT $2`,
        [lastId, chunkSize]
      );
      const deleted = res.rowCount || 0;
      totalDeleted += deleted;
      await poolClient.query("COMMIT");
      if (deleted < chunkSize) break; // หมดแล้ว
      // ถ้ายังมี เยียวยาด้วย sleep สั้น ๆ ถ้าต้องการ
    } catch (err) {
      await poolClient.query("ROLLBACK");
      throw err;
    }
  }
  return totalDeleted;
}

// fallback delete chunk ถ้า DELETE ... LIMIT ไม่รองรับ
async function deleteChunkedFallback(poolClient, lastId, chunkSize = 50000) {
  let totalDeleted = 0;
  while (true) {
    await poolClient.query("BEGIN");
    try {
      const res = await poolClient.query(
        `DELETE FROM etl_customer_crm
         WHERE id IN (
           SELECT id FROM etl_customer_crm
           WHERE recid > $1 AND rectype='BIGDATA'
           LIMIT $2
         )`,
        [lastId, chunkSize]
      );
      const deleted = res.rowCount || 0;
      totalDeleted += deleted;
      await poolClient.query("COMMIT");
      if (deleted < chunkSize) break;
    } catch (err) {
      await poolClient.query("ROLLBACK");
      throw err;
    }
  }
  return totalDeleted;
}

export async function runETL2(req, res) {
  const limit = parseInt(req.query.limit) || 1000;
  const startTime = Date.now();
  let client;
  let logId;
  let lastId;

  const stepDurations = {}; // เก็บเวลาแต่ละขั้นตอน

  try {
    // STEP 1: Get last ID
    let t0 = Date.now();
    lastId = await getLastId();
    stepDurations.getLastId = Date.now() - t0;
    console.log("getLastId", stepDurations.getLastId);

    // STEP 2: Get next batch number
    t0 = Date.now();
    const batchRes = await pool.query(`
      SELECT COALESCE(MAX(batch_no), 0) + 1 AS batch_no
      FROM migrate_log_customer
      WHERE DATE(started_at) = CURRENT_DATE
    `);
    const batchNo = batchRes.rows[0].batch_no;
    stepDurations.getBatchNo = Date.now() - t0;
    console.log("getBatchNo", stepDurations.getBatchNo);

    // STEP 3: Fetch contact data
    t0 = Date.now();
    const raw = await fetchContact(lastId, limit);
    const data = raw.data;
    const recordCount = data.length;
    stepDurations.fetchData = Date.now() - t0;
    console.log("fetchData", stepDurations.fetchData);

    // STEP 4: Save log start
    t0 = Date.now();
    logId = await saveLogStart({ continueId: lastId, batchNo });
    stepDurations.saveLogStart = Date.now() - t0;
    console.log("saveLogStart", stepDurations.saveLogStart);

    // STEP 5: Begin transaction
    t0 = Date.now();
    client = await pool.connect();
    await client.query("BEGIN");
    stepDurations.beginTransaction = Date.now() - t0;
    console.log("beginTransaction", stepDurations.beginTransaction);

    if (recordCount === 0) {
      t0 = Date.now();
      await saveLogFinish({
        logId,
        newLastId: lastId,
        recordCount: 0,
        status: "success",
        errorMessage: null,
      });
      stepDurations.saveLogFinish = Date.now() - t0;
      console.log("saveLogFinish", stepDurations.saveLogFinish);

      await client.query("COMMIT");
      stepDurations.commit = 0;
      return res.json({
        message: "All data synced",
        count: 0,
        durations: stepDurations,
      });
    }

    // STEP 6: Delete old records
    t0 = Date.now();
    await client.query(
      `DELETE FROM etl_customer_crm WHERE recid > $1 AND rectype='BIGDATA'`,
      [lastId]
    );
    stepDurations.deleteOldRecords = Date.now() - t0;
    console.log("deleteOldRecords", stepDurations.deleteOldRecords);

    // STEP 7: Save to Postgres
    t0 = Date.now();
    const save = await saveToPostgresBulk(data);
    // const save = await saveToPostgres(data, logId);
    stepDurations.saveToPostgres = Date.now() - t0;
    console.log("saveToPostgres", stepDurations.saveToPostgres);

    // STEP 8: Finish log
    t0 = Date.now();
    const newLastId = data[recordCount - 1].id;
    await saveLogFinish({
      logId,
      newLastId,
      recordCount,
      insertCount: save.insertCount,
      updateCount: save.updateCount,
      status: "success",
      errorMessage: null,
    });
    stepDurations.saveLogFinish = Date.now() - t0;
    console.log("saveLogFinish", stepDurations.saveLogFinish);

    // STEP 9: Commit transaction
    t0 = Date.now();
    await client.query("COMMIT");
    stepDurations.commit = Date.now() - t0;
    console.log("commit", stepDurations.commit);

    const totalDuration = Date.now() - startTime;

    res.json({
      message: "Batch completed",
      batch_no: batchNo,
      count: recordCount,
      lastId: newLastId,
      totalDuration_ms: totalDuration,
      stepDurations,
    });
  } catch (err) {
    if (client) await client.query("ROLLBACK");

    await saveLogFinish({
      logId,
      newLastId: lastId,
      recordCount: 0,
      status: "error",
      errorMessage: err.message,
    });
    console.error(err);

    res.status(500).json({ status: "error", message: err.message });
  }
}

export async function testETL(req, res) {
  const limit = parseInt(req.query.limit) || 1000;
  const startTime = Date.now();
  let logId;
  let lastId;
  const stepDurations = {};

  try {
    // STEP 1: Get last ID
    let t0 = Date.now();
    // lastId = await getLastId();
    lastId = 70339;
    stepDurations.getLastId = Date.now() - t0;

    // STEP 2: batchNo ...
    t0 = Date.now();
    const batchRes = await pool.query(`
      SELECT COALESCE(MAX(batch_no), 0) + 1 AS batch_no
      FROM migrate_log_customer
      WHERE DATE(started_at) = CURRENT_DATE
    `);
    const batchNo = batchRes.rows[0].batch_no;
    stepDurations.getBatchNo = Date.now() - t0;

    // STEP 3: fetch data
    t0 = Date.now();
    const raw = await fetchContact(lastId, limit);
    const data = raw.data;
    const recordCount = data.length;
    stepDurations.fetchData = Date.now() - t0;

    // STEP 4: save log start
    t0 = Date.now();
    // logId = await saveLogStart({ continueId: lastId, batchNo });
    stepDurations.saveLogStart = Date.now() - t0;

    if (recordCount === 0) {
      t0 = Date.now();
      // await saveLogFinish({
      //   logId,
      //   newLastId: lastId,
      //   recordCount: 0,
      //   status: "success",
      //   errorMessage: null,
      // });
      stepDurations.saveLogFinish = Date.now() - t0;
      return res.json({
        message: "All data synced",
        count: 0,
        durations: stepDurations,
      });
    }

    // ---------------------------
    // IMPORTANT: ทำ DELETE แบบ chunked โดยใช้ client ชั่วคราว
    // ---------------------------
    t0 = Date.now();
    // const clientForDelete = await pool.connect();
    // try {
    //   // เลือกฟังก์ชันที่ DB รองรับ
    //   try {
    //     await deleteChunked(clientForDelete, lastId, 10000); // ลบทีละ 10k เพื่อลด lock
    //   } catch (err) {
    //     // ถ้า delete ... limit ไม่รองรับ ให้ fallback แบบ ctid
    //     await deleteChunkedFallback(clientForDelete, lastId, 10000);
    //   }
    // } finally {
    //   clientForDelete.release();
    // }
    stepDurations.deleteOldRecords = Date.now() - t0;

    // ---------------------------
    // STEP: Save to Postgres (let the bulk function manage its own transactions)
    // ---------------------------
    t0 = Date.now();
    const saveRes = await saveToPostgresBulkTest(data); // ให้ฟังก์ชันนี้ open/commit connections ของตัวเอง
    stepDurations.saveToPostgres = Date.now() - t0;

    // STEP: finish log
    t0 = Date.now();
    const newLastId = data[recordCount - 1].id;
    // await saveLogFinish({
    //   logId,
    //   newLastId,
    //   recordCount,
    //   insertCount: saveRes.insertCount,
    //   updateCount: saveRes.updateCount,
    //   status: "success",
    //   errorMessage: null,
    // });
    stepDurations.saveLogFinish = Date.now() - t0;

    // total
    const totalDuration = Date.now() - startTime;
    console.log("totalDuration_ms", totalDuration);
    console.log("insertCount", saveRes.insertCount);
    console.log("updateCount", saveRes.updateCount);
    res.json({
      message: "Batch completed",
      batch_no: batchNo,
      count: recordCount,
      lastId: newLastId,
      insertCount: saveRes.insertCount,
      updateCount: saveRes.updateCount,
      totalDuration_ms: totalDuration,
      stepDurations,
    });
  } catch (err) {
    console.error("runETL error:", err);
    // ถ้ามี logId ให้บันทึกสถานะ error
    try {
      // if (logId)
      //   await saveLogFinish({
      //     logId,
      //     newLastId: lastId,
      //     recordCount: 0,
      //     status: "error",
      //     errorMessage: err.message,
      //   });
    } catch (e) {
      console.error("failed to save error log:", e);
    }
    return res.status(500).json({ status: "error", message: err.message });
  }
}
