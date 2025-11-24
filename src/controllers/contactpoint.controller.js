import pool from "../db.js";
import { fetchContact } from "../services/fetchContact.js";
import { saveToPostgres } from "../services/saveToPostgres.js";

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
  const startTime = Date.now();
  let client;
  let logId;
  let lastId;
  try {
    lastId = await getLastId();

    const batchRes = await pool.query(`
      SELECT COALESCE(MAX(batch_no), 0) + 1 AS batch_no
      FROM migrate_log_customer
      WHERE DATE(started_at) = CURRENT_DATE
    `);
    const batchNo = batchRes.rows[0].batch_no;

    const raw = await fetchContact(lastId, 1000);
    const data = raw.data;

    const recordCount = data.length;

    logId = await saveLogStart({ continueId: lastId, batchNo });

    client = await pool.connect();
    await client.query("BEGIN");

    if (recordCount === 0) {
      await saveLogFinish({
        logId,
        newLastId: lastId,
        recordCount: 0,
        status: "success",
        errorMessage: null,
      });

      await client.query("COMMIT");
      return res.json({ message: "All data synced", count: 0 });
    }

    await client.query(`DELETE FROM etl_customer_crm WHERE id > $1`, [lastId]);
    // return res.json({ status: "ok", data });
    const save = await saveToPostgres(data, logId);

    const newLastId = data[recordCount - 1].id; // สมมติ API ส่ง id เรียงลำดับ

    // const newLastId = data[data.length - 1].RECID2;

    await saveLogFinish({
      logId,
      newLastId,
      recordCount,
      insertCount: save.insertCount,
      updateCount: save.updateCount,
      status: "success",
      errorMessage: null,
    });

    await client.query("COMMIT");
    const duration = Date.now() - startTime;
    // res.json({ status: "ok", lastId, data });
    res.json({
      message: "Batch completed",
      batch_no: batchNo,
      count: recordCount,
      lastId: newLastId,
      duration_ms: duration,
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
