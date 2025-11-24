import "dotenv/config";
import express from "express";
import dotenv from "dotenv";
import cors from "cors";
import pool from "./db.js";
import contactPointRoutes from "./routes/contactpoint.route.js";

const app = express();
app.use(cors());
app.use(express.json());

const API_URL = process.env.API_URL;
const port = process.env.PORT || 3000;

app.get("/", async (req, res) => {
  try {
    const lastId = await getLastId();
    const apiUrl = lastId
      ? `${API_URL}/api/po?lastId=${lastId}`
      : `${API_URL}/api/po`;
    res.json({
      message: "Hello success",
      apiUrl,
    });
  } catch (error) {
    res.status(500).json({ message: "Hello failed", error: error.message });
  }
  // res.send("Hello Api ETL Nove CRM!");
});

// ฟังก์ชันช่วยดึง lastId ล่าสุด
async function getLastId() {
  const res = await pool.query(
    "SELECT last_id FROM migrate_log WHERE status='success' ORDER BY id DESC LIMIT 1"
  );
  return res.rows[0]?.last_id || null;
}

// ฟังก์ชันบันทึก log
async function saveLog({ lastId, recordCount, status, errorMessage }) {
  await pool.query(
    "INSERT INTO migrate_log (last_id, record_count, status, error_message) VALUES ($1,$2,$3,$4)",
    [lastId, recordCount, status, errorMessage]
  );
}

// API สำหรับ cronjob เรียกทำ migration
app.get("/api/migrate", async (req, res) => {
  let client;
  return res.status(400).json({
    message: "Disable Route",
    count: 0,
  });
  try {
    const lastId = await getLastId();
    const apiUrl = lastId
      ? `${API_URL}/api/po?lastId=${lastId}`
      : `${API_URL}/api/po`;

    console.log(`Fetching: ${apiUrl}`);
    const response = await fetch(apiUrl);
    const result = await response.json();

    const data = result.data;
    const recordCount = result.count;

    // เชื่อมต่อ PostgreSQL
    client = await pool.connect();
    await client.query("BEGIN");

    // ตรวจสอบข้อมูล
    if (!Array.isArray(data) || data.length === 0) {
      await saveLog({
        lastId,
        recordCount: 0,
        status: "success",
        errorMessage: null,
      });
      await client.query("COMMIT");
      return res.json({ message: "No new data", count: 0 });
    }
    await client.query("COMMIT");
    // return res.json({
    //   message: "Migrate test",
    //   test: "DELETE FROM etl_crm WHERE id IN (SELECT id FROM etl_crm WHERE recid < $1 AND rectype = 'CRM')",
    //   lastId: lastId,
    // });
    // ลบข้อมูลรอบที่ error ก่อนหน้า (ถ้ามี)
    await client.query(
      "DELETE FROM etl_crm WHERE id IN (SELECT id FROM etl_crm WHERE recid < $1 AND rectype = 'CRM')",
      [lastId || 0]
    );

    // แทรกข้อมูลใหม่
    for (const row of data) {
      await client.query(
        `INSERT INTO etl_crm (
    recid, po_datetime, po_date, po_time, po_no, shipping_code, shipping_by, shipping_name,
    cus_tel_no, hn_code, firstname, lastname, fullname, ship_address, ship_subdistrict,
    ship_district, ship_province, ship_zipcode, ship_psd, remark, cus_full_address,
    line_no, product_code, product_name, product_type_name, productdetail, productother,
    qty, priceperunit, totalprice, unitname, agentcode, empcode, paymenttype, paymentstatus,
    health_detail, sex, birthdate, submit_data_status, submit_call_status, channelname,
    sell_by, customer_status, comment, health, information
  ) VALUES (
    $1,$2,$3,$4,$5,$6,$7,$8,
    $9,$10,$11,$12,$13,$14,$15,
    $16,$17,$18,$19,$20,$21,
    $22,$23,$24,$25,$26,$27,
    $28,$29,$30,$31,$32,$33,$34,$35,
    $36,$37,$38,$39,$40,$41,
    $42,$43,$44,$45,$46
  )
  ON CONFLICT (id) DO NOTHING`,
        [
          row.recid,
          row.po_datetime,
          row.po_date,
          row.po_time,
          row.po_no,
          row.shipping_code,
          row.shipping_by?.toString(),
          row.shipping_name,
          row.cus_tel_no,
          row.hn_code,
          row.firstname,
          row.lastname,
          row.fullname,
          row.ship_address,
          row.ship_subdistrict,
          row.ship_district,
          row.ship_province,
          row.ship_zipcode,
          row.ship_psd,
          row.remark,
          row.cus_full_address,
          row.line_no,
          row.product_code,
          row.product_name,
          row.product_type_name,
          row.ProductDetail, // <<=== case-sensitive JSON key
          row.ProductOther,
          row.QTY,
          row.PricePerUnit,
          row.TotalPrice,
          row.UnitName,
          row.AgentCode,
          row.EmpCode,
          row.PaymentType,
          row.PaymentStatus,
          row.health_detail,
          row.Sex,
          row.birthdate ? row.birthdate.split("T")[0] : null, // แปลงเป็น DATE
          row.submit_data_status,
          row.submit_call_status,
          row.ChannelName,
          row.sell_by?.toString(),
          row.customer_status,
          row.comment,
          row.health,
          row.information,
        ]
      );
    }

    // บันทึก log
    const newLastId = data[data.length - 1].recid;
    await saveLog({
      lastId: newLastId,
      recordCount: recordCount,
      status: "success",
      errorMessage: null,
    });

    await client.query("COMMIT");
    res.json({
      message: "Migrate success",
      count: recordCount,
      lastId: newLastId,
    });
  } catch (err) {
    console.error("Migrate error:", err);
    if (client) await client.query("ROLLBACK");

    await saveLog({
      lastId: null,
      recordCount: 0,
      status: "error",
      errorMessage: err.message,
    });

    res.status(500).json({ message: "Migrate failed", error: err.message });
  } finally {
    if (client) client.release();
  }
});

app.use("/api/contactpoint", contactPointRoutes);

app.listen(port, () => {
  console.log(`✅ Server running at http://localhost:${port}`);
});
