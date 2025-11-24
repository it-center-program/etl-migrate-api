import { query } from "../db.js";
import { extractPhones, mergePhones } from "./groupContact.js";

async function saveLogDetail(logId, row, action = "insert") {
  const sql = `
    INSERT INTO migrate_log_customer_detail (log_id, recid, action, raw_data)
    VALUES ($1, $2, $3, $4)
  `;
  await query(sql, [logId, row.id, action, row]);
}
export async function saveToPostgres(rows, logId) {
  let insertCount = 0;
  let updateCount = 0;
  for (const row of rows) {
    const hn = row.hn_code;
    const newPhones = extractPhones(row.tel_no);

    const existing = await query(
      "SELECT * FROM etl_customer_crm WHERE hn_code = $1 LIMIT 1",
      [hn]
    ).then((r) => r.rows[0]);

    const merged = mergePhones(existing, newPhones);

    if (!existing) {
      // insert
      insertCount++;
      await query(
        `
        INSERT INTO etl_customer_crm (
          recid, hn_code, firstname, lastname, fullname,
          tel_no, tel_no2, tel_no3, tel_no4, tel_no5, 
          tel_no6, tel_no7, tel_no8, tel_no9, tel_no10, note_other,
          address, subdistrict, district, province, zipcode,
          full_address, customer_status, gender, birthdate,
          submit_data_status, submit_call_status, create_date,
          health_detail, comment, health, information, rectype
        )
        VALUES ($1,$2,$3,$4,
                $5,$6,$7,$8,$9,
                $10,$11,$12,$13,$14,$15,
                $16,$17,$18,$19,$20,
                $21,$22,$23,$24,
                $25,$26,$27,
                $28,$29,$30,$31,$32,$33)
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
    } else {
      // update tel_no1-10 + note_other
      updateCount++;
      await query(
        `
        UPDATE etl_customer_crm
        SET tel_no=$1, tel_no2=$2, tel_no3=$3, tel_no4=$4, tel_no5=$5,tel_no6=$6, tel_no7=$7, tel_no8=$8, tel_no9=$9, tel_no10=$10, note_other=$11
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
    }
    await saveLogDetail(logId, row, existing ? "update" : "insert");
  }
  return { insertCount, updateCount };
}
