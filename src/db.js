import pkg from "pg";
const { Pool } = pkg;

const pool = new Pool({
  host: process.env.DB_HOST || "10.0.0.114",
  user: process.env.DB_USER || "nova_etl",
  password: process.env.DB_PASSWORD || "etl!@#$",
  database: process.env.DB_NAME || "datamart",
  port: process.env.DB_PORT || 5432,
});

export const query = (text, params) => pool.query(text, params);

export default pool;
