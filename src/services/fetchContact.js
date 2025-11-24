import axios from "axios";

const API_URL = process.env.API_URL;

export async function fetchContact(lastId = 0, limit = 1000) {
  const url = `${API_URL}/api/contactpoint?lastId=${lastId}&limit=${limit}`;
  console.log(url);
  const res = await axios.get(url);
  return res.data; // assume array of records
}
