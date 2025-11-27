// แยกเบอร์โทรออกเป็น array
export function extractPhones(str) {
  if (!str) return [];
  return str
    .replaceAll(" ", "")
    .split(/[,; \/]+/)
    .map((s) => s.trim())
    .filter(Boolean);
}

// merge เบอร์เก่ากับใหม่
export function mergePhones(existing, newPhones) {
  // normalize helpers
  const norm = (v) => {
    if (v === null || v === undefined) return null;
    const s = String(v).trim();
    return s === "" ? null : s;
  };

  // เตรียม slots จาก existing (ตำแหน่งคงเดิม)
  const slots = [
    existing ? norm(existing.tel_no) : null,
    existing ? norm(existing.tel_no2) : null,
    existing ? norm(existing.tel_no3) : null,
    existing ? norm(existing.tel_no4) : null,
    existing ? norm(existing.tel_no5) : null,
    existing ? norm(existing.tel_no6) : null,
    existing ? norm(existing.tel_no7) : null,
    existing ? norm(existing.tel_no8) : null,
    existing ? norm(existing.tel_no9) : null,
    existing ? norm(existing.tel_no10) : null,
  ];

  // เก็บหมายเลขที่มีอยู่ใน slots (เพื่อป้องกัน duplicate)
  const used = new Set();
  for (const s of slots) {
    if (s) used.add(s);
  }

  // เตรียมคิวของ newPhones โดยตัดซ้ำและตัดหมายเลขที่อยู่แล้ว
  const newQueue = [];
  const seenNew = new Set();
  for (const p of newPhones || []) {
    if (!p) continue;
    const ph = String(p).trim();
    if (!ph) continue;
    if (used.has(ph)) continue; // มีแล้วใน slots → ข้าม
    if (seenNew.has(ph)) continue; // duplicate ใน newPhones → ข้าม
    seenNew.add(ph);
    newQueue.push(ph);
  }

  // เติมช่องว่างใน slots ด้วยค่าใน newQueue (จากซ้ายไปขวา)
  for (let i = 0; i < slots.length && newQueue.length > 0; i++) {
    if (!slots[i]) {
      const v = newQueue.shift();
      slots[i] = v;
      used.add(v);
    }
  }

  // แยก existing.note_other (ถ้ามี) — เก็บลำดับเดิม แต่จะตัดค่าที่ปัจจุบันอยู่ใน slots ออก
  const existingExtras = [];
  if (existing && existing.note_other) {
    const parts = String(existing.note_other).split(",");
    for (const part of parts) {
      const p = part.trim();
      if (!p) continue;
      if (used.has(p)) continue; // ถ้าอยู่ใน slots แล้ว ให้ข้าม
      if (!existingExtras.includes(p)) existingExtras.push(p);
    }
  }

  // ตอนนี้ newQueue อาจยังมีค่าที่เหลือ (เกิน 10) — นำไปต่อท้าย existingExtras (แต่ไม่ซ้ำ)
  for (const p of newQueue) {
    if (used.has(p)) continue;
    if (!existingExtras.includes(p)) existingExtras.push(p);
  }

  const note_other =
    existingExtras.length > 0 ? existingExtras.join(",") : null;

  return {
    tel_no: slots[0] || null,
    tel_no2: slots[1] || null,
    tel_no3: slots[2] || null,
    tel_no4: slots[3] || null,
    tel_no5: slots[4] || null,
    tel_no6: slots[5] || null,
    tel_no7: slots[6] || null,
    tel_no8: slots[7] || null,
    tel_no9: slots[8] || null,
    tel_no10: slots[9] || null,
    note_other,
  };
}
