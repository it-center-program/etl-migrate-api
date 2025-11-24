// แยกเบอร์โทรออกเป็น array
export function extractPhones(str) {
  if (!str) return [];
  return str
    .split(/[,; \/]+/)
    .map((s) => s.trim())
    .filter(Boolean);
}

// merge เบอร์เก่ากับใหม่
export function mergePhones(existing, newPhones) {
  let all = [];

  if (existing) {
    all.push(existing.tel_no);
    all.push(existing.tel_no2);
    all.push(existing.tel_no3);
    all.push(existing.tel_no4);
    all.push(existing.tel_no5);
    all.push(existing.tel_no6);
    all.push(existing.tel_no7);
    all.push(existing.tel_no8);
    all.push(existing.tel_no9);
    all.push(existing.tel_no10);
    if (existing.note_other) {
      all.push(...existing.note_other.split(","));
    }
  }

  all.push(...newPhones);
  all = [...new Set(all.filter(Boolean))];

  return {
    tel_no: all[0] || null,
    tel_no2: all[1] || null,
    tel_no3: all[2] || null,
    tel_no4: all[3] || null,
    tel_no5: all[4] || null,
    tel_no6: all[5] || null,
    tel_no7: all[6] || null,
    tel_no8: all[7] || null,
    tel_no9: all[8] || null,
    tel_no10: all[9] || null,
    note_other: all.length > 10 ? all.slice(10).join(",") : null,
  };
}
