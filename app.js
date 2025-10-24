const fs = require("fs");
const readline = require("readline");
const path = require("path");
const { pool, ensureTable } = require("./db");
const { parseCSVLineToFields, isRecordComplete } = require("./csvParser");
const { setNested } = require("./utils");

const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || process.env.BATCHSIZE || "1000", 10);

async function processCSVFile(csvPath) {
  await ensureTable();

  const fileStream = fs.createReadStream(csvPath, { encoding: "utf8" });
  const rl = readline.createInterface({ input: fileStream, crlfDelay: Infinity });

  let acc = "";
  let isFirstRecord = true;
  let headers = null;
  let headerCount = 0;
  let batchRows = [];
  let totalProcessed = 0;

  async function flushBatch() {
    if (!batchRows.length) return;

    const params = [];
    let paramIndex = 1;
    const placeholders = batchRows
      .map((row) => {
        params.push(row.name, row.age, JSON.stringify(row.address || null), JSON.stringify(row.additional_info || null));
        const place = `($${paramIndex}, $${paramIndex + 1}, $${paramIndex + 2}, $${paramIndex + 3})`;
        paramIndex += 4;
        return place;
      })
      .join(", ");

    fs.writeFileSync("./output.json", JSON.stringify(batchRows, null, 2));
    console.log("✅ JSON file written to output.json");

    const sql = `INSERT INTO public.users ("name", age, address, additional_info) VALUES ${placeholders};`;
    await pool.query(sql, params);

    totalProcessed += batchRows.length;
    batchRows = [];
  }

  for await (const physicalLine of rl) {
    if (acc.length > 0) acc += "\n";
    acc += physicalLine;

    if (!isRecordComplete(acc)) continue;

    const recordLine = acc;
    acc = "";

    const fields = parseCSVLineToFields(recordLine);

    if (isFirstRecord) {
      headers = fields.map((h) => h.trim());
      headerCount = headers.length;

      const required = ["name.firstName", "name.lastName", "age"];
      for (let i = 0; i < required.length; i++) {
        if (!headers[i] || headers[i] !== required[i]) {
          throw new Error(`Header validation failed. Expected header[${i}]="${required[i]}", got "${headers[i]}"`);
        }
      }
      isFirstRecord = false;
      continue;
    }

    while (fields.length < headerCount) fields.push("");

    const mapped = {};
    for (let i = 0; i < headerCount; i++) mapped[headers[i]] = fields[i] || "";

    const firstName = mapped["name.firstName"] || "";
    const lastName = mapped["name.lastName"] || "";
    const fullName = `${firstName}${lastName ? " " + lastName : ""}`.trim();

    const ageRaw = mapped["age"] || "";
    const age = parseInt(ageRaw.toString().trim(), 10);
    if (Number.isNaN(age)) throw new Error(`Invalid age value: "${ageRaw}"`);

    const addressObj = {};
    const additionalInfo = {};
    for (const [key, value] of Object.entries(mapped)) {
      if (["name.firstName", "name.lastName", "age"].includes(key)) continue;
      if (key.startsWith("address.")) setNested(addressObj, key.split(".").slice(1), value);
      else setNested(additionalInfo, key.split("."), value);
    }

    batchRows.push({
      name: fullName,
      age,
      address: Object.keys(addressObj).length ? addressObj : null,
      additional_info: Object.keys(additionalInfo).length ? additionalInfo : null,
    });

    if (batchRows.length >= BATCH_SIZE) await flushBatch();
  }

  await flushBatch();
  console.log(`✅ CSV processed. Total rows inserted: ${totalProcessed}`);

  // ---------------- Get age distribution from DB ----------------
//   const distributionSql = `
//     SELECT
//       SUM(CASE WHEN age < 20 THEN 1 ELSE 0 END) AS lt20,
//       SUM(CASE WHEN age >= 20 AND age <= 40 THEN 1 ELSE 0 END) AS b20_40,
//       SUM(CASE WHEN age > 40 AND age <= 60 THEN 1 ELSE 0 END) AS b40_60,
//       SUM(CASE WHEN age > 60 THEN 1 ELSE 0 END) AS gt60
//     FROM public.users;
//   `;
//   const { rows } = await pool.query(distributionSql);
//   const ageDistribution = rows[0];
//   console.log("✅ Age distribution from DB:", ageDistribution);

//   return { totalProcessed, ageDistribution };
// }

const distributionSql = `
  SELECT
    SUM(CASE WHEN age < 20 THEN 1 ELSE 0 END) AS "<20",
    SUM(CASE WHEN age >= 20 AND age <= 40 THEN 1 ELSE 0 END) AS "20-40",
    SUM(CASE WHEN age > 40 AND age <= 60 THEN 1 ELSE 0 END) AS "41-60",
    SUM(CASE WHEN age > 60 THEN 1 ELSE 0 END) AS ">60"
  FROM public.users;
`;

const { rows } = await pool.query(distributionSql);
const ageDistribution = rows[0];

// Print in table format
console.log("\n✅ Age distribution (from DB):");
console.table([ageDistribution]);
}
module.exports = { processCSVFile };
