// server.js
const express = require("express");
const path = require("path");
const fs = require("fs");
const dotenv = require("dotenv");
dotenv.config();

const { processCSVFile } = require("./app");

const PORT = process.env.PORT || 3000;
const CSV_PATH = process.env.CSV_PATH || "./data/users.csv";

const app = express();

app.get("/", (req, res) => {
  res.json({ status: "ok", csvPath: CSV_PATH });
});

app.post("/process", async (req, res) => {
  try {
    const csvAbsPath = path.resolve(CSV_PATH);
    if (!fs.existsSync(csvAbsPath)) {
      return res.status(400).json({ error: "CSV file not found", path: csvAbsPath });
    }

    res.json({ status: "processing", path: csvAbsPath });
    console.log("Starting processing of:", csvAbsPath);

    const result = await processCSVFile(csvAbsPath);
    console.log("Processing complete:", result);
  } catch (err) {
    console.error("Processing error:", err);
  }
});

app.listen(PORT, () => {
  console.log(`✅ Server listening on port ${PORT}`);
  console.log(`CSV path: ${CSV_PATH}`);
  console.log(`POST http://localhost:${PORT}/process to start processing`);
});
