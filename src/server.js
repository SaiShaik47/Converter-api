import express from "express";
import multer from "multer";
import os from "os";
import path from "path";
import fs from "fs/promises";
import { createReadStream } from "fs";
import { spawn } from "child_process";
import crypto from "crypto";
import XLSX from "xlsx";
import TelegramBot from "node-telegram-bot-api";

/* =========================
   CONFIG
========================= */
const app = express();
const PORT = process.env.PORT || 3000;
const BOT_TOKEN = process.env.BOT_TOKEN || "";

const MAX_MB = 25;
const MAX_BYTES = MAX_MB * 1024 * 1024;

/* =========================
   UPLOAD (API)
========================= */
const upload = multer({
  storage: multer.diskStorage({
    destination: (req, file, cb) => cb(null, os.tmpdir()),
    filename: (req, file, cb) => {
      const ext = path.extname(file.originalname || "").toLowerCase();
      const name = `upload_${Date.now()}_${crypto.randomBytes(6).toString("hex")}${ext}`;
      cb(null, name);
    }
  }),
  limits: { fileSize: MAX_BYTES }
});

function randName(ext) {
  return `f_${Date.now()}_${crypto.randomBytes(6).toString("hex")}${ext}`;
}

async function safeUnlink(p) {
  try { await fs.unlink(p); } catch {}
}

async function safeRmDir(dir) {
  try {
    const files = await fs.readdir(dir);
    await Promise.all(files.map(f => safeUnlink(path.join(dir, f))));
    await fs.rmdir(dir);
  } catch {}
}

function runCommand(cmd, args, options = {}) {
  return new Promise((resolve, reject) => {
    const p = spawn(cmd, args, { stdio: ["ignore", "pipe", "pipe"], ...options });
    let stderr = "";
    let stdout = "";

    if (p.stdout) p.stdout.on("data", d => (stdout += d.toString()));
    if (p.stderr) p.stderr.on("data", d => (stderr += d.toString()));

    p.on("error", (err) => {
      if (err.code === "ENOENT") {
        return reject(new Error(`${cmd} is not installed or not in PATH`));
      }
      return reject(err);
    });
    p.on("close", (code) => {
      if (code !== 0) {
        return reject(new Error(`${cmd} failed: ${stderr || stdout}`));
      }
      return resolve({ stdout, stderr });
    });
  });
}

/* =========================
   CONVERTERS
========================= */

// Excel -> PDF (LibreOffice)
function convertExcelToPdf(inputPath, outDir) {
  return new Promise((resolve, reject) => {
    const args = [
      "--headless",
      "--nologo",
      "--nofirststartwizard",
      "--nodefault",
      "--norestore",
      "--invisible",
      "--convert-to",
      "pdf:calc_pdf_Export",
      "--outdir",
      outDir,
      inputPath
    ];

    runCommand("soffice", args).then(() => resolve()).catch(reject);
  });
}

// PDF -> CSV (Tabula)
function tabulaPdfToCsv(pdfPath, outCsvPath, pages = "all") {
  return new Promise((resolve, reject) => {
    const jar = process.env.TABULA_JAR || "/opt/tabula/tabula.jar";
    const args = ["-jar", jar, "-p", pages, "-f", "CSV", "-o", outCsvPath, pdfPath];

    runCommand("java", args).then(() => resolve()).catch(reject);
  });
}

function libreOfficeConvert(inputPath, outDir, target, filter = "") {
  return new Promise((resolve, reject) => {
    const targetArg = filter ? `${target}:${filter}` : target;
    const args = [
      "--headless",
      "--nologo",
      "--nofirststartwizard",
      "--nodefault",
      "--norestore",
      "--invisible",
      "--convert-to",
      targetArg,
      "--outdir",
      outDir,
      inputPath
    ];

    runCommand("soffice", args)
      .then(async () => {
        const base = path.parse(inputPath).name;
        const outPath = path.join(outDir, `${base}.${target}`);
        await fs.access(outPath);
        resolve(outPath);
      })
      .catch(reject);
  });
}

// CSV -> XLSX (with quotes support)
function parseCsvLine(line) {
  const out = [];
  let cur = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];

    if (ch === '"') {
      if (inQuotes && line[i + 1] === '"') {
        cur += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (ch === "," && !inQuotes) {
      out.push(cur);
      cur = "";
      continue;
    }

    cur += ch;
  }

  out.push(cur);
  return out;
}

async function csvToXlsx(csvPath, xlsxPath) {
  const text = await fs.readFile(csvPath, "utf8");
  const lines = text.split(/\r?\n/).filter(l => l.trim().length > 0);
  const rows = lines.map(parseCsvLine);

  const wb = XLSX.utils.book_new();
  const ws = XLSX.utils.aoa_to_sheet(rows);
  XLSX.utils.book_append_sheet(wb, ws, "Extracted");
  XLSX.writeFile(wb, xlsxPath);
}

async function xlsxToCsv(xlsxPath, csvPath) {
  const wb = XLSX.readFile(xlsxPath);
  const first = wb.SheetNames[0];
  const csv = XLSX.utils.sheet_to_csv(wb.Sheets[first]);
  await fs.writeFile(csvPath, csv);
}

async function csvToJson(csvPath, jsonPath) {
  const text = await fs.readFile(csvPath, "utf8");
  const lines = text.split(/\r?\n/).filter(l => l.trim().length > 0);
  if (lines.length === 0) {
    await fs.writeFile(jsonPath, "[]");
    return;
  }

  const headers = parseCsvLine(lines[0]).map(h => h.trim());
  const rows = lines.slice(1).map(line => parseCsvLine(line));
  const data = rows.map(row => {
    const obj = {};
    headers.forEach((h, idx) => {
      obj[h] = row[idx] ?? "";
    });
    return obj;
  });

  await fs.writeFile(jsonPath, JSON.stringify(data, null, 2));
}

function normalizeJsonArray(jsonValue) {
  if (Array.isArray(jsonValue)) return jsonValue;
  if (jsonValue && typeof jsonValue === "object") return [jsonValue];
  return [];
}

async function jsonToCsv(jsonPath, csvPath) {
  const raw = await fs.readFile(jsonPath, "utf8");
  const data = normalizeJsonArray(JSON.parse(raw));
  const keys = Array.from(new Set(data.flatMap(row => Object.keys(row || {}))));

  const lines = [];
  lines.push(keys.join(","));
  for (const row of data) {
    const values = keys.map(key => {
      const value = row?.[key] ?? "";
      const str = String(value).replace(/"/g, '""');
      return str.includes(",") || str.includes("\n") ? `"${str}"` : str;
    });
    lines.push(values.join(","));
  }

  await fs.writeFile(csvPath, lines.join("\n"));
}

async function jsonToXlsx(jsonPath, xlsxPath) {
  const raw = await fs.readFile(jsonPath, "utf8");
  const data = normalizeJsonArray(JSON.parse(raw));
  const ws = XLSX.utils.json_to_sheet(data);
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, "Data");
  XLSX.writeFile(wb, xlsxPath);
}

async function xlsxToJson(xlsxPath, jsonPath) {
  const wb = XLSX.readFile(xlsxPath);
  const first = wb.SheetNames[0];
  const json = XLSX.utils.sheet_to_json(wb.Sheets[first], { defval: "" });
  await fs.writeFile(jsonPath, JSON.stringify(json, null, 2));
}

async function pdfToText(pdfPath, txtPath) {
  await runCommand("pdftotext", [pdfPath, txtPath]);
}

async function pdfToImagesZip(pdfPath, zipPath, workDir) {
  const prefix = path.join(workDir, "page");
  await runCommand("pdftoppm", ["-png", pdfPath, prefix]);
  await runCommand("zip", ["-j", zipPath, `${prefix}-*.png`], { shell: true });
}

async function imageToPdf(imagePath, pdfPath) {
  try {
    await runCommand("magick", [imagePath, pdfPath]);
  } catch (err) {
    if (!err.message.includes("magick")) throw err;
    await runCommand("convert", [imagePath, pdfPath]);
  }
}

/* =========================
   EXPRESS API
========================= */
app.get("/", (req, res) => {
  res.json({
    ok: true,
    service: "Converter API + Telegram Bot",
    endpoints: {
      excel_to_pdf: "POST /excel-to-pdf (form-data key: file)",
      pdf_to_excel: "POST /pdf-to-excel?pages=all (form-data key: file)",
      pdf_to_txt: "POST /pdf-to-txt (form-data key: file)",
      txt_to_pdf: "POST /txt-to-pdf (form-data key: file)",
      docx_to_pdf: "POST /docx-to-pdf (form-data key: file)",
      pdf_to_docx: "POST /pdf-to-docx (form-data key: file)",
      pptx_to_pdf: "POST /pptx-to-pdf (form-data key: file)",
      csv_to_xlsx: "POST /csv-to-xlsx (form-data key: file)",
      xlsx_to_csv: "POST /xlsx-to-csv (form-data key: file)",
      csv_to_json: "POST /csv-to-json (form-data key: file)",
      json_to_csv: "POST /json-to-csv (form-data key: file)",
      xlsx_to_json: "POST /xlsx-to-json (form-data key: file)",
      json_to_xlsx: "POST /json-to-xlsx (form-data key: file)",
      pdf_to_images: "POST /pdf-to-images (form-data key: file)",
      image_to_pdf: "POST /image-to-pdf (form-data key: file)"
    },
    limits: { max_upload_mb: MAX_MB }
  });
});

async function handleFileConversion(req, res, options) {
  const inputPath = req.file?.path;
  if (!inputPath) return res.status(400).json({ ok: false, error: "Upload file using key: file" });

  const ext = path.extname(req.file.originalname || "").toLowerCase();
  if (options.allowedExts && !options.allowedExts.includes(ext)) {
    await safeUnlink(inputPath);
    return res.status(400).json({ ok: false, error: options.invalidExtMessage });
  }

  const workDir = await fs.mkdtemp(path.join(os.tmpdir(), options.workPrefix || "conv-"));
  try {
    const outputPath = await options.convert(inputPath, workDir);
    res.setHeader("Content-Type", options.contentType);
    res.setHeader("Content-Disposition", `attachment; filename="${options.outputName}"`);
    createReadStream(outputPath).pipe(res);
    res.on("finish", async () => {
      await safeUnlink(inputPath);
      await safeRmDir(workDir);
    });
  } catch (e) {
    await safeUnlink(inputPath);
    await safeRmDir(workDir);
    res.status(500).json({ ok: false, error: e.message || "Conversion failed" });
  }
}

app.post("/excel-to-pdf", upload.single("file"), async (req, res) => {
  const inputPath = req.file?.path;
  if (!inputPath) return res.status(400).json({ ok: false, error: "Upload file using key: file" });

  const ext = path.extname(req.file.originalname || "").toLowerCase();
  if (![".xlsx", ".xls"].includes(ext)) {
    await safeUnlink(inputPath);
    return res.status(400).json({ ok: false, error: "Only .xlsx or .xls allowed" });
  }

  const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "x2p-"));

  try {
    await convertExcelToPdf(inputPath, workDir);
    const base = path.parse(inputPath).name;
    const pdfPath = path.join(workDir, `${base}.pdf`);
    await fs.access(pdfPath);

    res.setHeader("Content-Type", "application/pdf");
    res.setHeader("Content-Disposition", `attachment; filename="output.pdf"`);
    createReadStream(pdfPath).pipe(res);

    res.on("finish", async () => {
      await safeUnlink(inputPath);
      await safeRmDir(workDir);
    });
  } catch (e) {
    await safeUnlink(inputPath);
    await safeRmDir(workDir);
    res.status(500).json({ ok: false, error: e.message || "Conversion failed" });
  }
});

app.post("/pdf-to-excel", upload.single("file"), async (req, res) => {
  const inputPath = req.file?.path;
  if (!inputPath) return res.status(400).json({ ok: false, error: "Upload file using key: file" });

  const ext = path.extname(req.file.originalname || "").toLowerCase();
  if (ext !== ".pdf") {
    await safeUnlink(inputPath);
    return res.status(400).json({ ok: false, error: "Only .pdf allowed" });
  }

  const pages = (req.query.pages || "all").toString();
  const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "p2x-"));
  const outCsv = path.join(workDir, "tables.csv");
  const outXlsx = path.join(workDir, "output.xlsx");

  try {
    await tabulaPdfToCsv(inputPath, outCsv, pages);
    await csvToXlsx(outCsv, outXlsx);

    res.setHeader(
      "Content-Type",
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    );
    res.setHeader("Content-Disposition", `attachment; filename="output.xlsx"`);
    createReadStream(outXlsx).pipe(res);

    res.on("finish", async () => {
      await safeUnlink(inputPath);
      await safeRmDir(workDir);
    });
  } catch (e) {
    await safeUnlink(inputPath);
    await safeRmDir(workDir);
    res.status(500).json({ ok: false, error: e.message || "Conversion failed" });
  }
});

app.post("/pdf-to-txt", upload.single("file"), async (req, res) => {
  await handleFileConversion(req, res, {
    allowedExts: [".pdf"],
    invalidExtMessage: "Only .pdf allowed",
    workPrefix: "p2t-",
    contentType: "text/plain",
    outputName: "output.txt",
    convert: async (inputPath, workDir) => {
      const outPath = path.join(workDir, "output.txt");
      await pdfToText(inputPath, outPath);
      return outPath;
    }
  });
});

app.post("/txt-to-pdf", upload.single("file"), async (req, res) => {
  await handleFileConversion(req, res, {
    allowedExts: [".txt"],
    invalidExtMessage: "Only .txt allowed",
    workPrefix: "t2p-",
    contentType: "application/pdf",
    outputName: "output.pdf",
    convert: async (inputPath, workDir) => libreOfficeConvert(inputPath, workDir, "pdf")
  });
});

app.post("/docx-to-pdf", upload.single("file"), async (req, res) => {
  await handleFileConversion(req, res, {
    allowedExts: [".docx"],
    invalidExtMessage: "Only .docx allowed",
    workPrefix: "d2p-",
    contentType: "application/pdf",
    outputName: "output.pdf",
    convert: async (inputPath, workDir) => libreOfficeConvert(inputPath, workDir, "pdf")
  });
});

app.post("/pdf-to-docx", upload.single("file"), async (req, res) => {
  await handleFileConversion(req, res, {
    allowedExts: [".pdf"],
    invalidExtMessage: "Only .pdf allowed",
    workPrefix: "p2d-",
    contentType: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    outputName: "output.docx",
    convert: async (inputPath, workDir) => libreOfficeConvert(inputPath, workDir, "docx")
  });
});

app.post("/pptx-to-pdf", upload.single("file"), async (req, res) => {
  await handleFileConversion(req, res, {
    allowedExts: [".pptx"],
    invalidExtMessage: "Only .pptx allowed",
    workPrefix: "p2p-",
    contentType: "application/pdf",
    outputName: "output.pdf",
    convert: async (inputPath, workDir) => libreOfficeConvert(inputPath, workDir, "pdf")
  });
});

app.post("/csv-to-xlsx", upload.single("file"), async (req, res) => {
  await handleFileConversion(req, res, {
    allowedExts: [".csv"],
    invalidExtMessage: "Only .csv allowed",
    workPrefix: "c2x-",
    contentType: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    outputName: "output.xlsx",
    convert: async (inputPath, workDir) => {
      const outPath = path.join(workDir, "output.xlsx");
      await csvToXlsx(inputPath, outPath);
      return outPath;
    }
  });
});

app.post("/xlsx-to-csv", upload.single("file"), async (req, res) => {
  await handleFileConversion(req, res, {
    allowedExts: [".xlsx", ".xls"],
    invalidExtMessage: "Only .xlsx or .xls allowed",
    workPrefix: "x2c-",
    contentType: "text/csv",
    outputName: "output.csv",
    convert: async (inputPath, workDir) => {
      const outPath = path.join(workDir, "output.csv");
      await xlsxToCsv(inputPath, outPath);
      return outPath;
    }
  });
});

app.post("/csv-to-json", upload.single("file"), async (req, res) => {
  await handleFileConversion(req, res, {
    allowedExts: [".csv"],
    invalidExtMessage: "Only .csv allowed",
    workPrefix: "c2j-",
    contentType: "application/json",
    outputName: "output.json",
    convert: async (inputPath, workDir) => {
      const outPath = path.join(workDir, "output.json");
      await csvToJson(inputPath, outPath);
      return outPath;
    }
  });
});

app.post("/json-to-csv", upload.single("file"), async (req, res) => {
  await handleFileConversion(req, res, {
    allowedExts: [".json"],
    invalidExtMessage: "Only .json allowed",
    workPrefix: "j2c-",
    contentType: "text/csv",
    outputName: "output.csv",
    convert: async (inputPath, workDir) => {
      const outPath = path.join(workDir, "output.csv");
      await jsonToCsv(inputPath, outPath);
      return outPath;
    }
  });
});

app.post("/xlsx-to-json", upload.single("file"), async (req, res) => {
  await handleFileConversion(req, res, {
    allowedExts: [".xlsx", ".xls"],
    invalidExtMessage: "Only .xlsx or .xls allowed",
    workPrefix: "x2j-",
    contentType: "application/json",
    outputName: "output.json",
    convert: async (inputPath, workDir) => {
      const outPath = path.join(workDir, "output.json");
      await xlsxToJson(inputPath, outPath);
      return outPath;
    }
  });
});

app.post("/json-to-xlsx", upload.single("file"), async (req, res) => {
  await handleFileConversion(req, res, {
    allowedExts: [".json"],
    invalidExtMessage: "Only .json allowed",
    workPrefix: "j2x-",
    contentType: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    outputName: "output.xlsx",
    convert: async (inputPath, workDir) => {
      const outPath = path.join(workDir, "output.xlsx");
      await jsonToXlsx(inputPath, outPath);
      return outPath;
    }
  });
});

app.post("/pdf-to-images", upload.single("file"), async (req, res) => {
  await handleFileConversion(req, res, {
    allowedExts: [".pdf"],
    invalidExtMessage: "Only .pdf allowed",
    workPrefix: "p2i-",
    contentType: "application/zip",
    outputName: "images.zip",
    convert: async (inputPath, workDir) => {
      const outPath = path.join(workDir, "images.zip");
      await pdfToImagesZip(inputPath, outPath, workDir);
      return outPath;
    }
  });
});

app.post("/image-to-pdf", upload.single("file"), async (req, res) => {
  await handleFileConversion(req, res, {
    allowedExts: [".jpg", ".jpeg", ".png", ".tiff", ".bmp"],
    invalidExtMessage: "Only .jpg, .jpeg, .png, .tiff, or .bmp allowed",
    workPrefix: "i2p-",
    contentType: "application/pdf",
    outputName: "output.pdf",
    convert: async (inputPath, workDir) => {
      const outPath = path.join(workDir, "output.pdf");
      await imageToPdf(inputPath, outPath);
      return outPath;
    }
  });
});

/* =========================
   TELEGRAM BOT (RICH UX)
========================= */
function startTelegramBot() {
  if (!BOT_TOKEN) {
    console.log("BOT_TOKEN not set. Telegram bot disabled (API still works).");
    return;
  }

  const bot = new TelegramBot(BOT_TOKEN, { polling: true });

  const startText =
`✨ File Converter Bot

Send me a file and I will convert it. Add a caption like "to pdf" or "to xlsx" to pick a target.

Examples:
• Upload invoice.pdf with caption "to docx"
• Upload data.csv with caption "to xlsx"

Limits:
• Max size: ${MAX_MB} MB

Commands:
/start - welcome
/help  - how to use
/status - check bot`;

  const helpText =
`🧠 How to use

1) Send a file with optional caption:
   • "to pdf", "to docx", "to xlsx", "to txt", "to json", "to csv"

2) Supported conversions (examples):
   • Excel → PDF / CSV / JSON
   • PDF → Excel / TXT / DOCX / Images (zip)
   • DOCX / PPTX / TXT → PDF
   • CSV ↔ JSON, CSV → XLSX
   • JSON → CSV / XLSX
   • Image → PDF

3) File limit:
   • Max ${MAX_MB} MB

If something fails:
• Try a smaller file
• Check that the target format is supported`;

  bot.onText(/\/start/, (msg) => bot.sendMessage(msg.chat.id, startText));
  bot.onText(/\/help/, (msg) => bot.sendMessage(msg.chat.id, helpText));
  bot.onText(/\/status/, (msg) => bot.sendMessage(msg.chat.id, "✅ Bot is running and ready. Send a file."));

  function parseTarget(caption) {
    if (!caption) return null;
    const match = caption.match(/(?:^|\s)(?:to|convert\s+to|\/to)[:\s]+([a-z0-9]+)/i);
    return match ? match[1].toLowerCase() : null;
  }

  const telegramConversions = {
    ".xlsx": {
      pdf: { label: "Excel → PDF", outputExt: ".pdf", convert: (input, dir) => libreOfficeConvert(input, dir, "pdf") },
      csv: { label: "Excel → CSV", outputExt: ".csv", convert: async (input, dir) => {
        const outPath = path.join(dir, "output.csv");
        await xlsxToCsv(input, outPath);
        return outPath;
      }},
      json: { label: "Excel → JSON", outputExt: ".json", convert: async (input, dir) => {
        const outPath = path.join(dir, "output.json");
        await xlsxToJson(input, outPath);
        return outPath;
      }}
    },
    ".xls": {
      pdf: { label: "Excel → PDF", outputExt: ".pdf", convert: (input, dir) => libreOfficeConvert(input, dir, "pdf") },
      csv: { label: "Excel → CSV", outputExt: ".csv", convert: async (input, dir) => {
        const outPath = path.join(dir, "output.csv");
        await xlsxToCsv(input, outPath);
        return outPath;
      }},
      json: { label: "Excel → JSON", outputExt: ".json", convert: async (input, dir) => {
        const outPath = path.join(dir, "output.json");
        await xlsxToJson(input, outPath);
        return outPath;
      }}
    },
    ".pdf": {
      xlsx: { label: "PDF → Excel", outputExt: ".xlsx", convert: async (input, dir) => {
        const outCsv = path.join(dir, "tables.csv");
        const outXlsx = path.join(dir, "output.xlsx");
        await tabulaPdfToCsv(input, outCsv, "all");
        await csvToXlsx(outCsv, outXlsx);
        return outXlsx;
      }},
      txt: { label: "PDF → TXT", outputExt: ".txt", convert: async (input, dir) => {
        const outPath = path.join(dir, "output.txt");
        await pdfToText(input, outPath);
        return outPath;
      }},
      docx: { label: "PDF → DOCX", outputExt: ".docx", convert: (input, dir) => libreOfficeConvert(input, dir, "docx") },
      images: { label: "PDF → Images (ZIP)", outputExt: ".zip", convert: async (input, dir) => {
        const outPath = path.join(dir, "images.zip");
        await pdfToImagesZip(input, outPath, dir);
        return outPath;
      }}
    },
    ".docx": {
      pdf: { label: "DOCX → PDF", outputExt: ".pdf", convert: (input, dir) => libreOfficeConvert(input, dir, "pdf") }
    },
    ".pptx": {
      pdf: { label: "PPTX → PDF", outputExt: ".pdf", convert: (input, dir) => libreOfficeConvert(input, dir, "pdf") }
    },
    ".txt": {
      pdf: { label: "TXT → PDF", outputExt: ".pdf", convert: (input, dir) => libreOfficeConvert(input, dir, "pdf") }
    },
    ".csv": {
      xlsx: { label: "CSV → XLSX", outputExt: ".xlsx", convert: async (input, dir) => {
        const outPath = path.join(dir, "output.xlsx");
        await csvToXlsx(input, outPath);
        return outPath;
      }},
      json: { label: "CSV → JSON", outputExt: ".json", convert: async (input, dir) => {
        const outPath = path.join(dir, "output.json");
        await csvToJson(input, outPath);
        return outPath;
      }}
    },
    ".json": {
      csv: { label: "JSON → CSV", outputExt: ".csv", convert: async (input, dir) => {
        const outPath = path.join(dir, "output.csv");
        await jsonToCsv(input, outPath);
        return outPath;
      }},
      xlsx: { label: "JSON → XLSX", outputExt: ".xlsx", convert: async (input, dir) => {
        const outPath = path.join(dir, "output.xlsx");
        await jsonToXlsx(input, outPath);
        return outPath;
      }}
    },
    ".jpg": {
      pdf: { label: "Image → PDF", outputExt: ".pdf", convert: async (input, dir) => {
        const outPath = path.join(dir, "output.pdf");
        await imageToPdf(input, outPath);
        return outPath;
      }}
    },
    ".jpeg": {
      pdf: { label: "Image → PDF", outputExt: ".pdf", convert: async (input, dir) => {
        const outPath = path.join(dir, "output.pdf");
        await imageToPdf(input, outPath);
        return outPath;
      }}
    },
    ".png": {
      pdf: { label: "Image → PDF", outputExt: ".pdf", convert: async (input, dir) => {
        const outPath = path.join(dir, "output.pdf");
        await imageToPdf(input, outPath);
        return outPath;
      }}
    },
    ".tiff": {
      pdf: { label: "Image → PDF", outputExt: ".pdf", convert: async (input, dir) => {
        const outPath = path.join(dir, "output.pdf");
        await imageToPdf(input, outPath);
        return outPath;
      }}
    }
  };

  bot.on("document", async (msg) => {
    const chatId = msg.chat.id;
    const doc = msg.document;

    const fileName = doc.file_name || "file";
    const ext = path.extname(fileName).toLowerCase();
    const target = parseTarget(msg.caption);

    // Telegram gives file size too
    const size = doc.file_size || 0;
    if (size > MAX_BYTES) {
      return bot.sendMessage(chatId, `❌ File too large. Max allowed is ${MAX_MB} MB.`);
    }

    const options = telegramConversions[ext] || {};
    const defaultTarget = Object.keys(options)[0];
    const resolvedTarget = target || defaultTarget;
    const conversion = resolvedTarget ? options[resolvedTarget] : null;

    if (!conversion) {
      const supportedTargets = Object.keys(options);
      if (supportedTargets.length === 0) {
        return bot.sendMessage(chatId, "❌ Unsupported file type. Send a supported file (PDF, DOCX, PPTX, XLSX, CSV, JSON, TXT, JPG/PNG).");
      }
      return bot.sendMessage(
        chatId,
        `❌ Unsupported target. Try: ${supportedTargets.map(t => `"to ${t}"`).join(", ")}.`
      );
    }

    const status = await bot.sendMessage(chatId, `⏳ Received: *${conversion.label}*\nDownloading...`, {
      parse_mode: "Markdown"
    });

    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "tg-"));
    const outputPath = path.join(os.tmpdir(), randName(conversion.outputExt));

    try {
      // Download file from Telegram
      const downloadedPath = await bot.downloadFile(doc.file_id, workDir);

      await bot.editMessageText(`⚙️ Converting: *${conversion.label}*\nPlease wait...`, {
        chat_id: chatId,
        message_id: status.message_id,
        parse_mode: "Markdown"
      });

      const convertedPath = await conversion.convert(downloadedPath, workDir);
      await fs.copyFile(convertedPath, outputPath);

      await bot.editMessageText(`✅ Done: *${conversion.label}*\nUploading result...`, {
        chat_id: chatId,
        message_id: status.message_id,
        parse_mode: "Markdown"
      });

      // Send result file back
      await bot.sendDocument(chatId, outputPath, {
        caption: `✅ ${conversion.label}`
      });

      if (resolvedTarget === "xlsx" && ext === ".pdf") {
        await bot.sendMessage(chatId, "Tip: If this PDF was scanned, results can be messy. Send a text-based PDF for best tables.");
      }

    } catch (e) {
      await bot.sendMessage(chatId, `❌ Conversion failed.\nReason: ${e.message}`);
    } finally {
      await safeUnlink(outputPath);
      await safeRmDir(workDir);
    }
  });

  console.log("Telegram bot started (polling).");
}

/* =========================
   START
========================= */
app.listen(PORT, () => {
  console.log(`API running on port ${PORT}`);
  startTelegramBot();
});
