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

    const p = spawn("soffice", args, { stdio: ["ignore", "pipe", "pipe"] });

    let err = "";
    p.stderr.on("data", d => (err += d.toString()));
    p.on("error", reject);
    p.on("close", code => {
      if (code !== 0) return reject(new Error(`LibreOffice failed: ${err}`));
      resolve();
    });
  });
}

// PDF -> CSV (Tabula)
function tabulaPdfToCsv(pdfPath, outCsvPath, pages = "all") {
  return new Promise((resolve, reject) => {
    const jar = process.env.TABULA_JAR || "/opt/tabula/tabula.jar";
    const args = ["-jar", jar, "-p", pages, "-f", "CSV", "-o", outCsvPath, pdfPath];

    const p = spawn("java", args, { stdio: ["ignore", "pipe", "pipe"] });

    let err = "";
    p.stderr.on("data", d => (err += d.toString()));
    p.on("error", reject);
    p.on("close", code => {
      if (code !== 0) return reject(new Error(`Tabula failed: ${err}`));
      resolve();
    });
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

/* =========================
   EXPRESS API
========================= */
app.get("/", (req, res) => {
  res.json({
    ok: true,
    service: "Converter API + Telegram Bot",
    endpoints: {
      excel_to_pdf: "POST /excel-to-pdf (form-data key: file)",
      pdf_to_excel: "POST /pdf-to-excel?pages=all (form-data key: file)"
    },
    limits: { max_upload_mb: MAX_MB }
  });
});

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

Send me a file and I will convert it:

✅ Excel (.xlsx / .xls) → PDF
✅ PDF → Excel (.xlsx)

Limits:
• Max size: ${MAX_MB} MB
• Best PDF→Excel result: when PDF has real text tables (not scanned)

Commands:
/start - welcome
/help  - how to use
/status - check bot`;

  const helpText =
`🧠 How to use

1) Just send a file:
   • Excel → I return PDF
   • PDF → I return Excel

2) Tips for best result (PDF → Excel):
   • Works best when PDF text is selectable
   • Scanned PDFs may give poor tables

3) File limit:
   • Max ${MAX_MB} MB

If something fails:
• Try a smaller file
• Try pages with clearer tables`;

  bot.onText(/\/start/, (msg) => bot.sendMessage(msg.chat.id, startText));
  bot.onText(/\/help/, (msg) => bot.sendMessage(msg.chat.id, helpText));
  bot.onText(/\/status/, (msg) => bot.sendMessage(msg.chat.id, "✅ Bot is running and ready. Send a file."));

  bot.on("document", async (msg) => {
    const chatId = msg.chat.id;
    const doc = msg.document;

    const fileName = doc.file_name || "file";
    const ext = path.extname(fileName).toLowerCase();

    // Telegram gives file size too
    const size = doc.file_size || 0;
    if (size > MAX_BYTES) {
      return bot.sendMessage(chatId, `❌ File too large. Max allowed is ${MAX_MB} MB.`);
    }

    let convertType = null;
    let outputExt = null;

    if ([".xlsx", ".xls"].includes(ext)) {
      convertType = "excel2pdf";
      outputExt = ".pdf";
    } else if (ext === ".pdf") {
      convertType = "pdf2excel";
      outputExt = ".xlsx";
    } else {
      return bot.sendMessage(chatId, "❌ Unsupported file. Send only Excel (.xlsx/.xls) or PDF (.pdf).");
    }

    const niceName =
      convertType === "excel2pdf"
        ? "Excel → PDF"
        : "PDF → Excel";

    const status = await bot.sendMessage(chatId, `⏳ Received: *${niceName}*\nDownloading...`, {
      parse_mode: "Markdown"
    });

    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "tg-"));
    const outputPath = path.join(os.tmpdir(), randName(outputExt));

    try {
      // Download file from Telegram
      const downloadedPath = await bot.downloadFile(doc.file_id, workDir);

      await bot.editMessageText(`⚙️ Converting: *${niceName}*\nPlease wait...`, {
        chat_id: chatId,
        message_id: status.message_id,
        parse_mode: "Markdown"
      });

      if (convertType === "excel2pdf") {
        const outDir = await fs.mkdtemp(path.join(os.tmpdir(), "x2p-"));
        try {
          await convertExcelToPdf(downloadedPath, outDir);
          const base = path.parse(downloadedPath).name;
          const pdfPath = path.join(outDir, `${base}.pdf`);
          await fs.access(pdfPath);
          await fs.copyFile(pdfPath, outputPath);
          await safeRmDir(outDir);
        } catch (e) {
          await safeRmDir(outDir);
          throw e;
        }
      } else {
        const outCsv = path.join(workDir, "tables.csv");
        await tabulaPdfToCsv(downloadedPath, outCsv, "all");
        await csvToXlsx(outCsv, outputPath);
      }

      await bot.editMessageText(`✅ Done: *${niceName}*\nUploading result...`, {
        chat_id: chatId,
        message_id: status.message_id,
        parse_mode: "Markdown"
      });

      // Send result file back
      await bot.sendDocument(chatId, outputPath, {
        caption:
          convertType === "excel2pdf"
            ? "✅ Converted to PDF"
            : "✅ Converted to Excel"
      });

      // Friendly extra note for PDF->Excel
      if (convertType === "pdf2excel") {
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
