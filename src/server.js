import express from "express";
import multer from "multer";
import os from "os";
import path from "path";
import fs from "fs/promises";
import { createReadStream, createWriteStream } from "fs";
import { pipeline } from "stream/promises";
import { spawn } from "child_process";
import crypto from "crypto";
import XLSX from "xlsx";
import TelegramBot from "node-telegram-bot-api";
import pdfParse from "pdf-parse";
import PDFKitDocument from "pdfkit";
import { PDFDocument, StandardFonts, degrees, rgb } from "pdf-lib";
import https from "https";
import archiver from "archiver";
import { Document, Packer, Paragraph } from "docx";
import { createCanvas } from "@napi-rs/canvas";
import * as pdfjsLib from "pdfjs-dist/legacy/build/pdf.mjs";

/* =========================
   CONFIG
========================= */
const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

const PORT = process.env.PORT || 3000;
const BOT_TOKEN = process.env.BOT_TOKEN || "";
const LOG_CHANNEL_ID = process.env.LOG_CHANNEL_ID || "-1003575783554";
const LOG_CHANNEL_USERNAME = process.env.LOG_CHANNEL_USERNAME || "@OsintLogsUpdates";

const DEFAULT_MAX_MB = 25;
const MAX_FILE_MB_CAP = 2048;

function resolveLimitMb(rawValue, fallbackMb, capMb = MAX_FILE_MB_CAP) {
  const parsed = Number(rawValue ?? fallbackMb);
  if (!Number.isFinite(parsed)) return fallbackMb;
  return Math.max(1, Math.min(capMb, Math.floor(parsed)));
}

const MAX_MB = resolveLimitMb(process.env.API_MAX_MB, DEFAULT_MAX_MB);
const MAX_BYTES = MAX_MB * 1024 * 1024;
const TELEGRAM_MAX_MB_CAP = 2048;
const telegramMaxMb = resolveLimitMb(process.env.TELEGRAM_MAX_MB, MAX_MB, TELEGRAM_MAX_MB_CAP);
const ADMIN_USER_ID = 5695514027;
const ADMIN_USERNAME = "hayforks";
const USERS_DB_PATH =
  process.env.USERS_DB_PATH ||
  path.join(process.cwd(), "data", "users.json");

async function ensureUsersDbFile() {
  const dir = path.dirname(USERS_DB_PATH);
  await fs.mkdir(dir, { recursive: true });

  try {
    await fs.access(USERS_DB_PATH);
  } catch {
    await fs.writeFile(
      USERS_DB_PATH,
      JSON.stringify({ users: {} }, null, 2),
      "utf8"
    );
  }
}

async function loadUsersDb() {
  await ensureUsersDbFile();
  const raw = await fs.readFile(USERS_DB_PATH, "utf8");

  try {
    const db = JSON.parse(raw || "{}");
    if (!db.users || typeof db.users !== "object") db.users = {};
    return db;
  } catch {
    const db = { users: {} };
    await fs.writeFile(USERS_DB_PATH, JSON.stringify(db, null, 2), "utf8");
    return db;
  }
}

async function saveUsersDb(db) {
  await ensureUsersDbFile();
  await fs.writeFile(USERS_DB_PATH, JSON.stringify(db, null, 2), "utf8");
}

function normalizeUserRecord(existing, from) {
  const now = new Date().toISOString();
  return {
    userId: String(from?.id || existing?.userId || ""),
    username: from?.username ? `@${from.username}` : (existing?.username || ""),
    fullName: [from?.first_name, from?.last_name].filter(Boolean).join(" ") || existing?.fullName || "Unknown",
    status: existing?.status || "pending",
    createdAt: existing?.createdAt || now,
    updatedAt: now,
    approvedAt: existing?.approvedAt || null,
    rejectedAt: existing?.rejectedAt || null
  };
}

function buildApprovalKeyboard(userId) {
  return {
    inline_keyboard: [[
      { text: "✅ Approve", callback_data: `admin:approve:${userId}` },
      { text: "❌ Reject", callback_data: `admin:reject:${userId}` }
    ]]
  };
}

// ===== ADMIN CHECK (FIX for: isAdminUser is not defined) =====
const ADMIN_IDS = (process.env.ADMIN_IDS || "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean)
  .map(Number);

const ADMIN_USERNAMES = (process.env.ADMIN_USERNAMES || "")
  .split(",")
  .map((s) => s.trim().replace(/^@/, "").toLowerCase())
  .filter(Boolean);

function isAdminUser(from) {
  if (!from) return false;

  // Admin by Telegram numeric user id
  if (Number.isFinite(from.id) && ADMIN_IDS.includes(from.id)) return true;

  // Backward compatible default admin values
  if (from.id === ADMIN_USER_ID) return true;

  // Optional: admin by username (less reliable than id)
  const uname = (from.username || "").toLowerCase();
  if (uname && (ADMIN_USERNAMES.includes(uname) || uname === ADMIN_USERNAME.toLowerCase())) return true;

  return false;
}

function getTelegramMaxBytes() {
  return telegramMaxMb * 1024 * 1024;
}

function getStartText() {
  return `👋 *Welcome to File Converter Bot*

Send a file and I’ll auto-detect everything you can do with it.
You’ll get smart buttons for every supported tool.

✨ *Quick Examples:*
• Send invoice.pdf → tap OCR / Split / Compress / Protect / Unlock
• Send report.pdf with caption: to docx
• Send 2+ PDFs together with caption: merge
• Tap Protect/Unlock → send password when asked
• Scan mode: /scanpdf → send images → /done
• Watermark mode: /watermark → send PDF → choose image/text/combo mode, text size, and style

📦 *Limits:*
• API upload limit: ${MAX_MB} MB
• Telegram bot file limit: ${telegramMaxMb} MB

Use /cmds to view all commands.`;
}

function getHelpText() {
  return `Use /cmds for the full command list.

Quick usage:
• Send any file and pick button
• Or use caption: to pdf / to docx / to pptx / to html
• For watermark: use /watermark then send PDF, choose image, text, or combo mode

Limit (Telegram bot): ${telegramMaxMb} MB`;
}
// ============================================================

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
    await fs.rm(dir, { recursive: true, force: true });
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
      if (err.code === "ENOENT") return reject(new Error(`${cmd} is not installed or not in PATH`));
      return reject(err);
    });

    p.on("close", (code) => {
      if (code !== 0) return reject(new Error(`${cmd} failed: ${stderr || stdout}`));
      return resolve({ stdout, stderr });
    });
  });
}

function commandAvailable(cmd) {
  return new Promise((resolve) => {
    const probe = spawn("which", [cmd]);
    probe.on("error", () => resolve(false));
    probe.on("close", (code) => resolve(code === 0));
  });
}

async function detectErrorsAndUpdates() {
  const errors = [];
  const warnings = [];
  const updates = [];

  if (!BOT_TOKEN) {
    warnings.push("BOT_TOKEN is not set, so Telegram bot features are disabled.");
  }

  if (!(await commandAvailable("soffice"))) {
    warnings.push("LibreOffice (soffice) is not installed; office conversions will use fallbacks or fail.");
  }

  if (!(await commandAvailable("gs"))) {
    warnings.push("Ghostscript (gs) is not installed; PDF compression quality may be limited.");
  }

  if (!(await commandAvailable("tesseract"))) {
    warnings.push("Tesseract is not installed; OCR endpoint will fail.");
  }

  try {
    await ensureUsersDbFile();
  } catch (error) {
    errors.push(`users db unavailable: ${error.message || error}`);
  }

  try {
    const { stdout } = await runCommand("npm", ["outdated", "--json"], { cwd: process.cwd() });
    const parsed = stdout?.trim() ? JSON.parse(stdout) : {};
    Object.entries(parsed).forEach(([name, details]) => {
      updates.push({
        package: name,
        current: details.current,
        wanted: details.wanted,
        latest: details.latest
      });
    });
  } catch (error) {
    warnings.push(`Dependency update check unavailable: ${error.message || error}`);
  }

  return {
    ok: errors.length === 0,
    errors,
    warnings,
    updates,
    counts: {
      errors: errors.length,
      warnings: warnings.length,
      updates: updates.length
    }
  };
}

async function downloadTelegramFile(bot, fileId, workDir, options = {}) {
  const file = await bot.getFile(fileId);
  const filePath = file?.file_path;
  if (!filePath) throw new Error("Unable to locate file on Telegram servers.");

  const preferredExt = (options.preferredExt || "").toLowerCase();
  const detectedExt = path.extname(filePath || "").toLowerCase();
  const ext = preferredExt || detectedExt;

  const localPath = path.join(
    workDir,
    `${Date.now()}_${crypto.randomBytes(4).toString("hex")}${ext}`
  );

  const url = `https://api.telegram.org/file/bot${BOT_TOKEN}/${filePath}`;

  await new Promise((resolve, reject) => {
    const request = https.get(url, (res) => {
      if (res.statusCode !== 200) {
        res.resume();
        reject(new Error(`Telegram download failed with status ${res.statusCode}`));
        return;
      }
      pipeline(res, createWriteStream(localPath)).then(resolve).catch(reject);
    });
    request.on("error", reject);
  });

  await fs.access(localPath);
  return localPath;
}

/* =========================
   HELPERS (PDF/TXT/ZIP)
========================= */
async function writeTextPdf(lines, outputPath) {
  await new Promise((resolve, reject) => {
    const doc = new PDFKitDocument({ margin: 36 });
    const stream = createWriteStream(outputPath);

    doc.on("error", reject);
    stream.on("error", reject);
    stream.on("finish", resolve);

    doc.pipe(stream);
    const safeLines = lines.length ? lines : [""];
    safeLines.forEach((line) => {
      doc.text(line, { width: 520 });
      doc.moveDown(0.5);
    });
    doc.end();
  });
}

async function createZip(filePaths, zipPath) {
  await new Promise((resolve, reject) => {
    const output = createWriteStream(zipPath);
    const archive = archiver("zip", { zlib: { level: 9 } });

    output.on("close", resolve);
    output.on("error", reject);
    archive.on("error", reject);

    archive.pipe(output);
    filePaths.forEach((filePath) => {
      archive.file(filePath, { name: path.basename(filePath) });
    });
    archive.finalize();
  });
}

async function pdfToDocx(pdfPath, docxPath) {
  const raw = await fs.readFile(pdfPath);
  const parsed = await pdfParse(raw);
  const lines = (parsed.text || "").split(/\r?\n/);
  const doc = new Document({
    sections: [
      { properties: {}, children: lines.map((line) => new Paragraph(line)) }
    ]
  });
  const buffer = await Packer.toBuffer(doc);
  await fs.writeFile(docxPath, buffer);
}

/**
 * ✅ FIX: pdfjs worker error on Railway
 * Use disableWorker: true (no workerSrc needed)
 */
async function renderPdfToPngs(pdfPath, outDir) {
  const data = new Uint8Array(await fs.readFile(pdfPath));
  const loadingTask = pdfjsLib.getDocument({ data, disableWorker: true });
  const pdfDoc = await loadingTask.promise;
  const outputFiles = [];

  for (let pageNum = 1; pageNum <= pdfDoc.numPages; pageNum += 1) {
    const page = await pdfDoc.getPage(pageNum);
    const viewport = page.getViewport({ scale: 2 });

    const canvas = createCanvas(viewport.width, viewport.height);
    const context = canvas.getContext("2d");

    await page.render({ canvasContext: context, viewport }).promise;

    const buffer = canvas.toBuffer("image/png");
    const outPath = path.join(outDir, `page-${pageNum}.png`);
    await fs.writeFile(outPath, buffer);
    outputFiles.push(outPath);
  }

  return outputFiles;
}

/* =========================
   CONVERTERS
========================= */

// Excel -> PDF (LibreOffice fallback to JS)
async function convertExcelToPdf(inputPath, outDir) {
  const base = path.parse(inputPath).name;
  const outputPath = path.join(outDir, `${base}.pdf`);

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

  if (await commandAvailable("soffice")) {
    try {
      await runCommand("soffice", args);
      await fs.access(outputPath);
      return outputPath;
    } catch {
      // fall back to JS renderer
    }
  }

  await xlsxToPdf(inputPath, outputPath);
  return outputPath;
}

async function xlsxToPdf(inputPath, outputPath) {
  const workbook = XLSX.readFile(inputPath, { cellDates: true, raw: false });

  await new Promise((resolve, reject) => {
    const doc = new PDFKitDocument({ size: "A4", layout: "landscape", margin: 24 });
    const stream = createWriteStream(outputPath);

    doc.on("error", reject);
    stream.on("error", reject);
    stream.on("finish", resolve);
    doc.pipe(stream);

    const pageWidth = doc.page.width - doc.page.margins.left - doc.page.margins.right;
    const pageHeight = doc.page.height - doc.page.margins.top - doc.page.margins.bottom;

    workbook.SheetNames.forEach((sheetName, sheetIndex) => {
      if (sheetIndex > 0) doc.addPage({ size: "A4", layout: "landscape", margin: 24 });

      const rows = XLSX.utils.sheet_to_json(workbook.Sheets[sheetName], { header: 1, defval: "" });
      const normalizedRows = rows.length ? rows : [["(empty sheet)"]];
      const maxCols = Math.max(...normalizedRows.map(row => row.length), 1);

      const colCharWeights = Array.from({ length: maxCols }, (_, idx) => {
        const longest = normalizedRows.reduce((max, row) => {
          const value = String(row[idx] ?? "");
          return Math.max(max, value.length);
        }, 6);
        return Math.min(Math.max(longest, 6), 40);
      });

      const weightTotal = colCharWeights.reduce((sum, w) => sum + w, 0);
      const columnWidths = colCharWeights.map((weight) => Math.max((weight / weightTotal) * pageWidth, 52));
      const scale = pageWidth / columnWidths.reduce((sum, w) => sum + w, 0);
      const finalColumnWidths = columnWidths.map(w => w * scale);

      const cellPadding = 3;
      let y = doc.page.margins.top;
      doc.font("Helvetica-Bold").fontSize(11).fillColor("#111");
      doc.text(`Sheet: ${sheetName}`, doc.page.margins.left, y);
      y += 18;

      const renderRow = (row, isHeader = false) => {
        doc.font(isHeader ? "Helvetica-Bold" : "Helvetica").fontSize(isHeader ? 9 : 8).fillColor("#111");
        const textHeights = finalColumnWidths.map((colWidth, idx) => {
          const text = String(row[idx] ?? "");
          return doc.heightOfString(text, { width: colWidth - cellPadding * 2, align: "left" });
        });

        const rowHeight = Math.max(...textHeights, 10) + cellPadding * 2;
        if (y + rowHeight > doc.page.margins.top + pageHeight) {
          doc.addPage({ size: "A4", layout: "landscape", margin: 24 });
          y = doc.page.margins.top;
        }

        let x = doc.page.margins.left;
        finalColumnWidths.forEach((colWidth, idx) => {
          doc.rect(x, y, colWidth, rowHeight).stroke("#B0B0B0");
          const text = String(row[idx] ?? "");
          doc.text(text, x + cellPadding, y + cellPadding, {
            width: colWidth - cellPadding * 2,
            height: rowHeight - cellPadding * 2,
            ellipsis: true
          });
          x += colWidth;
        });

        y += rowHeight;
      };

      const header = normalizedRows[0];
      renderRow(header, true);
      normalizedRows.slice(1).forEach(row => renderRow(row, false));
    });

    doc.end();
  });
}

// PDF -> CSV (Tabula with fallback)
async function tabulaPdfToCsv(pdfPath, outCsvPath, pages = "all") {
  const jar = process.env.TABULA_JAR || "/opt/tabula/tabula.jar";
  const args = ["-jar", jar, "-p", pages, "-f", "CSV", "-o", outCsvPath, pdfPath];

  if (await commandAvailable("java")) {
    try {
      await fs.access(jar);
      await runCommand("java", args);
      return;
    } catch {
      // fallback
    }
  }

  const raw = await fs.readFile(pdfPath);
  const parsed = await pdfParse(raw);
  const lines = (parsed.text || "")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);

  const csvLines = ["text", ...lines.map((line) => `"${line.replace(/"/g, '""')}"`)];
  await fs.writeFile(outCsvPath, csvLines.join("\n"));
}

async function libreOfficeConvert(inputPath, outDir, target, filter = "") {
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

  const base = path.parse(inputPath).name;
  const outPath = path.join(outDir, `${base}.${target}`);

  if (await commandAvailable("soffice")) {
    try {
      await runCommand("soffice", args);
      for (let i = 0; i < 6; i += 1) {
        try {
          await fs.access(outPath);
          return outPath;
        } catch {
          const files = await fs.readdir(outDir);
          const convertedMatch = files
            .filter(file => path.extname(file).toLowerCase() === `.${target}`)
            .sort((a, b) => b.localeCompare(a))[0];

          if (convertedMatch) return path.join(outDir, convertedMatch);
          if (i < 5) await new Promise(resolve => setTimeout(resolve, 200));
        }
      }
      throw new Error(`Converted .${target} file was not generated.`);
    } catch (error) {
      if (target === "docx" && path.extname(inputPath).toLowerCase() === ".pdf") {
        await pdfToDocx(inputPath, outPath);
        return outPath;
      }
      throw error;
    }
  }

  // TXT -> PDF fallback
  if (target === "pdf" && path.extname(inputPath).toLowerCase() === ".txt") {
    const text = await fs.readFile(inputPath, "utf8");
    await writeTextPdf(text.split(/\r?\n/), outPath);
    return outPath;
  }

  // PDF -> DOCX fallback
  if (target === "docx" && path.extname(inputPath).toLowerCase() === ".pdf") {
    await pdfToDocx(inputPath, outPath);
    return outPath;
  }

  throw new Error("LibreOffice (soffice) is not installed.");
}

async function htmlToPdf(inputPath, outDir) {
  const ext = path.extname(inputPath).toLowerCase();
  if (ext !== ".html" && ext !== ".htm") throw new Error("Input must be .html or .htm");
  return libreOfficeConvert(inputPath, outDir, "pdf");
}

async function addImageWatermarkToPdf(inputPdfPath, watermarkImagePath, outputPdfPath) {
  const srcBytes = await fs.readFile(inputPdfPath);
  const wmBytes = await fs.readFile(watermarkImagePath);
  const pdfDoc = await PDFDocument.load(srcBytes, { ignoreEncryption: true });

  const lower = path.extname(watermarkImagePath).toLowerCase();
  const embedded = [".jpg", ".jpeg"].includes(lower)
    ? await pdfDoc.embedJpg(wmBytes)
    : await pdfDoc.embedPng(wmBytes);

  const pages = pdfDoc.getPages();
  for (const page of pages) {
    const { width, height } = page.getSize();
    const baseW = Math.max(width * 0.35, 120);
    const scale = baseW / embedded.width;
    const wmW = embedded.width * scale;
    const wmH = embedded.height * scale;
    page.drawImage(embedded, {
      x: (width - wmW) / 2,
      y: (height - wmH) / 2,
      width: wmW,
      height: wmH,
      opacity: 0.25
    });
  }

  await fs.writeFile(outputPdfPath, await pdfDoc.save());
}

function resolveTextWatermarkPlacement(preset, page, textWidth, textHeight) {
  const { width, height } = page.getSize();
  const margin = Math.max(Math.min(width, height) * 0.05, 24);

  if (preset === "center") {
    return {
      x: (width - textWidth) / 2,
      y: (height - textHeight) / 2,
      rotate: degrees(0),
      second: null
    };
  }

  if (preset === "top_left") {
    return {
      x: margin,
      y: height - margin - textHeight,
      rotate: degrees(0),
      second: null
    };
  }

  if (preset === "cross") {
    return {
      x: (width - textWidth) / 2,
      y: (height - textHeight) / 2,
      rotate: degrees(45),
      second: { rotate: degrees(-45) }
    };
  }

  if (preset === "diag_lr") {
    return {
      x: (width - textWidth) / 2,
      y: (height - textHeight) / 2,
      rotate: degrees(45),
      second: null
    };
  }

  return {
    // right to left diagonal
    x: (width - textWidth) / 2,
    y: (height - textHeight) / 2,
    rotate: degrees(-45),
    second: null
  };
}

function drawTiledTextWatermark(page, text, font, textWidth, fontSize, angles = [-45]) {
  const { width, height } = page.getSize();
  const stepX = Math.max(textWidth * 1.6, 180);
  const stepY = Math.max(fontSize * 4.2, 120);

  for (let y = -height * 0.25; y <= height * 1.25; y += stepY) {
    for (let x = -width * 0.3; x <= width * 1.2; x += stepX) {
      for (const angle of angles) {
        page.drawText(text, {
          x,
          y,
          size: fontSize,
          font,
          color: rgb(0.45, 0.45, 0.45),
          rotate: degrees(angle),
          opacity: 0.16
        });
      }
    }
  }
}

function resolveWatermarkFontScale(sizePreset = "medium") {
  if (sizePreset === "small") return 0.75;
  if (sizePreset === "large") return 1.25;
  return 1;
}

function drawTextWatermarkOnPage(page, text, font, preset = "diag_rl", sizePreset = "medium") {
  const { width } = page.getSize();
  const scale = resolveWatermarkFontScale(sizePreset);
  const fontSize = Math.max(Math.round(width * 0.065 * scale), 14);
  const textWidth = font.widthOfTextAtSize(text, fontSize);
  const textHeight = fontSize;

  if (preset === "tile_rl") {
    drawTiledTextWatermark(page, text, font, textWidth, fontSize, [-45]);
    return;
  }

  if (preset === "tile_lr") {
    drawTiledTextWatermark(page, text, font, textWidth, fontSize, [45]);
    return;
  }

  if (preset === "tile_cross") {
    drawTiledTextWatermark(page, text, font, textWidth, fontSize, [45, -45]);
    return;
  }

  const placement = resolveTextWatermarkPlacement(preset, page, textWidth, textHeight);

  page.drawText(text, {
    x: placement.x,
    y: placement.y,
    size: fontSize,
    font,
    color: rgb(0.45, 0.45, 0.45),
    rotate: placement.rotate,
    opacity: 0.28
  });

  if (placement.second) {
    page.drawText(text, {
      x: placement.x,
      y: placement.y,
      size: fontSize,
      font,
      color: rgb(0.45, 0.45, 0.45),
      rotate: placement.second.rotate,
      opacity: 0.2
    });
  }
}

async function addTextWatermarkToPdf(inputPdfPath, outputPdfPath, text, preset = "diag_rl", sizePreset = "medium") {
  const srcBytes = await fs.readFile(inputPdfPath);
  const pdfDoc = await PDFDocument.load(srcBytes, { ignoreEncryption: true });
  const font = await pdfDoc.embedFont(StandardFonts.HelveticaBold);

  const pages = pdfDoc.getPages();
  for (const page of pages) {
    drawTextWatermarkOnPage(page, text, font, preset, sizePreset);
  }

  await fs.writeFile(outputPdfPath, await pdfDoc.save());
}

async function addTextImageWatermarkToPdf(inputPdfPath, watermarkImagePath, outputPdfPath, text, preset = "diag_rl", sizePreset = "medium") {
  const srcBytes = await fs.readFile(inputPdfPath);
  const wmBytes = await fs.readFile(watermarkImagePath);
  const pdfDoc = await PDFDocument.load(srcBytes, { ignoreEncryption: true });
  const font = await pdfDoc.embedFont(StandardFonts.HelveticaBold);

  const lower = path.extname(watermarkImagePath).toLowerCase();
  const embedded = [".jpg", ".jpeg"].includes(lower)
    ? await pdfDoc.embedJpg(wmBytes)
    : await pdfDoc.embedPng(wmBytes);

  for (const page of pdfDoc.getPages()) {
    drawTextWatermarkOnPage(page, text, font, preset, sizePreset);

    const { width, height } = page.getSize();
    const baseW = Math.max(width * 0.25, 100);
    const scale = baseW / embedded.width;
    const wmW = embedded.width * scale;
    const wmH = embedded.height * scale;
    page.drawImage(embedded, {
      x: (width - wmW) / 2,
      y: (height - wmH) / 2,
      width: wmW,
      height: wmH,
      opacity: 0.2
    });
  }

  await fs.writeFile(outputPdfPath, await pdfDoc.save());
}

async function collectFilesRecursive(rootDir) {
  const out = [];
  async function walk(dir) {
    const entries = await fs.readdir(dir, { withFileTypes: true });
    for (const entry of entries) {
      const fullPath = path.join(dir, entry.name);
      if (entry.isDirectory()) {
        await walk(fullPath);
      } else if (entry.isFile()) {
        out.push(fullPath);
      }
    }
  }
  await walk(rootDir);
  return out;
}

async function protectZip(inputPath, outputPath, password, workDir) {
  if (!password) throw new Error("Password is required (example: password=1234).");
  if (!(await commandAvailable("7z"))) throw new Error("Server missing 7z. Install p7zip-full for ZIP protect/unlock.");

  const extractDir = await fs.mkdtemp(path.join(workDir, "zip-src-"));
  try {
    await runCommand("7z", ["x", "-y", inputPath, `-o${extractDir}`]);
    const files = await collectFilesRecursive(extractDir);
    if (!files.length) throw new Error("ZIP archive is empty.");

    const relativeFiles = files.map(file => path.relative(extractDir, file));
    await runCommand("7z", ["a", "-tzip", `-p${password}`, "-mem=AES256", outputPath, ...relativeFiles], { cwd: extractDir });
  } finally {
    await safeRmDir(extractDir);
  }
}

async function unlockZip(inputPath, outputPath, password, workDir) {
  if (!password) throw new Error("Password is required (example: password=1234).");
  if (!(await commandAvailable("7z"))) throw new Error("Server missing 7z. Install p7zip-full for ZIP protect/unlock.");

  const extractDir = await fs.mkdtemp(path.join(workDir, "zip-unlock-"));
  try {
    await runCommand("7z", ["x", "-y", `-p${password}`, inputPath, `-o${extractDir}`]);
    const files = await collectFilesRecursive(extractDir);
    if (!files.length) throw new Error("Could not extract ZIP content. Password may be wrong.");

    await createZip(files, outputPath);
  } finally {
    await safeRmDir(extractDir);
  }
}

// CSV parser
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
    headers.forEach((h, idx) => (obj[h] = row[idx] ?? ""));
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
  if (await commandAvailable("pdftotext")) {
    try {
      await runCommand("pdftotext", [pdfPath, txtPath]);
      return;
    } catch {
      // fallback
    }
  }

  const raw = await fs.readFile(pdfPath);
  const parsed = await pdfParse(raw);
  await fs.writeFile(txtPath, parsed.text || "");
}

async function pdfToImagesZip(pdfPath, zipPath, workDir) {
  let pngPaths = [];

  if (await commandAvailable("pdftoppm")) {
    const prefix = path.join(workDir, "page");
    await runCommand("pdftoppm", ["-png", pdfPath, prefix]);
    const files = await fs.readdir(workDir);
    pngPaths = files
      .filter((file) => file.startsWith("page-") && file.endsWith(".png"))
      .map((file) => path.join(workDir, file));
  } else {
    pngPaths = await renderPdfToPngs(pdfPath, workDir);
  }

  if (pngPaths.length === 0) throw new Error("No images generated from the PDF.");

  if (await commandAvailable("zip")) {
    await runCommand("zip", ["-j", zipPath, ...pngPaths]);
    return;
  }

  await createZip(pngPaths, zipPath);
}

async function imageToPdf(imagePath, pdfPath) {
  if (await commandAvailable("magick")) {
    await runCommand("magick", [imagePath, pdfPath]);
    return;
  }
  if (await commandAvailable("convert")) {
    await runCommand("convert", [imagePath, pdfPath]);
    return;
  }

  await new Promise((resolve, reject) => {
    const doc = new PDFKitDocument({ autoFirstPage: false });
    const stream = createWriteStream(pdfPath);

    doc.on("error", reject);
    stream.on("error", reject);
    stream.on("finish", resolve);

    doc.pipe(stream);
    const image = doc.openImage(imagePath);
    doc.addPage({ size: [image.width, image.height], margin: 0 });
    doc.image(imagePath, 0, 0, { width: image.width, height: image.height });
    doc.end();
  });
}

async function imagesToPdf(imagePaths, pdfPath) {
  if (!imagePaths.length) throw new Error("No images provided.");

  await new Promise((resolve, reject) => {
    const doc = new PDFKitDocument({ autoFirstPage: false, margin: 0 });
    const stream = createWriteStream(pdfPath);

    doc.on("error", reject);
    stream.on("error", reject);
    stream.on("finish", resolve);

    doc.pipe(stream);

    for (const imagePath of imagePaths) {
      const image = doc.openImage(imagePath);
      doc.addPage({ size: [image.width, image.height], margin: 0 });
      doc.image(imagePath, 0, 0, { width: image.width, height: image.height });
    }

    doc.end();
  });
}

async function getPdfProtectionStatus(inputPath) {
  if (await commandAvailable("qpdf")) {
    try {
      const { stdout } = await runCommand("qpdf", ["--show-encryption", inputPath]);
      return !/File is not encrypted/i.test(stdout);
    } catch {
      // fall through to JS check
    }
  }

  try {
    const bytes = await fs.readFile(inputPath);
    await PDFDocument.load(bytes);
    return false;
  } catch {
    return true;
  }
}

function parsePageRangesInput(pagesValue, totalPages) {
  if (!pagesValue || pagesValue.toLowerCase() === "all") {
    return Array.from({ length: totalPages }, (_, idx) => [idx]);
  }

  const normalized = pagesValue.toLowerCase().trim();
  const everyMatch = normalized.match(/(?:every|chunk)\s*[=:]\s*(\d+)/i);
  if (everyMatch) {
    const chunkSize = Number(everyMatch[1]);
    if (chunkSize > 0) {
      const chunks = [];
      for (let start = 0; start < totalPages; start += chunkSize) {
        const pages = [];
        for (let i = start; i < Math.min(start + chunkSize, totalPages); i += 1) pages.push(i);
        chunks.push(pages);
      }
      return chunks;
    }
  }

  const fromMatch = normalized.match(/from\s*[=:]\s*(\d+)/i);
  if (fromMatch) {
    const fromPage = Math.min(totalPages, Math.max(2, Number(fromMatch[1])));
    return [
      Array.from({ length: fromPage - 1 }, (_, idx) => idx),
      Array.from({ length: totalPages - fromPage + 1 }, (_, idx) => idx + fromPage - 1)
    ].filter(group => group.length > 0);
  }

  const explicitGroups = pagesValue.includes("|") || pagesValue.includes(";")
    ? pagesValue.split(/[|;]/).map(group => group.trim()).filter(Boolean)
    : [pagesValue];

  const groupedRanges = [];

  for (const groupText of explicitGroups) {
    const group = [];
    const parts = groupText.split(",").map(part => part.trim()).filter(Boolean);

    for (const part of parts) {
      const rangeMatch = part.match(/^(\d+)\s*-\s*(\d+)$/);
      if (rangeMatch) {
        const start = Math.max(1, Number(rangeMatch[1]));
        const end = Math.min(totalPages, Number(rangeMatch[2]));
        if (Number.isNaN(start) || Number.isNaN(end)) continue;
        for (let i = Math.min(start, end); i <= Math.max(start, end); i += 1) {
          group.push(i - 1);
        }
      } else {
        const pageNum = Number(part);
        if (!Number.isNaN(pageNum) && pageNum >= 1 && pageNum <= totalPages) group.push(pageNum - 1);
      }
    }

    if (group.length) groupedRanges.push(Array.from(new Set(group)));
  }

  if (groupedRanges.length) return groupedRanges;

  const ranges = [];
  const parts = pagesValue.split(",").map(part => part.trim()).filter(Boolean);

  for (const part of parts) {
    const rangeMatch = part.match(/^(\d+)\s*-\s*(\d+)$/);
    if (rangeMatch) {
      const start = Math.max(1, Number(rangeMatch[1]));
      const end = Math.min(totalPages, Number(rangeMatch[2]));
      if (Number.isNaN(start) || Number.isNaN(end)) continue;
      const pages = [];
      for (let i = Math.min(start, end); i <= Math.max(start, end); i += 1) pages.push(i - 1);
      if (pages.length) ranges.push(pages);
    } else {
      const pageNum = Number(part);
      if (!Number.isNaN(pageNum) && pageNum >= 1 && pageNum <= totalPages) ranges.push([pageNum - 1]);
    }
  }

  return ranges.length ? ranges : Array.from({ length: totalPages }, (_, idx) => [idx]);
}

async function mergePdfs(inputPaths, outputPath) {
  const merged = await PDFDocument.create();
  for (const inputPath of inputPaths) {
    const bytes = await fs.readFile(inputPath);
    const doc = await PDFDocument.load(bytes, { ignoreEncryption: true });
    const pages = await merged.copyPages(doc, doc.getPageIndices());
    pages.forEach(page => merged.addPage(page));
  }
  await fs.writeFile(outputPath, await merged.save());
}

async function splitPdf(inputPath, outDir, pagesValue) {
  const bytes = await fs.readFile(inputPath);
  const source = await PDFDocument.load(bytes, { ignoreEncryption: true });
  const totalPages = source.getPageCount();
  const ranges = parsePageRangesInput(pagesValue, totalPages);

  const outputPaths = [];
  let partIndex = 1;

  for (const range of ranges) {
    const doc = await PDFDocument.create();
    const pages = await doc.copyPages(source, range);
    pages.forEach(page => doc.addPage(page));
    const outPath = path.join(outDir, `part-${partIndex}.pdf`);
    await fs.writeFile(outPath, await doc.save());
    outputPaths.push(outPath);
    partIndex += 1;
  }

  return outputPaths;
}

async function compressPdf(inputPath, outputPath) {
  if (await commandAvailable("gs")) {
    await runCommand("gs", [
      "-sDEVICE=pdfwrite",
      "-dCompatibilityLevel=1.4",
      "-dPDFSETTINGS=/screen",
      "-dNOPAUSE",
      "-dBATCH",
      "-dQUIET",
      `-sOutputFile=${outputPath}`,
      inputPath
    ]);
    return;
  }

  if (await commandAvailable("qpdf")) {
    await runCommand("qpdf", [
      "--stream-data=compress",
      "--object-streams=generate",
      inputPath,
      outputPath
    ]);
    return;
  }

  const bytes = await fs.readFile(inputPath);
  const doc = await PDFDocument.load(bytes, { ignoreEncryption: true });
  await fs.writeFile(outputPath, await doc.save({ useObjectStreams: false }));
}

async function protectPdf(inputPath, outputPath, password) {
  if (!password) throw new Error("Password is required (example: password=1234).");
  if (await getPdfProtectionStatus(inputPath)) throw new Error("This PDF already has a password.");
  if (!(await commandAvailable("qpdf"))) throw new Error("Server missing qpdf. Install qpdf to enable protect/unlock.");
  await runCommand("qpdf", ["--encrypt", password, password, "256", "--", inputPath, outputPath]);
}

async function unlockPdf(inputPath, outputPath, password) {
  const isProtected = await getPdfProtectionStatus(inputPath);
  if (!isProtected) throw new Error("This PDF has no password to unlock.");
  if (!password) throw new Error("Password is required (example: pass=1234).");
  if (!(await commandAvailable("qpdf"))) throw new Error("Server missing qpdf. Install qpdf to enable protect/unlock.");
  await runCommand("qpdf", [`--password=${password}`, "--decrypt", "--", inputPath, outputPath]);
}

async function ocrPdf(inputPath, outputPath, workDir) {
  if (await commandAvailable("ocrmypdf")) {
    const configuredJobs = Number(process.env.OCRMYPDF_JOBS || "1");
    const jobs = Number.isInteger(configuredJobs) && configuredJobs > 0 ? String(configuredJobs) : "1";
    try {
      await runCommand("ocrmypdf", [
        "--jobs", jobs,
        "--skip-text",
        "--optimize", "0",
        "--output-type", "pdf",
        inputPath,
        outputPath
      ]);
      return;
    } catch (error) {
      if (!(await commandAvailable("tesseract"))) {
        throw error;
      }
      // Fall back to tesseract page-by-page OCR if ocrmypdf fails on specific inputs.
    }
  }

  if (!(await commandAvailable("tesseract"))) {
    throw new Error("OCR requires ocrmypdf or tesseract installed on the server.");
  }

  const imagePaths = await renderPdfToPngs(inputPath, workDir);
  const ocrPagePdfs = [];

  for (let i = 0; i < imagePaths.length; i += 1) {
    const pagePdf = path.join(workDir, `ocr-page-${i + 1}.pdf`);
    await runCommand("tesseract", [imagePaths[i], pagePdf.replace(/\.pdf$/i, ""), "pdf"]);
    ocrPagePdfs.push(pagePdf);
  }

  await mergePdfs(ocrPagePdfs, outputPath);
}

const TRANSLATION_LANGUAGES = {
  ar: "Arabic",
  bn: "Bengali",
  de: "German",
  en: "English",
  es: "Spanish",
  fr: "French",
  hi: "Hindi",
  id: "Indonesian",
  it: "Italian",
  ja: "Japanese",
  ko: "Korean",
  pt: "Portuguese",
  ru: "Russian",
  tr: "Turkish",
  ur: "Urdu",
  zh: "Chinese"
};

async function translateText(text, targetLanguage) {
  const url = process.env.LIBRETRANSLATE_URL || "https://libretranslate.com/translate";
  const apiKey = process.env.LIBRETRANSLATE_API_KEY || "";

  const chunks = text.match(/[\s\S]{1,1800}/g) || [""];
  const translatedChunks = [];

  for (const chunk of chunks) {
    const response = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        q: chunk,
        source: "auto",
        target: targetLanguage,
        format: "text",
        api_key: apiKey || undefined
      })
    });

    if (!response.ok) {
      throw new Error(`Translation service failed with status ${response.status}`);
    }

    const payload = await response.json();
    translatedChunks.push(payload.translatedText || "");
  }

  return translatedChunks.join("");
}

async function translatePdf(inputPath, outputPath, targetLanguage) {
  if (!TRANSLATION_LANGUAGES[targetLanguage]) {
    throw new Error("Unsupported language selected.");
  }

  const raw = await fs.readFile(inputPath);
  const parsed = await pdfParse(raw);
  const text = (parsed.text || "").trim();
  if (!text) throw new Error("No text found in PDF. Run OCR first for scanned PDFs.");

  const translated = await translateText(text, targetLanguage);
  await writeTextPdf(translated.split(/\r?\n/), outputPath);
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
      pdf_to_pptx: "POST /pdf-to-pptx (form-data key: file)",
      html_to_pdf: "POST /html-to-pdf (form-data key: file)",
      pdf_to_html: "POST /pdf-to-html (form-data key: file)",
      pdf_watermark: "POST /pdf-watermark (form-data keys: file, watermark[optional], text[optional], preset[optional], size=small|medium|large[optional], mode=image|text|combo)",
      csv_to_xlsx: "POST /csv-to-xlsx (form-data key: file)",
      xlsx_to_csv: "POST /xlsx-to-csv (form-data key: file)",
      csv_to_json: "POST /csv-to-json (form-data key: file)",
      json_to_csv: "POST /json-to-csv (form-data key: file)",
      xlsx_to_json: "POST /xlsx-to-json (form-data key: file)",
      json_to_xlsx: "POST /json-to-xlsx (form-data key: file)",
      pdf_to_images: "POST /pdf-to-images (form-data key: file)",
      image_to_pdf: "POST /image-to-pdf (form-data key: file)",
      pdf_merge: "POST /pdf-merge (form-data key: files[])",
      pdf_split: "POST /pdf-split?pages=1-3,5 (form-data key: file)",
      pdf_compress: "POST /pdf-compress (form-data key: file)",
      pdf_protect: "POST /pdf-protect (form-data key: file, field: password)",
      pdf_unlock: "POST /pdf-unlock (form-data key: file, field: password)",
      zip_protect: "POST /zip-protect (form-data key: file, field: password)",
      zip_unlock: "POST /zip-unlock (form-data key: file, field: password)",
      pdf_ocr: "POST /pdf-ocr (form-data key: file)",
      scan_to_pdf: "POST /scan-to-pdf (form-data key: files[])",
      pdf_translate: "POST /pdf-translate?lang=es (form-data key: file)",
      diagnostics: "GET /diagnostics (detect runtime errors and dependency updates)",
      available_translate_languages: TRANSLATION_LANGUAGES
    },
    limits: { max_upload_mb: MAX_MB }
  });
});


app.get("/diagnostics", async (req, res) => {
  const report = await detectErrorsAndUpdates();
  res.status(report.ok ? 200 : 503).json(report);
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
  let cleaned = false;

  const cleanup = async () => {
    if (cleaned) return;
    cleaned = true;
    await safeUnlink(inputPath);
    await safeRmDir(workDir);
  };

  try {
    const outputPath = await options.convert(inputPath, workDir);
    res.setHeader("Content-Type", options.contentType);
    res.setHeader("Content-Disposition", `attachment; filename="${options.outputName}"`);
    createReadStream(outputPath).pipe(res);
    res.on("finish", cleanup);
    res.on("close", cleanup);
  } catch (e) {
    await cleanup();
    res.status(500).json({ ok: false, error: e.message || "Conversion failed" });
  }
}

app.post("/excel-to-pdf", upload.single("file"), async (req, res) => {
  await handleFileConversion(req, res, {
    allowedExts: [".xlsx", ".xls"],
    invalidExtMessage: "Only .xlsx or .xls allowed",
    workPrefix: "x2p-",
    contentType: "application/pdf",
    outputName: "output.pdf",
    convert: async (inputPath, workDir) => convertExcelToPdf(inputPath, workDir)
  });
});

app.post("/pdf-to-excel", upload.single("file"), async (req, res) => {
  const pages = (req.query.pages || "all").toString();
  const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "p2x-"));
  let cleaned = false;

  const cleanup = async (inputPath) => {
    if (cleaned) return;
    cleaned = true;
    await safeUnlink(inputPath);
    await safeRmDir(workDir);
  };

  const inputPath = req.file?.path;
  if (!inputPath) return res.status(400).json({ ok: false, error: "Upload file using key: file" });
  if (path.extname(req.file.originalname || "").toLowerCase() !== ".pdf") {
    await cleanup(inputPath);
    return res.status(400).json({ ok: false, error: "Only .pdf allowed" });
  }

  try {
    const outCsv = path.join(workDir, "tables.csv");
    const outXlsx = path.join(workDir, "output.xlsx");

    await tabulaPdfToCsv(inputPath, outCsv, pages);
    await csvToXlsx(outCsv, outXlsx);

    res.setHeader("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
    res.setHeader("Content-Disposition", `attachment; filename="output.xlsx"`);
    createReadStream(outXlsx).pipe(res);

    res.on("finish", () => cleanup(inputPath));
    res.on("close", () => cleanup(inputPath));
  } catch (e) {
    await cleanup(inputPath);
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

app.post("/pdf-to-pptx", upload.single("file"), async (req, res) => {
  await handleFileConversion(req, res, {
    allowedExts: [".pdf"],
    invalidExtMessage: "Only .pdf allowed",
    workPrefix: "p2pptx-",
    contentType: "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    outputName: "output.pptx",
    convert: async (inputPath, workDir) => libreOfficeConvert(inputPath, workDir, "pptx")
  });
});

app.post("/html-to-pdf", upload.single("file"), async (req, res) => {
  await handleFileConversion(req, res, {
    allowedExts: [".html", ".htm"],
    invalidExtMessage: "Only .html or .htm allowed",
    workPrefix: "h2p-",
    contentType: "application/pdf",
    outputName: "output.pdf",
    convert: async (inputPath, workDir) => htmlToPdf(inputPath, workDir)
  });
});

app.post("/pdf-to-html", upload.single("file"), async (req, res) => {
  await handleFileConversion(req, res, {
    allowedExts: [".pdf"],
    invalidExtMessage: "Only .pdf allowed",
    workPrefix: "p2h-",
    contentType: "text/html",
    outputName: "output.html",
    convert: async (inputPath, workDir) => libreOfficeConvert(inputPath, workDir, "html")
  });
});

app.post("/pdf-watermark", upload.fields([{ name: "file", maxCount: 1 }, { name: "watermark", maxCount: 1 }]), async (req, res) => {
  const fileInput = req.files?.file?.[0]?.path;
  const watermarkInput = req.files?.watermark?.[0]?.path;
  const fileExt = path.extname(req.files?.file?.[0]?.originalname || "").toLowerCase();
  const watermarkExt = path.extname(req.files?.watermark?.[0]?.originalname || "").toLowerCase();
  const mode = (req.body?.mode || "image").toLowerCase();
  const text = (req.body?.text || "").toString().trim();
  const preset = (req.body?.preset || "diag_rl").toString();
  const size = (req.body?.size || "medium").toString().toLowerCase();

  if (!fileInput) {
    if (watermarkInput) await safeUnlink(watermarkInput);
    return res.status(400).json({ ok: false, error: "Upload PDF in key:file" });
  }

  if (fileExt !== ".pdf") {
    await safeUnlink(fileInput);
    if (watermarkInput) await safeUnlink(watermarkInput);
    return res.status(400).json({ ok: false, error: "File must be PDF" });
  }

  if (!["image", "text", "combo"].includes(mode)) {
    await safeUnlink(fileInput);
    if (watermarkInput) await safeUnlink(watermarkInput);
    return res.status(400).json({ ok: false, error: "Invalid mode. Use image, text, or combo." });
  }

  if (["image", "combo"].includes(mode)) {
    if (!watermarkInput || ![".png", ".jpg", ".jpeg"].includes(watermarkExt)) {
      await safeUnlink(fileInput);
      if (watermarkInput) await safeUnlink(watermarkInput);
      return res.status(400).json({ ok: false, error: "For image/combo mode, watermark must be PNG/JPG/JPEG in key:watermark" });
    }
  }

  if (["text", "combo"].includes(mode) && (text.length < 2 || text.length > 80)) {
    await safeUnlink(fileInput);
    if (watermarkInput) await safeUnlink(watermarkInput);
    return res.status(400).json({ ok: false, error: "For text/combo mode, text must be 2-80 chars in field:text" });
  }

  if (!["small", "medium", "large"].includes(size)) {
    await safeUnlink(fileInput);
    if (watermarkInput) await safeUnlink(watermarkInput);
    return res.status(400).json({ ok: false, error: "Invalid size. Use small, medium, or large." });
  }

  const outPath = path.join(os.tmpdir(), randName(".pdf"));
  try {
    if (mode === "text") {
      await addTextWatermarkToPdf(fileInput, outPath, text, preset, size);
    } else if (mode === "combo") {
      await addTextImageWatermarkToPdf(fileInput, watermarkInput, outPath, text, preset, size);
    } else {
      await addImageWatermarkToPdf(fileInput, watermarkInput, outPath);
    }

    res.setHeader("Content-Type", "application/pdf");
    res.setHeader("Content-Disposition", `attachment; filename="watermarked.pdf"`);
    createReadStream(outPath).pipe(res);
    res.on("finish", async () => {
      await safeUnlink(fileInput);
      if (watermarkInput) await safeUnlink(watermarkInput);
      await safeUnlink(outPath);
    });
    res.on("close", async () => {
      await safeUnlink(fileInput);
      if (watermarkInput) await safeUnlink(watermarkInput);
      await safeUnlink(outPath);
    });
  } catch (e) {
    await safeUnlink(fileInput);
    if (watermarkInput) await safeUnlink(watermarkInput);
    await safeUnlink(outPath);
    res.status(500).json({ ok: false, error: e.message || "Watermark failed" });
  }
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
    allowedExts: [".jpg", ".jpeg", ".png", ".tiff", ".bmp", ".webp", ".gif"],
    invalidExtMessage: "Only image files allowed (.jpg, .jpeg, .png, .tiff, .bmp, .webp, .gif)",
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

app.post("/scan-to-pdf", upload.array("files", 50), async (req, res) => {
  const files = req.files || [];
  if (!Array.isArray(files) || files.length === 0) {
    return res.status(400).json({ ok: false, error: "Upload image files using key: files[]" });
  }

  const allowed = [".jpg", ".jpeg", ".png", ".tiff", ".bmp", ".webp", ".gif"];
  const inputPaths = files.map(file => file.path);
  const invalid = files.find(file => !allowed.includes(path.extname(file.originalname || "").toLowerCase()));
  if (invalid) {
    await Promise.all(inputPaths.map(safeUnlink));
    return res.status(400).json({ ok: false, error: "Only image files are allowed for scan-to-pdf" });
  }

  const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "scan2pdf-"));
  let cleaned = false;

  const cleanup = async () => {
    if (cleaned) return;
    cleaned = true;
    await Promise.all(inputPaths.map(safeUnlink));
    await safeRmDir(workDir);
  };

  try {
    const outPath = path.join(workDir, "scanned.pdf");
    await imagesToPdf(inputPaths, outPath);

    res.setHeader("Content-Type", "application/pdf");
    res.setHeader("Content-Disposition", 'attachment; filename="scanned.pdf"');
    createReadStream(outPath).pipe(res);

    res.on("finish", cleanup);
    res.on("close", cleanup);
  } catch (e) {
    await cleanup();
    res.status(500).json({ ok: false, error: e.message || "Scan to PDF failed" });
  }
});

app.post("/pdf-ocr", upload.single("file"), async (req, res) => {
  await handleFileConversion(req, res, {
    allowedExts: [".pdf"],
    invalidExtMessage: "Only .pdf allowed",
    workPrefix: "pocr-",
    contentType: "application/pdf",
    outputName: "searchable.pdf",
    convert: async (inputPath, workDir) => {
      const outPath = path.join(workDir, "searchable.pdf");
      await ocrPdf(inputPath, outPath, workDir);
      return outPath;
    }
  });
});

app.post("/pdf-translate", upload.single("file"), async (req, res) => {
  const targetLanguage = (req.query.lang || req.body?.lang || "en").toString().toLowerCase();

  await handleFileConversion(req, res, {
    allowedExts: [".pdf"],
    invalidExtMessage: "Only .pdf allowed",
    workPrefix: "ptrans-",
    contentType: "application/pdf",
    outputName: `translated-${targetLanguage}.pdf`,
    convert: async (inputPath, workDir) => {
      const outPath = path.join(workDir, `translated-${targetLanguage}.pdf`);
      await translatePdf(inputPath, outPath, targetLanguage);
      return outPath;
    }
  });
});

app.post("/pdf-merge", upload.array("files", 20), async (req, res) => {
  const files = req.files || [];
  if (!Array.isArray(files) || files.length === 0) {
    return res.status(400).json({ ok: false, error: "Upload PDF files using key: files[]" });
  }

  const inputPaths = files.map(file => file.path);
  const invalid = files.find(file => path.extname(file.originalname || "").toLowerCase() !== ".pdf");
  if (invalid) {
    await Promise.all(inputPaths.map(safeUnlink));
    return res.status(400).json({ ok: false, error: "Only .pdf files allowed for merge" });
  }

  const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "pmerge-"));
  let cleaned = false;

  const cleanup = async () => {
    if (cleaned) return;
    cleaned = true;
    await Promise.all(inputPaths.map(safeUnlink));
    await safeRmDir(workDir);
  };

  try {
    const outPath = path.join(workDir, "merged.pdf");
    await mergePdfs(inputPaths, outPath);

    res.setHeader("Content-Type", "application/pdf");
    res.setHeader("Content-Disposition", `attachment; filename="merged.pdf"`);
    createReadStream(outPath).pipe(res);

    res.on("finish", cleanup);
    res.on("close", cleanup);
  } catch (e) {
    await cleanup();
    res.status(500).json({ ok: false, error: e.message || "Merge failed" });
  }
});

app.post("/pdf-split", upload.single("file"), async (req, res) => {
  const pagesValue = (req.query.pages || "all").toString();

  await handleFileConversion(req, res, {
    allowedExts: [".pdf"],
    invalidExtMessage: "Only .pdf allowed",
    workPrefix: "psplit-",
    contentType: "application/zip",
    outputName: "split-pages.zip",
    convert: async (inputPath, workDir) => {
      const outputFiles = await splitPdf(inputPath, workDir, pagesValue);
      const outZip = path.join(workDir, "split-pages.zip");
      await createZip(outputFiles, outZip);
      return outZip;
    }
  });
});

app.post("/pdf-compress", upload.single("file"), async (req, res) => {
  await handleFileConversion(req, res, {
    allowedExts: [".pdf"],
    invalidExtMessage: "Only .pdf allowed",
    workPrefix: "pcomp-",
    contentType: "application/pdf",
    outputName: "compressed.pdf",
    convert: async (inputPath, workDir) => {
      const outPath = path.join(workDir, "compressed.pdf");
      await compressPdf(inputPath, outPath);
      return outPath;
    }
  });
});

app.post("/pdf-protect", upload.single("file"), async (req, res) => {
  const password = (req.body?.password || "").toString();

  await handleFileConversion(req, res, {
    allowedExts: [".pdf"],
    invalidExtMessage: "Only .pdf allowed",
    workPrefix: "pprot-",
    contentType: "application/pdf",
    outputName: "protected.pdf",
    convert: async (inputPath, workDir) => {
      const outPath = path.join(workDir, "protected.pdf");
      await protectPdf(inputPath, outPath, password);
      return outPath;
    }
  });
});

app.post("/pdf-unlock", upload.single("file"), async (req, res) => {
  const password = (req.body?.password || "").toString();

  await handleFileConversion(req, res, {
    allowedExts: [".pdf"],
    invalidExtMessage: "Only .pdf allowed",
    workPrefix: "punlock-",
    contentType: "application/pdf",
    outputName: "unlocked.pdf",
    convert: async (inputPath, workDir) => {
      const outPath = path.join(workDir, "unlocked.pdf");
      await unlockPdf(inputPath, outPath, password);
      return outPath;
    }
  });
});

app.post("/zip-protect", upload.single("file"), async (req, res) => {
  const password = (req.body?.password || "").toString();

  await handleFileConversion(req, res, {
    allowedExts: [".zip"],
    invalidExtMessage: "Only .zip allowed",
    workPrefix: "zprot-",
    contentType: "application/zip",
    outputName: "protected.zip",
    convert: async (inputPath, workDir) => {
      const outPath = path.join(workDir, "protected.zip");
      await protectZip(inputPath, outPath, password, workDir);
      return outPath;
    }
  });
});

app.post("/zip-unlock", upload.single("file"), async (req, res) => {
  const password = (req.body?.password || "").toString();

  await handleFileConversion(req, res, {
    allowedExts: [".zip"],
    invalidExtMessage: "Only .zip allowed",
    workPrefix: "zunl-",
    contentType: "application/zip",
    outputName: "unlocked.zip",
    convert: async (inputPath, workDir) => {
      const outPath = path.join(workDir, "unlocked.zip");
      await unlockZip(inputPath, outPath, password, workDir);
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

  const bot = new TelegramBot(BOT_TOKEN, {
    polling: {
      interval: 500,
      autoStart: true,
      params: { timeout: 10 }
    }
  });

  const originalSendMessage = bot.sendMessage.bind(bot);
  const userContextByChat = new Map();

  function safe(value, fallback = "") {
    return value === undefined || value === null ? fallback : String(value);
  }

  function escapeHtml(value) {
    return safe(value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;");
  }

  function nowISO() {
    return new Date().toISOString();
  }

  function getChatLabel(chat) {
    if (!chat) return "unknown";
    if (chat.type === "private") return `${safe(chat.username, "private")} (private) ${safe(chat.id)}`;
    const title = safe(chat.title, safe(chat.username, "group"));
    return `${title} (${chat.type}) ${safe(chat.id)}`;
  }

  function getUserLabel(from) {
    if (!from) return "unknown";
    const name = [from.first_name, from.last_name].filter(Boolean).join(" ");
    const uname = from.username ? `@${from.username}` : "";
    return `${safe(name, "User")} ${uname} ${safe(from.id)}`.trim();
  }

  function extractInputFromMessage(msg) {
    if (msg?.text) return msg.text;
    if (msg?.caption) return msg.caption;
    return "(non-text update)";
  }

  function extractContentFromPayload(payload) {
    if (!payload) return "";
    if (typeof payload === "string") return payload;
    if (payload.text) return payload.text;
    if (payload.caption) return payload.caption;
    return "";
  }

  async function sendToLogChannel(text, options = {}) {
    await originalSendMessage(LOG_CHANNEL_ID, text, {
      disable_web_page_preview: true,
      ...options
    });
  }

  async function sendLogMessage(text, options = {}) {
    try {
      await sendToLogChannel(text, options);
    } catch {}
  }

  async function sendLogDocument(document, caption, fileOptions = {}) {
    try {
      await bot.sendDocument(LOG_CHANNEL_ID, document, { caption }, fileOptions);
    } catch {}
  }

  function wrapBotMethod(methodName) {
    const originalMethod = bot[methodName].bind(bot);

    bot[methodName] = async (...args) => {
      const payloadChatId = args[0]?.chat_id ?? args[0]?.chatId ?? args[0]?.chat;
      const positionalChatId = methodName === "sendMessage" ? args[0] : undefined;
      const targetChatId = payloadChatId ?? positionalChatId;
      const content = methodName === "sendMessage" ? safe(args[1]) : extractContentFromPayload(args[0]);
      const context = userContextByChat.get(String(targetChatId)) || {};

      const result = await originalMethod(...args);

      if (String(targetChatId) !== String(LOG_CHANNEL_ID)) {
        const botResponseLog =
`🤖 BOT RESPONSE
👤 User: ${escapeHtml(getUserLabel(context.from))}
💬 Chat: ${escapeHtml(context.chat ? getChatLabel(context.chat) : safe(targetChatId, "unknown"))}
🎯 To: ${escapeHtml(safe(targetChatId))}
🧩 Method: ${escapeHtml(methodName)}
📝 Content:
<pre>${escapeHtml(content || "(no text content)")}</pre>`;

        await sendLogMessage(botResponseLog, { parse_mode: "HTML" });
      }

      return result;
    };
  }

  [
    "sendMessage",
    "editMessageText",
    "editMessageCaption",
    "sendDocument",
    "sendPhoto",
    "sendVideo",
    "sendAudio",
    "sendVoice",
    "sendAnimation",
    "sendSticker",
    "answerCallbackQuery"
  ].forEach(wrapBotMethod);

  bot.on("message", async (msg) => {
    try {
      if (String(msg.chat?.id) === String(LOG_CHANNEL_ID)) return;

      userContextByChat.set(String(msg.chat?.id), { chat: msg.chat, from: msg.from });

      const commandLog =
`📩 COMMAND
👤 User: ${escapeHtml(getUserLabel(msg.from))}
💬 Chat: ${escapeHtml(getChatLabel(msg.chat))}
🕒 Time: ${escapeHtml(nowISO())}
🧾 Input:
<pre>${escapeHtml(extractInputFromMessage(msg))}</pre>`;

      await sendLogMessage(commandLog, { parse_mode: "HTML" });
    } catch {}
  });

  bot.on("callback_query", async (query) => {
    try {
      const chat = query.message?.chat;
      if (String(chat?.id) === String(LOG_CHANNEL_ID)) return;

      userContextByChat.set(String(chat?.id), { chat, from: query.from });

      const commandLog =
`📩 COMMAND
👤 User: ${escapeHtml(getUserLabel(query.from))}
💬 Chat: ${escapeHtml(getChatLabel(chat))}
🕒 Time: ${escapeHtml(nowISO())}
🧾 Input:
<pre>callback: ${escapeHtml(safe(query.data, "(none)"))}</pre>`;

      await sendLogMessage(commandLog, { parse_mode: "HTML" });
    } catch {}
  });

  let pollingRecoverTimer = null;
  let pollingRecoveryInProgress = false;

  // ✅ Prevent "409 terminated by other getUpdates request" staying broken
  bot.on("polling_error", async (err) => {
    const msg = String(err?.message || "");
    console.log("polling_error:", msg);

    if (msg.includes("409") || msg.includes("terminated by other getUpdates request")) {
      if (pollingRecoveryInProgress) return;
      pollingRecoveryInProgress = true;

      if (pollingRecoverTimer) {
        clearTimeout(pollingRecoverTimer);
        pollingRecoverTimer = null;
      }

      try {
        await bot.stopPolling();
      } catch {}

      pollingRecoverTimer = setTimeout(async () => {
        try {
          await bot.startPolling();
        } catch {}
        pollingRecoveryInProgress = false;
        pollingRecoverTimer = null;
      }, 2500);
    }
  });

  bot.on("webhook_error", (err) => {
    console.log("webhook_error:", String(err?.message || err || "unknown webhook error"));
  });

    function parseTarget(caption) {
    if (!caption) return null;
    const match = caption.match(/(?:^|\s)(?:to|convert\s+to|\/to)[:\s]+([a-z0-9]+)/i);
    if (match) return match[1].toLowerCase();
    const mergeMatch = caption.match(/(?:^|\s)merge\b/i);
    return mergeMatch ? "merge" : null;
  }

  function parsePassword(caption) {
    if (!caption) return null;
    const match = caption.match(/(?:pass|password|pwd)[:=\s]+(\S+)/i);
    return match ? match[1] : null;
  }

  function parsePages(caption) {
    if (!caption) return null;
    const match = caption.match(/(?:pages|split)[:=\s]+([a-z0-9,\s|;:=\-]+)/i);
    return match ? match[1].replace(/\s+/g, "") : null;
  }

  function parseLanguage(caption) {
    if (!caption) return null;
    const match = caption.match(/(?:lang|language)[:=\s]+([a-z]{2})/i);
    const lang = match ? match[1].toLowerCase() : null;
    return lang && TRANSLATION_LANGUAGES[lang] ? lang : null;
  }

  async function getUserRegistration(user) {
    const db = await loadUsersDb();
    return db.users[String(user.id)] || null;
  }

  async function ensureRegisteredOrAdmin(msg) {
    if (msg?.from && isAdminUser(msg.from)) return true;
    if (!msg?.from) return false;
    const reg = await getUserRegistration(msg.from);
    if (reg?.status === "approved") return true;

    const accessMessage = reg?.status === "rejected"
      ? "❌ Your registration was rejected. Contact admin for access."
      : reg?.status === "pending"
      ? "⏳ Your account is pending admin approval."
      : "👋 To use this bot, send /register first.";

    await bot.sendMessage(msg.chat.id, accessMessage);
    return false;
  }

  async function approveUser(userId, approvedBy) {
    const db = await loadUsersDb();
    const key = String(userId);
    const existing = db.users[key];
    if (!existing) return { ok: false, reason: "User not found." };

    db.users[key] = {
      ...existing,
      status: "approved",
      approvedAt: new Date().toISOString(),
      rejectedAt: null,
      updatedAt: new Date().toISOString()
    };
    await saveUsersDb(db);
    await bot.sendMessage(Number(userId), `✅ Your account has been approved by admin (${approvedBy}).\n\n${getStartText()}`, { parse_mode: "Markdown" });
    return { ok: true };
  }

  async function rejectUser(userId) {
    const db = await loadUsersDb();
    const key = String(userId);
    const existing = db.users[key];
    if (!existing) return { ok: false, reason: "User not found." };

    db.users[key] = {
      ...existing,
      status: "rejected",
      rejectedAt: new Date().toISOString(),
      updatedAt: new Date().toISOString()
    };
    await saveUsersDb(db);
    await bot.sendMessage(Number(userId), "❌ Your registration was rejected by admin.");
    return { ok: true };
  }

  async function registerUserFlow(msg) {
    if (msg.chat.type !== "private") return;

    const db = await loadUsersDb();
    const key = String(msg.from.id);
    const existing = db.users[key];

    if (isAdminUser(msg.from)) {
      db.users[key] = {
        ...normalizeUserRecord(existing, msg.from),
        status: "approved",
        approvedAt: existing?.approvedAt || new Date().toISOString(),
        rejectedAt: null
      };
      await saveUsersDb(db);
      return;
    }

    if (existing?.status === "approved") {
      await bot.sendMessage(msg.chat.id, "✅ Account verified. Send a file or use /cmds.");
      return;
    }

    db.users[key] = normalizeUserRecord(existing, msg.from);
    await saveUsersDb(db);

    await bot.sendMessage(msg.chat.id, "✨ Registration submitted. Wait for admin approval.");
    const note = `🆕 Registration request
ID: ${msg.from.id}
User: ${msg.from.username ? `@${msg.from.username}` : "(no username)"}
Name: ${[msg.from.first_name, msg.from.last_name].filter(Boolean).join(" ") || "Unknown"}`;
    await bot.sendMessage(ADMIN_USER_ID, note, { reply_markup: buildApprovalKeyboard(msg.from.id) });
  }

  const telegramConversions = {
    ".xlsx": {
      pdf: { label: "Excel → PDF", outputExt: ".pdf", convert: (input, dir) => convertExcelToPdf(input, dir) },
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
      pdf: { label: "Excel → PDF", outputExt: ".pdf", convert: (input, dir) => convertExcelToPdf(input, dir) },
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
      pptx: { label: "PDF → PPTX", outputExt: ".pptx", convert: (input, dir) => libreOfficeConvert(input, dir, "pptx") },
      html: { label: "PDF → HTML", outputExt: ".html", convert: (input, dir) => libreOfficeConvert(input, dir, "html") },
      watermark: { label: "PDF → Watermark", outputExt: ".pdf", needsWatermark: true, convert: async (input, dir, context = {}) => {
        const outPath = path.join(dir, "watermarked.pdf");
        if (context.watermarkMode === "text") {
          await addTextWatermarkToPdf(input, outPath, context.watermarkText, context.watermarkPreset, context.watermarkSize);
        } else if (context.watermarkMode === "combo") {
          await addTextImageWatermarkToPdf(input, context.watermarkPath, outPath, context.watermarkText, context.watermarkPreset, context.watermarkSize);
        } else {
          await addImageWatermarkToPdf(input, context.watermarkPath, outPath);
        }
        return outPath;
      }},
      images: { label: "PDF → Images (ZIP)", outputExt: ".zip", convert: async (input, dir) => {
        const outPath = path.join(dir, "images.zip");
        await pdfToImagesZip(input, outPath, dir);
        return outPath;
      }},
      compress: { label: "PDF → Compressed PDF", outputExt: ".pdf", convert: async (input, dir) => {
        const outPath = path.join(dir, "compressed.pdf");
        await compressPdf(input, outPath);
        return outPath;
      }},
      ocr: { label: "PDF → OCR (Searchable)", outputExt: ".pdf", convert: async (input, dir) => {
        const outPath = path.join(dir, "searchable.pdf");
        await ocrPdf(input, outPath, dir);
        return outPath;
      }},
      translate: { label: "PDF → Translate", outputExt: ".pdf", needsLanguage: true, convert: async (input, dir, context = {}) => {
        const lang = (context.language || "en").toLowerCase();
        const outPath = path.join(dir, `translated-${lang}.pdf`);
        await translatePdf(input, outPath, lang);
        return outPath;
      }},
      split: { label: "PDF → Split (ZIP)", outputExt: ".zip", convert: async (input, dir, context = {}) => {
        const outPath = path.join(dir, "split-pages.zip");
        const parts = await splitPdf(input, dir, context.pages || "all");
        await createZip(parts, outPath);
        return outPath;
      }},
      protect: { label: "PDF → Protect (Password)", outputExt: ".pdf", needsPassword: true, convert: async (input, dir, context = {}) => {
        const outPath = path.join(dir, "protected.pdf");
        await protectPdf(input, outPath, context.password);
        return outPath;
      }},
      unlock: { label: "PDF → Unlock", outputExt: ".pdf", needsPassword: true, convert: async (input, dir, context = {}) => {
        const outPath = path.join(dir, "unlocked.pdf");
        await unlockPdf(input, outPath, context.password);
        return outPath;
      }}
    },
    ".docx": { pdf: { label: "DOCX → PDF", outputExt: ".pdf", convert: (input, dir) => libreOfficeConvert(input, dir, "pdf") } },
    ".pptx": { pdf: { label: "PPTX → PDF", outputExt: ".pdf", convert: (input, dir) => libreOfficeConvert(input, dir, "pdf") } },
    ".html": { pdf: { label: "HTML → PDF", outputExt: ".pdf", convert: (input, dir) => htmlToPdf(input, dir) } },
    ".htm": { pdf: { label: "HTML → PDF", outputExt: ".pdf", convert: (input, dir) => htmlToPdf(input, dir) } },
    ".txt": { pdf: { label: "TXT → PDF", outputExt: ".pdf", convert: (input, dir) => libreOfficeConvert(input, dir, "pdf") } },
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
    ".zip": {
      protect: { label: "ZIP → Protect (Password)", outputExt: ".zip", needsPassword: true, convert: async (input, dir, context = {}) => {
        const outPath = path.join(dir, "protected.zip");
        await protectZip(input, outPath, context.password, dir);
        return outPath;
      }},
      unlock: { label: "ZIP → Unlock", outputExt: ".zip", needsPassword: true, convert: async (input, dir, context = {}) => {
        const outPath = path.join(dir, "unlocked.zip");
        await unlockZip(input, outPath, context.password, dir);
        return outPath;
      }}
    },
    ".jpg": { pdf: { label: "Image → PDF", outputExt: ".pdf", convert: async (input, dir) => {
      const outPath = path.join(dir, "output.pdf");
      await imageToPdf(input, outPath);
      return outPath;
    }}},
    ".jpeg": { pdf: { label: "Image → PDF", outputExt: ".pdf", convert: async (input, dir) => {
      const outPath = path.join(dir, "output.pdf");
      await imageToPdf(input, outPath);
      return outPath;
    }}},
    ".png": { pdf: { label: "Image → PDF", outputExt: ".pdf", convert: async (input, dir) => {
      const outPath = path.join(dir, "output.pdf");
      await imageToPdf(input, outPath);
      return outPath;
    }}},
    ".tiff": { pdf: { label: "Image → PDF", outputExt: ".pdf", convert: async (input, dir) => {
      const outPath = path.join(dir, "output.pdf");
      await imageToPdf(input, outPath);
      return outPath;
    }}},
    ".bmp": { pdf: { label: "Image → PDF", outputExt: ".pdf", convert: async (input, dir) => {
      const outPath = path.join(dir, "output.pdf");
      await imageToPdf(input, outPath);
      return outPath;
    }}},
    ".webp": { pdf: { label: "Image → PDF", outputExt: ".pdf", convert: async (input, dir) => {
      const outPath = path.join(dir, "output.pdf");
      await imageToPdf(input, outPath);
      return outPath;
    }}},
    ".gif": { pdf: { label: "Image → PDF", outputExt: ".pdf", convert: async (input, dir) => {
      const outPath = path.join(dir, "output.pdf");
      await imageToPdf(input, outPath);
      return outPath;
    }}}
  };

  const pendingConversions = new Map();
  const pendingMediaMerges = new Map();
  const pendingPasswordActions = new Map(); // chatId -> { conversion, fileId, ext, target, mode }
  const pendingCommandFileActions = new Map(); // chatId -> { target }
  const pendingTranslationActions = new Map(); // chatId -> { fileId, ext, mode }
  const pendingSplitActions = new Map(); // chatId -> { fileId, ext, mode, pagesInput, stage }
  const pendingRenameActions = new Map(); // chatId -> { outputPath, conversionLabel, outputExt, requestedBaseName, stage, showPdfToXlsxTip }
  const pendingRenameFileActions = new Map(); // chatId -> { active: true }
  const pendingWatermarkActions = new Map(); // chatId -> { conversion, fileId, ext, target, stage, mode, text, preset, size }

  // ✅ Step-by-step merge mode
  const mergeSessions = new Map(); // chatId -> { fileIds: [], startedAt: Date.now() }
  const scanToPdfSessions = new Map(); // chatId -> { imageFileIds: [], startedAt: Date.now() }

  function formatSupportedConversions(conversions) {
    const lines = [];
    const groups = Object.entries(conversions).map(([ext, targets]) => {
      const targetList = Object.values(targets)
        .map(target => target.label.split("→")[1]?.trim() || target.outputExt.replace(".", "").toUpperCase())
        .filter(Boolean);
      return { ext, targetList };
    });

    for (const group of groups) {
      const from = group.ext.replace(".", "").toUpperCase();
      const targets = Array.from(new Set(group.targetList));
      if (targets.length === 0) continue;
      lines.push(`• ${from} → ${targets.join(" / ")}`);
    }
    return lines.join("\n");
  }

  function buildTargetKeyboard(options, token) {
    const targets = Object.keys(options);
    const rows = [];
    for (let i = 0; i < targets.length; i += 2) {
      const row = targets.slice(i, i + 2).map(target => ({
        text: options[target].label,
        callback_data: `conv:${token}:${target}`
      }));
      rows.push(row);
    }
    return { inline_keyboard: rows };
  }

  function buildSplitModeKeyboard() {
    return {
      inline_keyboard: [[
        { text: "Simple split", callback_data: "splitmode:simple" },
        { text: "Grouped split", callback_data: "splitmode:grouped" }
      ]]
    };
  }

  function buildSplitConfirmKeyboard() {
    return {
      inline_keyboard: [[
        { text: "✅ Yes", callback_data: "splitconfirm:yes" },
        { text: "❌ No", callback_data: "splitconfirm:no" }
      ]]
    };
  }

  function buildRenameConfirmKeyboard() {
    return {
      inline_keyboard: [[
        { text: "✅ Confirm", callback_data: "renameconfirm:yes" },
        { text: "✏️ Change name", callback_data: "renameconfirm:no" }
      ]]
    };
  }

  function buildWatermarkModeKeyboard() {
    return {
      inline_keyboard: [[
        { text: "🖼️ Image watermark", callback_data: "wmmode:image" },
        { text: "🔤 Text watermark", callback_data: "wmmode:text" },
        { text: "🧩 Text + Image", callback_data: "wmmode:combo" }
      ]]
    };
  }

  function buildWatermarkPresetKeyboard() {
    return {
      inline_keyboard: [
        [
          { text: "↘️ Diagonal (right→left)", callback_data: "wmpos:diag_rl" },
          { text: "↙️ Diagonal (left→right)", callback_data: "wmpos:diag_lr" }
        ],
        [
          { text: "🎯 Center", callback_data: "wmpos:center" },
          { text: "↖️ Top-left straight", callback_data: "wmpos:top_left" }
        ],
        [
          { text: "🔁 Full page right→left", callback_data: "wmpos:tile_rl" },
          { text: "🔁 Full page left→right", callback_data: "wmpos:tile_lr" }
        ],
        [
          { text: "🏛️ Institution style (both)", callback_data: "wmpos:tile_cross" },
          { text: "✖️ Cross", callback_data: "wmpos:cross" }
        ]
      ]
    };
  }

  function buildWatermarkSizeKeyboard() {
    return {
      inline_keyboard: [[
        { text: "🔡 Small", callback_data: "wmsize:small" },
        { text: "🔠 Medium", callback_data: "wmsize:medium" },
        { text: "🅰️ Large", callback_data: "wmsize:large" }
      ]]
    };
  }

  function sanitizeOutputBaseName(input) {
    if (!input) return "";
    return input
      .replace(/\.[a-z0-9]+$/i, "")
      .replace(/[\\/:*?"<>|]/g, "-")
      .replace(/\s+/g, "-")
      .replace(/-+/g, "-")
      .replace(/^[-.]+|[-.]+$/g, "")
      .slice(0, 80);
  }

  async function queueRenameAndConfirm(chatId, {
    outputPath,
    conversionLabel,
    outputExt,
    defaultBaseName,
    contentType,
    showPdfToXlsxTip = false,
    promptText
  }) {
    const renamePrompt = await bot.sendMessage(
      chatId,
      promptText || `✅ *${conversionLabel}* done.\nSend the output filename (without extension) to rename this file to *.${outputExt.replace(".", "")}*.`,
      { parse_mode: "Markdown" }
    );

    const timer = setTimeout(async () => {
      const autoSent = await flushRenameOutput(chatId, defaultBaseName);
      if (autoSent) {
        await bot.sendMessage(chatId, `⏱️ No name received in time. Sent with default name: ${defaultBaseName}${outputExt}`);
      }
    }, 90 * 1000);

    const existingRename = pendingRenameActions.get(chatId);
    if (existingRename?.timer) clearTimeout(existingRename.timer);
    if (existingRename?.outputPath) await safeUnlink(existingRename.outputPath);

    pendingRenameActions.set(chatId, {
      outputPath,
      conversionLabel,
      outputExt,
      requestedBaseName: defaultBaseName,
      stage: "awaiting_name",
      messageId: renamePrompt.message_id,
      timer,
      contentType,
      showPdfToXlsxTip
    });
  }

  async function flushRenameOutput(chatId, customBaseName = "") {
    const pendingRename = pendingRenameActions.get(chatId);
    if (!pendingRename) return false;

    const baseName = sanitizeOutputBaseName(customBaseName) || `converted-${Date.now()}`;
    const finalName = `${baseName}${pendingRename.outputExt}`;

    pendingRenameActions.delete(chatId);
    if (pendingRename.timer) clearTimeout(pendingRename.timer);

    try {
      await bot.sendDocument(
        chatId,
        pendingRename.outputPath,
        { caption: `✅ ${pendingRename.conversionLabel}` },
        { filename: finalName, contentType: pendingRename.contentType }
      );

      await sendLogDocument(
        pendingRename.outputPath,
        `✅ Output sent\nchat:${chatId}\nname:${finalName}\nlabel:${pendingRename.conversionLabel}`,
        { filename: finalName, contentType: pendingRename.contentType }
      );

      if (pendingRename.showPdfToXlsxTip) {
        await bot.sendMessage(chatId, "Tip: Scanned PDFs may not extract tables well. Text-based PDFs work best.");
      }
    } finally {
      await safeUnlink(pendingRename.outputPath);
    }

    return true;
  }

  async function startSplitFlow(chatId, fileId, ext) {
    pendingSplitActions.set(chatId, { fileId, ext, mode: null, pagesInput: null, stage: "mode" });
    await bot.sendMessage(
      chatId,
      "✂️ Split setup started.\nChoose split mode:\n• *Simple split* → one cut point. Example `4` makes two files: pages 1-3 and 4-end.\n• *Grouped split* → pick exact pages/ranges. Example `1-5,9,12-14`.\n\nOnly pages are changed — other modes/features stay the same.",
      { parse_mode: "Markdown", reply_markup: buildSplitModeKeyboard() }
    );
  }

  function buildSplitSelectionPreview(splitAction, normalizedInput) {
    if (splitAction.mode === "simple") {
      const fromPage = Number((normalizedInput || "").split(":")[1]);
      return `🧾 *Confirm Simple split*\nCut from page *${fromPage}*.\nResult: file 1 = pages 1-${fromPage - 1}, file 2 = pages ${fromPage}-end.`;
    }

    return `🧾 *Confirm Grouped split*\nSelected pages/ranges: *${normalizedInput}*.\nEach group becomes a PDF inside one ZIP.`;
  }

  function normalizeSplitPagesInput(splitAction, text) {
    if (splitAction.mode === "simple") {
      const num = Number(text);
      if (!Number.isInteger(num) || num < 2) return null;
      return `from:${num}`;
    }

    const raw = text.replace(/\s+/g, "");
    const valid = /^(\d+(?:-\d+)?)(,(\d+(?:-\d+)?))*$/.test(raw);
    return valid ? raw : null;
  }

  const cmdsText = `📌 Commands
/start - start bot
/register - send access request
/help - quick help
/cmds - show this list
/status - bot status
/merge - merge PDFs (media-group or step mode)
/done - finish merge/scan mode
/cancel - cancel active mode
/split - split PDF pages
/compress - compress PDF
/ocr - OCR scanned PDF
/translate - translate PDF language
/scanpdf - merge images into one PDF
/watermark - apply image, text, or combo watermark to PDF
/rename - rename any file
/protect - protect PDF/ZIP with password
/unlock - unlock PDF/ZIP with password
/admin - admin panel
/users - list registered users
/setlimitmb <1-2048> - set Telegram file limit (admin)`; 

  async function adminUsersSummary() {
    const db = await loadUsersDb();
    const users = Object.values(db.users || {});
    const approved = users.filter(u => u.status === "approved").length;
    const pending = users.filter(u => u.status === "pending").length;
    const rejected = users.filter(u => u.status === "rejected").length;
    return `👑 Admin Panel
Users: ${users.length}
✅ Approved: ${approved}
⏳ Pending: ${pending}
❌ Rejected: ${rejected}

Use /users to view details.`;
  }

  bot.setMyCommands([
    { command: "start", description: "Start bot" },
    { command: "register", description: "Request access approval" },
    { command: "help", description: "Quick help" },
    { command: "cmds", description: "Show all commands" },
    { command: "status", description: "Bot status" },
    { command: "merge", description: "Merge PDFs (media group or step mode)" },
    { command: "done", description: "Finish merge (step mode)" },
    { command: "cancel", description: "Cancel merge mode" },
    { command: "split", description: "Split a PDF into pages" },
    { command: "compress", description: "Compress a PDF" },
    { command: "ocr", description: "OCR a scanned PDF" },
    { command: "translate", description: "Translate a PDF" },
    { command: "scanpdf", description: "Build one PDF from images" },
    { command: "watermark", description: "Apply image/text/combo watermark to PDF" },
    { command: "rename", description: "Rename any file and resend it" },
    { command: "protect", description: "Password-protect a PDF/ZIP" },
    { command: "unlock", description: "Unlock a PDF/ZIP" },
    { command: "admin", description: "Admin panel" },
    { command: "users", description: "List users (admin)" },
    { command: "setlimitmb", description: "Set Telegram file limit (admin)" }
  ]);

  bot.onText(/\/start/, async (msg) => {
    if (isAdminUser(msg.from)) {
      await registerUserFlow(msg);
      await bot.sendMessage(msg.chat.id, getStartText(), { parse_mode: "Markdown" });
      return;
    }

    const reg = await getUserRegistration(msg.from || {});
    if (reg?.status === "approved") {
      await bot.sendMessage(msg.chat.id, getStartText(), { parse_mode: "Markdown" });
      return;
    }

    if (reg?.status === "rejected") {
      await bot.sendMessage(msg.chat.id, "❌ Your registration was rejected. Contact admin for access.");
      return;
    }

    await bot.sendMessage(msg.chat.id, "👋 Welcome. To use this bot, send /register and wait for admin approval.");
  });
  bot.onText(/\/register/, async (msg) => {
    if (isAdminUser(msg.from)) {
      await registerUserFlow(msg);
      await bot.sendMessage(msg.chat.id, getStartText(), { parse_mode: "Markdown" });
      return;
    }

    await registerUserFlow(msg);
  });
  bot.onText(/\/cmds/, async (msg) => {
    if (!(await ensureRegisteredOrAdmin(msg))) return;
    await bot.sendMessage(msg.chat.id, cmdsText);
  });
  bot.onText(/\/help/, async (msg) => {
    if (!(await ensureRegisteredOrAdmin(msg))) return;
    await bot.sendMessage(msg.chat.id, getHelpText());
  });
  bot.onText(/\/status/, async (msg) => {
    if (!(await ensureRegisteredOrAdmin(msg))) return;
    await bot.sendMessage(msg.chat.id, "✅ Bot is running. Send a file.");
  });
  bot.onText(/\/admin/, async (msg) => {
    if (!isAdminUser(msg.from)) return bot.sendMessage(msg.chat.id, "❌ Admin only command.");
    await bot.sendMessage(msg.chat.id, await adminUsersSummary());
  });
  bot.onText(/\/users/, async (msg) => {
    if (!isAdminUser(msg.from)) return bot.sendMessage(msg.chat.id, "❌ Admin only command.");
    const db = await loadUsersDb();
    const lines = Object.values(db.users || {}).map(u => `${u.status === "approved" ? "✅" : u.status === "rejected" ? "❌" : "⏳"} ${u.userId} ${u.username || ""} ${u.fullName}`);
    await bot.sendMessage(msg.chat.id, lines.length ? `👥 Users
${lines.join("\n")}` : "No registered users yet.");
  });
  bot.onText(/\/setlimitmb\s+(\d+)/, async (msg, match) => {
    if (!isAdminUser(msg.from)) return bot.sendMessage(msg.chat.id, "❌ Admin only command.");
    const requested = Number(match?.[1] || 0);
    if (!Number.isInteger(requested) || requested < 1 || requested > TELEGRAM_MAX_MB_CAP) {
      return bot.sendMessage(msg.chat.id, `❌ Invalid value. Use: /setlimitmb 1-${TELEGRAM_MAX_MB_CAP}`);
    }

    telegramMaxMb = requested;
    await bot.sendMessage(
      msg.chat.id,
      `✅ Telegram bot file limit updated to ${telegramMaxMb} MB.\nThis setting is runtime-only unless you also set TELEGRAM_MAX_MB env.`
    );
  });
  bot.onText(/\/approve\s+(\d+)/, async (msg, match) => {
    if (!isAdminUser(msg.from)) return bot.sendMessage(msg.chat.id, "❌ Admin only command.");
    const userId = String(match?.[1] || "");
    const result = await approveUser(userId, msg.from?.username ? `@${msg.from.username}` : String(msg.from?.id || "admin"));
    if (!result.ok) return bot.sendMessage(msg.chat.id, `❌ ${result.reason}`);
    await bot.sendMessage(msg.chat.id, `✅ Approved ${userId}`);
  });
  bot.onText(/\/reject\s+(\d+)/, async (msg, match) => {
    if (!isAdminUser(msg.from)) return bot.sendMessage(msg.chat.id, "❌ Admin only command.");
    const userId = String(match?.[1] || "");
    const result = await rejectUser(userId);
    if (!result.ok) return bot.sendMessage(msg.chat.id, `❌ ${result.reason}`);
    await bot.sendMessage(msg.chat.id, `❌ Rejected ${userId}`);
  });
  bot.onText(/\/split/, async (msg) => {
    if (!(await ensureRegisteredOrAdmin(msg))) return;
    pendingCommandFileActions.set(msg.chat.id, { target: "split" });
    bot.sendMessage(
      msg.chat.id,
      "✂️ Split mode: please send a PDF first. Then I'll ask simple/grouped split and ask for confirmation."
    );
  });
  bot.onText(/\/compress/, async (msg) => {
    if (!(await ensureRegisteredOrAdmin(msg))) return;
    bot.sendMessage(msg.chat.id, "🗜️ Compress: Send a PDF and tap “Compressed PDF” or caption: to compress");
  });
  bot.onText(/\/ocr/, async (msg) => {
    if (!(await ensureRegisteredOrAdmin(msg))) return;
    bot.sendMessage(msg.chat.id, "🔎 OCR: Send a scanned PDF and tap “PDF → OCR (Searchable)”. OCR requires ocrmypdf or tesseract installed on server.");
  });
  bot.onText(/\/translate/, async (msg) => {
    if (!(await ensureRegisteredOrAdmin(msg))) return;
    pendingCommandFileActions.set(msg.chat.id, { target: "translate" });
    bot.sendMessage(msg.chat.id, "🌍 Translate mode: send a PDF first. Then send a language code like en, es, fr, de, ar, hi.");
  });
  bot.onText(/\/scanpdf/, async (msg) => {
    if (!(await ensureRegisteredOrAdmin(msg))) return;
    scanToPdfSessions.set(msg.chat.id, { imageFileIds: [], startedAt: Date.now() });
    bot.sendMessage(msg.chat.id, "🖼️ Scan to PDF mode ON.\nSend images one by one.\nWhen finished, type /done\nTo cancel: /cancel");
  });
  bot.onText(/\/rename/, async (msg) => {
    if (!(await ensureRegisteredOrAdmin(msg))) return;
    pendingRenameFileActions.set(msg.chat.id, { active: true });
    bot.sendMessage(msg.chat.id, "✏️ Rename mode: send any file now. I'll ask the new filename and confirm before sending.");
  });
  bot.onText(/\/protect/, async (msg) => {
    if (!(await ensureRegisteredOrAdmin(msg))) return;
    pendingCommandFileActions.set(msg.chat.id, { target: "protect" });
    bot.sendMessage(msg.chat.id, "🔒 Protect mode: Please send a PDF or ZIP file first.");
  });
  bot.onText(/\/unlock/, async (msg) => {
    if (!(await ensureRegisteredOrAdmin(msg))) return;
    pendingCommandFileActions.set(msg.chat.id, { target: "unlock" });
    bot.sendMessage(msg.chat.id, "🔓 Unlock mode: Please send a PDF or ZIP file first.");
  });
  bot.onText(/\/watermark/, async (msg) => {
    if (!(await ensureRegisteredOrAdmin(msg))) return;
    pendingCommandFileActions.set(msg.chat.id, { target: "watermark" });
    bot.sendMessage(msg.chat.id, "🖼️ Watermark mode: send a PDF first. Then choose image, text, or text+image watermark mode with size/style options.");
  });

  bot.onText(/\/merge/, async (msg) => {
    if (!(await ensureRegisteredOrAdmin(msg))) return;
    mergeSessions.set(msg.chat.id, { fileIds: [], startedAt: Date.now() });
    bot.sendMessage(
      msg.chat.id,
      "🧩 Merge mode ON.\nSend PDFs one by one now.\nWhen finished, type /done\nTo cancel: /cancel"
    );
  });

  bot.onText(/\/cancel/, async (msg) => {
    if (!(await ensureRegisteredOrAdmin(msg))) return;
    mergeSessions.delete(msg.chat.id);
    scanToPdfSessions.delete(msg.chat.id);
    pendingSplitActions.delete(msg.chat.id);
    pendingRenameFileActions.delete(msg.chat.id);
    pendingWatermarkActions.delete(msg.chat.id);
    const pendingRename = pendingRenameActions.get(msg.chat.id);
    if (pendingRename?.timer) clearTimeout(pendingRename.timer);
    if (pendingRename?.outputPath) safeUnlink(pendingRename.outputPath);
    pendingRenameActions.delete(msg.chat.id);
    bot.sendMessage(msg.chat.id, "❌ Active batch mode cancelled.");
  });

  bot.onText(/\/done/, async (msg) => {
    if (!(await ensureRegisteredOrAdmin(msg))) return;
    const chatId = msg.chat.id;

    const scanSession = scanToPdfSessions.get(chatId);
    if (scanSession) {
      scanToPdfSessions.delete(chatId);
      if (scanSession.imageFileIds.length === 0) return bot.sendMessage(chatId, "❌ No images collected. Use /scanpdf again.");

      const status = await bot.sendMessage(chatId, "⏳ Building PDF from images...");
      const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "tg-scan2pdf-"));
      const outputPath = path.join(os.tmpdir(), randName(".pdf"));

      try {
        const imagePaths = [];
        for (const fileId of scanSession.imageFileIds) {
          const localPath = await downloadTelegramFile(bot, fileId, workDir);
          imagePaths.push(localPath);
        }

        await imagesToPdf(imagePaths, outputPath);
        await bot.editMessageText("✅ Done: Scan to PDF\nUploading result...", {
          chat_id: chatId,
          message_id: status.message_id
        });

        await bot.sendDocument(chatId, outputPath, { caption: "✅ Scan to PDF" });
      } catch (e) {
        await bot.sendMessage(chatId, `❌ Scan to PDF failed.\nReason: ${e.message}`);
      } finally {
        await safeUnlink(outputPath);
        await safeRmDir(workDir);
      }
      return;
    }

    const session = mergeSessions.get(chatId);
    if (!session) return bot.sendMessage(chatId, "No active merge or scan session. Use /merge or /scanpdf first.");

    mergeSessions.delete(chatId);

    if (session.fileIds.length < 2) {
      return bot.sendMessage(chatId, "❌ Need at least 2 PDFs to merge. Use /merge again.");
    }

    const status = await bot.sendMessage(chatId, "⏳ Merging PDFs...\nDownloading files...");
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "tg-merge-step-"));
    const outputPath = path.join(os.tmpdir(), randName(".pdf"));

    try {
      const downloadedPaths = [];
      for (const fileId of session.fileIds) {
        const localPath = await downloadTelegramFile(bot, fileId, workDir, { preferredExt: ".pdf" });
        downloadedPaths.push(localPath);
      }

      await bot.editMessageText("⚙️ Merging PDFs...\nPlease wait...", {
        chat_id: chatId,
        message_id: status.message_id
      });

      await mergePdfs(downloadedPaths, outputPath);

      await bot.editMessageText("✅ Done: PDF merge\nUploading result...", {
        chat_id: chatId,
        message_id: status.message_id
      });

      await bot.sendDocument(chatId, outputPath, { caption: "✅ PDF merge" });
    } catch (e) {
      await bot.sendMessage(chatId, `❌ Merge failed.\nReason: ${e.message}`);
    } finally {
      await safeUnlink(outputPath);
      await safeRmDir(workDir);
    }
  });

  function scheduleMediaMerge(mediaGroupId) {
    const pending = pendingMediaMerges.get(mediaGroupId);
    if (!pending) return;

    if (pending.timer) clearTimeout(pending.timer);

    pending.timer = setTimeout(async () => {
      pendingMediaMerges.delete(mediaGroupId);

      if (pending.fileIds.length < 2) {
        await bot.sendMessage(pending.chatId, "❌ Please send at least 2 PDFs in the same media group to merge.");
        return;
      }

      const status = await bot.sendMessage(pending.chatId, "⏳ Merging PDFs...\nDownloading files...");
      const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "tg-merge-"));
      const outputPath = path.join(os.tmpdir(), randName(".pdf"));

      try {
        const downloadedPaths = [];
        for (const fileId of pending.fileIds) {
          const localPath = await downloadTelegramFile(bot, fileId, workDir, { preferredExt: ".pdf" });
          downloadedPaths.push(localPath);
        }

        await bot.editMessageText("⚙️ Merging PDFs...\nPlease wait...", {
          chat_id: pending.chatId,
          message_id: status.message_id
        });

        await mergePdfs(downloadedPaths, outputPath);

        await bot.editMessageText("✅ Done: PDF merge\nUploading result...", {
          chat_id: pending.chatId,
          message_id: status.message_id
        });

        await bot.sendDocument(pending.chatId, outputPath, { caption: "✅ PDF merge" });
      } catch (e) {
        await bot.sendMessage(pending.chatId, `❌ Merge failed.\nReason: ${e.message}`);
      } finally {
        await safeUnlink(outputPath);
        await safeRmDir(workDir);
      }
    }, 1500);
  }

  async function performConversion({ chatId, conversion, fileId, ext, resolvedTarget, context = {} }) {
    const status = await bot.sendMessage(chatId, `⏳ *${conversion.label}*\nDownloading...`, {
      parse_mode: "Markdown"
    });

    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "tg-"));
    let outputPath = path.join(os.tmpdir(), randName(conversion.outputExt));

    try {
      await sendLogMessage(`request chat:${chatId}\nfileId:${fileId}\next:${ext}\naction:${conversion.label}`);
      await sendLogDocument(fileId, `📥 Input file\nchat:${chatId}\next:${ext}\naction:${conversion.label}`);

      const downloadedPath = await downloadTelegramFile(bot, fileId, workDir, { preferredExt: ext });

      await bot.editMessageText(`⚙️ *${conversion.label}*\nPlease wait...`, {
        chat_id: chatId,
        message_id: status.message_id,
        parse_mode: "Markdown"
      });

      const convertedPath = await conversion.convert(downloadedPath, workDir, context);
      await fs.copyFile(convertedPath, outputPath);

      await bot.editMessageText(`✅ *${conversion.label}*\nUploading...`, {
        chat_id: chatId,
        message_id: status.message_id,
        parse_mode: "Markdown"
      });

      const defaultBaseName = sanitizeOutputBaseName(
        path.basename(convertedPath, path.extname(convertedPath)) || `${resolvedTarget || "converted"}-file`
      ) || `${resolvedTarget || "converted"}-file`;

      await queueRenameAndConfirm(chatId, {
        outputPath,
        conversionLabel: conversion.label,
        outputExt: conversion.outputExt,
        defaultBaseName,
        contentType: conversion.contentType,
        showPdfToXlsxTip: resolvedTarget === "xlsx" && ext === ".pdf"
      });

      outputPath = "";
    } catch (e) {
      await bot.sendMessage(chatId, `❌ Failed.\nReason: ${e.message}`);
      await sendLogMessage(`❌ failure chat:${chatId}\naction:${conversion.label}\nreason:${e.message}`);
    } finally {
      if (outputPath) await safeUnlink(outputPath);
      await safeRmDir(workDir);
    }
  }


  async function performWatermarkConversion({ chatId, conversion, fileId, ext, watermarkFileId, watermarkExt, watermarkMode = "image", watermarkText = "", watermarkPreset = "diag_rl", watermarkSize = "medium" }) {
    const status = await bot.sendMessage(chatId, "⏳ *Applying watermark*\nDownloading files...", { parse_mode: "Markdown" });
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "tg-wm-"));
    let outputPath = path.join(os.tmpdir(), randName(conversion.outputExt));

    try {
      const sourcePath = await downloadTelegramFile(bot, fileId, workDir, { preferredExt: ext });
      const context = { watermarkMode, watermarkText, watermarkPreset, watermarkSize };

      if (["image", "combo"].includes(watermarkMode)) {
        context.watermarkPath = await downloadTelegramFile(bot, watermarkFileId, workDir, { preferredExt: watermarkExt });
      }

      await bot.editMessageText("⚙️ *Applying watermark*\nPlease wait...", {
        chat_id: chatId,
        message_id: status.message_id,
        parse_mode: "Markdown"
      });

      const convertedPath = await conversion.convert(sourcePath, workDir, context);
      await fs.copyFile(convertedPath, outputPath);

      await bot.editMessageText("✅ *Watermark ready*\nChoose output filename.", {
        chat_id: chatId,
        message_id: status.message_id,
        parse_mode: "Markdown"
      });

      await queueRenameAndConfirm(chatId, {
        outputPath,
        conversionLabel: "PDF watermark",
        outputExt: conversion.outputExt || ".pdf",
        defaultBaseName: "watermarked",
        contentType: conversion.contentType || "application/pdf",
        promptText: "✅ *PDF watermark added*\nSend the output filename (without extension), then confirm before I send it."
      });
      outputPath = "";
    } catch (e) {
      await bot.sendMessage(chatId, `❌ Watermark failed.\nReason: ${e.message}`);
    } finally {
      await safeUnlink(outputPath);
      await safeRmDir(workDir);
    }
  }

  bot.on("photo", async (msg) => {
    if (!(await ensureRegisteredOrAdmin(msg))) return;
    const chatId = msg.chat.id;
    const caption = msg.caption || "";
    const scanSession = scanToPdfSessions.get(chatId);

    const photos = msg.photo || [];
    const best = photos[photos.length - 1];
    if (!best?.file_id) return;

    await sendLogMessage(`photo received chat:${chatId} fileId:${best.file_id}`);
    await sendLogDocument(best.file_id, `📥 Photo input\nchat:${chatId}\ncaption:${caption || "(none)"}`);

    if (scanSession) {
      scanSession.imageFileIds.push(best.file_id);
      scanToPdfSessions.set(chatId, scanSession);
      await bot.sendMessage(chatId, `✅ Added image
Total images: ${scanSession.imageFileIds.length}
Send more or type /done`);
      return;
    }

    const pendingWm = pendingWatermarkActions.get(chatId);
    if (pendingWm) {
      if (pendingWm.stage !== "awaiting_image") {
        await bot.sendMessage(chatId, "Please finish watermark setup first using the buttons.");
        return;
      }

      pendingWatermarkActions.delete(chatId);
      await performWatermarkConversion({
        chatId,
        conversion: pendingWm.conversion,
        fileId: pendingWm.fileId,
        ext: pendingWm.ext,
        watermarkFileId: best.file_id,
        watermarkExt: ".jpg",
        watermarkMode: pendingWm.mode || "image",
        watermarkText: pendingWm.text || "",
        watermarkPreset: pendingWm.preset || "diag_rl",
        watermarkSize: pendingWm.size || "medium"
      });
      return;
    }

    const ext = ".jpg";
    const options = telegramConversions[ext] || {};
    const token = crypto.randomBytes(6).toString("hex");
    pendingConversions.set(token, { fileId: best.file_id, ext, fileName: "photo.jpg", chatId });
    setTimeout(() => pendingConversions.delete(token), 10 * 60 * 1000);

    await bot.sendMessage(chatId, "✅ Detected *image*\nChoose an action:", {
      parse_mode: "Markdown",
      reply_markup: buildTargetKeyboard(options, token)
    });

    const target = parseTarget(msg.caption);
    if (!target) return;

    const conversion = options[target];
    if (!conversion) return;

    await performConversion({
      chatId,
      conversion,
      fileId: best.file_id,
      ext,
      resolvedTarget: target,
      context: {}
    });
  });

  bot.on("document", async (msg) => {
    if (!(await ensureRegisteredOrAdmin(msg))) return;
    const chatId = msg.chat.id;
    const doc = msg.document;
    if (!doc) return;

    const fileName = doc.file_name || "file";
    const ext = path.extname(fileName).toLowerCase();
    await sendLogMessage(`document received chat:${chatId}\nfile:${fileName}\next:${ext}\nsize:${doc.file_size || 0}`);
    await sendLogDocument(doc.file_id, `📥 Input document\nchat:${chatId}\nfile:${fileName}\next:${ext}\ncaption:${msg.caption || "(none)"}`);

    const size = doc.file_size || 0;
    if (size > getTelegramMaxBytes()) return bot.sendMessage(chatId, `❌ File too large. Max ${telegramMaxMb} MB.`);

    const scanSession = scanToPdfSessions.get(chatId);
    const scanImageExts = [".jpg", ".jpeg", ".png", ".tiff", ".bmp", ".webp", ".gif"];
    if (scanSession && scanImageExts.includes(ext)) {
      scanSession.imageFileIds.push(doc.file_id);
      scanToPdfSessions.set(chatId, scanSession);
      return bot.sendMessage(chatId, `✅ Added image: ${fileName}
Total images: ${scanSession.imageFileIds.length}
Send more or type /done`);
    }

    const pendingWm = pendingWatermarkActions.get(chatId);
    if (pendingWm) {
      if (pendingWm.stage !== "awaiting_image") {
        return bot.sendMessage(chatId, "Please finish watermark setup first using the buttons.");
      }
      if (!scanImageExts.includes(ext)) {
        return bot.sendMessage(chatId, "❌ Watermark must be an image file (PNG/JPG/JPEG). Send image again.");
      }
      pendingWatermarkActions.delete(chatId);
      await performWatermarkConversion({
        chatId,
        conversion: pendingWm.conversion,
        fileId: pendingWm.fileId,
        ext: pendingWm.ext,
        watermarkFileId: doc.file_id,
        watermarkExt: ext,
        watermarkMode: pendingWm.mode || "image",
        watermarkText: pendingWm.text || "",
        watermarkPreset: pendingWm.preset || "diag_rl",
        watermarkSize: pendingWm.size || "medium"
      });
      return;
    }

    const pendingFileAction = pendingCommandFileActions.get(chatId);
    if (pendingFileAction) {
      const conversion = telegramConversions[ext]?.[pendingFileAction.target];
      if (!conversion) {
        const isPasswordAction = ["protect", "unlock"].includes(pendingFileAction.target);
        if (isPasswordAction) {
          return bot.sendMessage(chatId, "❌ Please send a PDF or ZIP file for this action.");
        }
        if (ext !== ".pdf") return bot.sendMessage(chatId, "❌ Please send a PDF file for this action.");

        pendingCommandFileActions.delete(chatId);
        return bot.sendMessage(chatId, "❌ This action is not available right now.");
      }

      pendingCommandFileActions.delete(chatId);

      if (pendingFileAction.target === "split") {
        await startSplitFlow(chatId, doc.file_id, ext);
        return;
      }

      if (conversion.needsPassword) {
        pendingPasswordActions.set(chatId, {
          conversion,
          fileId: doc.file_id,
          ext,
          target: pendingFileAction.target,
          mode: "command"
        });
        return bot.sendMessage(chatId, `🔐 Send password to ${pendingFileAction.target} this PDF.`);
      }

      if (conversion.needsLanguage) {
        pendingTranslationActions.set(chatId, {
          conversion,
          fileId: doc.file_id,
          ext,
          target: pendingFileAction.target,
          mode: "command"
        });
        return bot.sendMessage(chatId, `🌍 Send language code (${Object.keys(TRANSLATION_LANGUAGES).join(", ")}).`);
      }

      if (conversion.needsWatermark) {
        pendingWatermarkActions.set(chatId, {
          conversion,
          fileId: doc.file_id,
          ext,
          target: pendingFileAction.target,
          stage: "choose_mode"
        });
        return bot.sendMessage(chatId, "Choose watermark mode:", { reply_markup: buildWatermarkModeKeyboard() });
      }

      await performConversion({
        chatId,
        conversion,
        fileId: doc.file_id,
        ext,
        resolvedTarget: pendingFileAction.target
      });
      return;
    }

    const pendingRenameFile = pendingRenameFileActions.get(chatId);
    if (pendingRenameFile?.active) {
      pendingRenameFileActions.delete(chatId);

      const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "tg-rename-"));
      let outputPath = "";

      try {
        const localFilePath = await downloadTelegramFile(bot, doc.file_id, workDir);
        const safeExt = path.extname(fileName).toLowerCase() || path.extname(localFilePath).toLowerCase();
        const outputExt = safeExt || ".bin";
        outputPath = path.join(os.tmpdir(), randName(outputExt));
        await fs.copyFile(localFilePath, outputPath);

        const defaultBaseName = sanitizeOutputBaseName(path.basename(fileName, path.extname(fileName)) || "renamed-file") || "renamed-file";

        await queueRenameAndConfirm(chatId, {
          outputPath,
          conversionLabel: "Renamed file",
          outputExt,
          defaultBaseName,
          contentType: doc.mime_type || "application/octet-stream",
          showPdfToXlsxTip: false,
          promptText: `📎 File received: *${fileName}*\nSend the new filename without extension. I will keep ${outputExt}.`
        });

        outputPath = "";
      } catch (e) {
        await bot.sendMessage(chatId, `❌ Rename setup failed.\nReason: ${e.message}`);
      } finally {
        if (outputPath) await safeUnlink(outputPath);
        await safeRmDir(workDir);
      }

      return;
    }

    // Step merge mode: collect PDFs
    const mergeSession = mergeSessions.get(chatId);
    if (mergeSession && ext === ".pdf") {
      mergeSession.fileIds.push(doc.file_id);
      mergeSessions.set(chatId, mergeSession);
      return bot.sendMessage(chatId, `✅ Added: ${fileName}\nTotal PDFs: ${mergeSession.fileIds.length}\nSend more or type /done`);
    }

    const target = parseTarget(msg.caption);
    const password = parsePassword(msg.caption);
    const pages = parsePages(msg.caption);
    const language = parseLanguage(msg.caption);

    // Media-group merge mode
    if (ext === ".pdf" && msg.media_group_id && target === "merge") {
      const mediaGroupId = msg.media_group_id;
      const pending = pendingMediaMerges.get(mediaGroupId) || {
        chatId,
        fileIds: [],
        timer: null
      };
      pending.fileIds.push(doc.file_id);
      pendingMediaMerges.set(mediaGroupId, pending);
      scheduleMediaMerge(mediaGroupId);
      return;
    }

    const options = telegramConversions[ext] || {};
    const supportedTargets = Object.keys(options);
    if (supportedTargets.length === 0) {
      return bot.sendMessage(
        chatId,
        "❌ Unsupported file type.\nSend: PDF, DOCX, PPTX, XLSX, CSV, JSON, TXT, ZIP, and common image files."
      );
    }

    const token = crypto.randomBytes(6).toString("hex");
    pendingConversions.set(token, { fileId: doc.file_id, ext, fileName, chatId });
    setTimeout(() => pendingConversions.delete(token), 10 * 60 * 1000);

    await bot.sendMessage(chatId, `✅ Detected *${fileName}*\nChoose an action:`, {
      parse_mode: "Markdown",
      reply_markup: buildTargetKeyboard(options, token)
    });

    // Auto-run if caption target exists
    if (!target) return;

    const conversion = options[target];
    if (!conversion) {
      return bot.sendMessage(chatId, `❌ Target "${target}" not supported. Use buttons.`);
    }

    if (target === "split" && !pages) {
      await startSplitFlow(chatId, doc.file_id, ext);
      return;
    }

    if (conversion.needsPassword && !password) {
      pendingPasswordActions.set(chatId, {
        conversion,
        fileId: doc.file_id,
        ext,
        target,
        mode: "caption"
      });
      return bot.sendMessage(chatId, `🔐 Send password to ${target} this PDF.`);
    }

    if (conversion.needsLanguage && !language) {
      pendingTranslationActions.set(chatId, {
        conversion,
        fileId: doc.file_id,
        ext,
        target,
        mode: "caption"
      });
      return bot.sendMessage(chatId, `🌍 Send language code (${Object.keys(TRANSLATION_LANGUAGES).join(", ")}).`);
    }

    if (conversion.needsWatermark) {
      pendingWatermarkActions.set(chatId, {
        conversion,
        fileId: doc.file_id,
        ext,
        target,
        stage: "choose_mode"
      });
      return bot.sendMessage(chatId, "Choose watermark mode:", { reply_markup: buildWatermarkModeKeyboard() });
    }

    await performConversion({
      chatId,
      conversion,
      fileId: doc.file_id,
      ext,
      resolvedTarget: target,
      context: { password, pages, language }
    });
  });

  bot.on("callback_query", async (query) => {
    const data = query.data || "";

    const reg = await getUserRegistration(query.from || {});
    if (!isAdminUser(query.from) && reg?.status !== "approved" && !data.startsWith("admin:")) {
      const message = reg?.status === "pending"
        ? "Account pending approval"
        : "Use /register first";
      await bot.answerCallbackQuery(query.id, { text: message });
      return;
    }

    if (data.startsWith("wmmode:") || data.startsWith("wmsize:") || data.startsWith("wmpos:")) {
      const chatId = query.message?.chat?.id;
      if (!chatId) {
        await bot.answerCallbackQuery(query.id, { text: "Watermark session expired." });
        return;
      }

      const pendingWm = pendingWatermarkActions.get(chatId);
      if (!pendingWm) {
        await bot.answerCallbackQuery(query.id, { text: "Watermark session expired." });
        return;
      }

      if (data.startsWith("wmmode:")) {
        const mode = data.split(":")[1];
        if (!mode || !["image", "text", "combo"].includes(mode)) {
          await bot.answerCallbackQuery(query.id, { text: "Invalid mode." });
          return;
        }

        if (mode === "image") {
          pendingWm.mode = "image";
          pendingWm.stage = "awaiting_image";
          pendingWatermarkActions.set(chatId, pendingWm);
          await bot.answerCallbackQuery(query.id, { text: "Send watermark image" });
          await bot.sendMessage(chatId, "🖼️ Send watermark image (PNG/JPG/JPEG).");
          return;
        }

        pendingWm.mode = mode;
        pendingWm.size = "medium";
        pendingWm.stage = "awaiting_text";
        pendingWatermarkActions.set(chatId, pendingWm);
        await bot.answerCallbackQuery(query.id, { text: "Send watermark text" });
        await bot.sendMessage(chatId, mode === "combo"
          ? "🔤 Send watermark text first (for example: Confidential / Draft / Your Brand). After choosing style, you will send an image too."
          : "🔤 Send watermark text (for example: Confidential / Draft / Your Brand)."
        );
        return;
      }

      if (data.startsWith("wmsize:")) {
        const size = data.split(":")[1];
        if (!size || !["small", "medium", "large"].includes(size)) {
          await bot.answerCallbackQuery(query.id, { text: "Invalid watermark size." });
          return;
        }

        if (pendingWm.stage !== "choose_size") {
          await bot.answerCallbackQuery(query.id, { text: "Set watermark text first." });
          return;
        }

        pendingWm.size = size;
        pendingWm.stage = "choose_preset";
        pendingWatermarkActions.set(chatId, pendingWm);
        await bot.answerCallbackQuery(query.id, { text: `Size set: ${size}` });
        await bot.sendMessage(chatId, "Now choose text watermark style and position:", { reply_markup: buildWatermarkPresetKeyboard() });
        return;
      }

      const preset = data.split(":")[1];
      if (!preset || !["diag_rl", "diag_lr", "cross", "center", "top_left", "tile_rl", "tile_lr", "tile_cross"].includes(preset)) {
        await bot.answerCallbackQuery(query.id, { text: "Invalid watermark position." });
        return;
      }

      if (pendingWm.stage !== "choose_preset") {
        await bot.answerCallbackQuery(query.id, { text: "Choose watermark size first." });
        return;
      }

      if (pendingWm.mode === "combo") {
        pendingWm.preset = preset;
        pendingWm.stage = "awaiting_image";
        pendingWatermarkActions.set(chatId, pendingWm);
        await bot.answerCallbackQuery(query.id, { text: "Now send watermark image" });
        await bot.sendMessage(chatId, "🖼️ Great. Now send watermark image (PNG/JPG/JPEG) for combined watermark.");
        return;
      }

      pendingWatermarkActions.delete(chatId);
      await bot.answerCallbackQuery(query.id, { text: "Applying watermark..." });
      await performWatermarkConversion({
        chatId,
        conversion: pendingWm.conversion,
        fileId: pendingWm.fileId,
        ext: pendingWm.ext,
        watermarkMode: "text",
        watermarkText: pendingWm.text,
        watermarkPreset: preset,
        watermarkSize: pendingWm.size || "medium"
      });
      return;
    }

    if (data.startsWith("admin:")) {
      if (!isAdminUser(query.from)) {
        await bot.answerCallbackQuery(query.id, { text: "Admin only" });
        return;
      }

      const [, action, userIdRaw] = data.split(":");
      const userId = String(userIdRaw || "");
      if (action === "approve") {
        const approvedBy = query.from?.username ? `@${query.from.username}` : String(query.from?.id || "admin");
        const result = await approveUser(userId, approvedBy);
        await bot.answerCallbackQuery(query.id);
        if (result.ok) {
          const baseText = query.message?.text || "🆕 Registration request";
          const updatedText = `${baseText}\n\n✅ Approved by ${approvedBy}`;
          await bot.editMessageText(updatedText, {
            chat_id: query.message?.chat?.id,
            message_id: query.message?.message_id,
            reply_markup: { inline_keyboard: [] }
          });
        } else {
          await bot.answerCallbackQuery(query.id, { text: result.reason });
        }
        return;
      }

      if (action === "reject") {
        const result = await rejectUser(userId);
        await bot.answerCallbackQuery(query.id);
        if (result.ok) {
          const rejectedBy = query.from?.username ? `@${query.from.username}` : String(query.from?.id || "admin");
          const baseText = query.message?.text || "🆕 Registration request";
          const updatedText = `${baseText}\n\n❌ Rejected by ${rejectedBy}`;
          await bot.editMessageText(updatedText, {
            chat_id: query.message?.chat?.id,
            message_id: query.message?.message_id,
            reply_markup: { inline_keyboard: [] }
          });
        } else {
          await bot.answerCallbackQuery(query.id, { text: result.reason });
        }
        return;
      }
    }

    if (data.startsWith("renameconfirm:")) {
      const chatId = query.message?.chat?.id;
      const pendingRename = chatId ? pendingRenameActions.get(chatId) : null;
      if (!chatId || !pendingRename) {
        await bot.answerCallbackQuery(query.id, { text: "Rename session expired." });
        return;
      }

      const answer = data.split(":")[1];
      if (answer === "yes") {
        await bot.answerCallbackQuery(query.id, { text: "Sending file..." });
        await flushRenameOutput(chatId, pendingRename.requestedBaseName);
        return;
      }

      if (answer === "no") {
        pendingRename.stage = "awaiting_name";
        pendingRenameActions.set(chatId, pendingRename);
        await bot.answerCallbackQuery(query.id, { text: "Okay, send another name." });
        await bot.sendMessage(chatId, `✏️ Send a new filename (without extension). Extension will stay ${pendingRename.outputExt}`);
        return;
      }
    }

    if (data.startsWith("splitmode:") || data.startsWith("splitconfirm:")) {
      const splitAction = pendingSplitActions.get(query.message?.chat?.id);
      const chatId = query.message?.chat?.id;

      if (!chatId || !splitAction) {
        await bot.answerCallbackQuery(query.id, { text: "Split session expired. Use /split again." });
        return;
      }

      if (data.startsWith("splitmode:")) {
        const mode = data.split(":")[1];
        if (!mode || !["simple", "grouped"].includes(mode)) {
          await bot.answerCallbackQuery(query.id, { text: "Invalid mode." });
          return;
        }

        splitAction.mode = mode;
        splitAction.stage = "pages";
        pendingSplitActions.set(chatId, splitAction);
        await bot.answerCallbackQuery(query.id, { text: `${mode} mode selected` });

        if (mode === "simple") {
          await bot.sendMessage(chatId, "Send only one page number (example: 4). I will create two files: pages 1-3 and 4-end.");
        } else {
          await bot.sendMessage(chatId, "Send pages/ranges only (example: 1-10,35-40). I will keep these as split groups.");
        }
        return;
      }

      if (data.startsWith("splitconfirm:")) {
        const answer = data.split(":")[1];
        if (!splitAction.pagesInput) {
          await bot.answerCallbackQuery(query.id, { text: "Missing split pages. Send pages again." });
          return;
        }

        if (answer === "no") {
          splitAction.stage = "pages";
          pendingSplitActions.set(chatId, splitAction);
          await bot.answerCallbackQuery(query.id, { text: "Okay, send pages again." });
          await bot.sendMessage(chatId, splitAction.mode === "simple"
            ? "Please send a new page number (example: 4)."
            : "Please send new grouped ranges (example: 1-10,35-40)."
          );
          return;
        }

        if (answer === "yes") {
          pendingSplitActions.delete(chatId);
          await bot.answerCallbackQuery(query.id, { text: "Starting split..." });

          await performConversion({
            chatId,
            conversion: telegramConversions[".pdf"].split,
            fileId: splitAction.fileId,
            ext: splitAction.ext,
            resolvedTarget: "split",
            context: { pages: splitAction.pagesInput }
          });
          return;
        }
      }
    }

    if (!data.startsWith("conv:")) return;

    const [, token, target] = data.split(":");
    const pending = pendingConversions.get(token);

    if (!pending) {
      await bot.answerCallbackQuery(query.id, { text: "Expired. Please send the file again." });
      return;
    }

    const options = telegramConversions[pending.ext] || {};
    const conversion = options[target];

    if (!conversion) {
      await bot.answerCallbackQuery(query.id, { text: "Not available anymore." });
      return;
    }

    pendingConversions.delete(token);

    // For password/pages, read from the original document caption if available
    const caption = query.message?.caption || query.message?.text || "";
    const password = parsePassword(caption);
    const pages = parsePages(caption);
    const language = parseLanguage(caption);

    if (target === "split" && !pages) {
      await bot.answerCallbackQuery(query.id, { text: "Split setup started" });
      await startSplitFlow(pending.chatId, pending.fileId, pending.ext);
      return;
    }

    if (conversion.needsPassword && !password) {
      pendingPasswordActions.set(pending.chatId, {
        conversion,
        fileId: pending.fileId,
        ext: pending.ext,
        target,
        mode: "button"
      });
      await bot.answerCallbackQuery(query.id, { text: "Send password in chat" });
      await bot.sendMessage(pending.chatId, `🔐 Send password to ${target} this PDF.`);
      return;
    }

    if (conversion.needsLanguage && !language) {
      pendingTranslationActions.set(pending.chatId, {
        conversion,
        fileId: pending.fileId,
        ext: pending.ext,
        target,
        mode: "button"
      });
      await bot.answerCallbackQuery(query.id, { text: "Send language code in chat" });
      await bot.sendMessage(pending.chatId, `🌍 Send language code (${Object.keys(TRANSLATION_LANGUAGES).join(", ")}).`);
      return;
    }

    if (conversion.needsWatermark) {
      pendingWatermarkActions.set(pending.chatId, {
        conversion,
        fileId: pending.fileId,
        ext: pending.ext,
        target,
        stage: "choose_mode"
      });
      await bot.answerCallbackQuery(query.id, { text: "Choose watermark mode" });
      await bot.sendMessage(pending.chatId, "Choose watermark mode:", { reply_markup: buildWatermarkModeKeyboard() });
      return;
    }

    await bot.answerCallbackQuery(query.id, { text: `Starting ${conversion.label}...` });

    await performConversion({
      chatId: pending.chatId,
      conversion,
      fileId: pending.fileId,
      ext: pending.ext,
      resolvedTarget: target,
      context: { password, pages, language }
    });
  });


  bot.on("text", async (msg) => {
    if (!(await ensureRegisteredOrAdmin(msg))) return;
    const chatId = msg.chat.id;
    const text = (msg.text || "").trim();
    if (!text || text.startsWith("/")) return;

    const pendingRename = pendingRenameActions.get(chatId);
    if (pendingRename) {
      const cleaned = sanitizeOutputBaseName(text);
      if (!cleaned) {
        await bot.sendMessage(chatId, "❌ Invalid filename. Use letters/numbers and avoid only symbols.");
        return;
      }

      pendingRename.requestedBaseName = cleaned;
      pendingRename.stage = "awaiting_confirm";
      pendingRenameActions.set(chatId, pendingRename);

      await bot.sendMessage(
        chatId,
        `Confirm new filename: *${cleaned}${pendingRename.outputExt}* ?`,
        { parse_mode: "Markdown", reply_markup: buildRenameConfirmKeyboard() }
      );
      return;
    }

    const pendingTranslation = pendingTranslationActions.get(chatId);
    if (pendingTranslation) {
      const language = text.toLowerCase();
      if (!TRANSLATION_LANGUAGES[language]) {
        await bot.sendMessage(chatId, `❌ Invalid language code. Use one of: ${Object.keys(TRANSLATION_LANGUAGES).join(", ")}`);
        return;
      }

      pendingTranslationActions.delete(chatId);
      await performConversion({
        chatId,
        conversion: pendingTranslation.conversion,
        fileId: pendingTranslation.fileId,
        ext: pendingTranslation.ext,
        resolvedTarget: pendingTranslation.target,
        context: { language }
      });
      return;
    }

    const pendingSplit = pendingSplitActions.get(chatId);
    if (pendingSplit && pendingSplit.stage === "pages") {
      const normalized = normalizeSplitPagesInput(pendingSplit, text);
      if (!normalized) {
        await bot.sendMessage(chatId, pendingSplit.mode === "simple"
          ? "❌ Invalid number. Send a page number greater than 1 (example: 4)."
          : "❌ Invalid format. Use grouped ranges like 1-10,35-40"
        );
        return;
      }

      pendingSplit.pagesInput = normalized;
      pendingSplit.stage = "confirm";
      pendingSplitActions.set(chatId, pendingSplit);
      await bot.sendMessage(
        chatId,
        buildSplitSelectionPreview(pendingSplit, normalized),
        { parse_mode: "Markdown", reply_markup: buildSplitConfirmKeyboard() }
      );
      return;
    }

    const pendingWm = pendingWatermarkActions.get(chatId);
    if (pendingWm?.stage === "awaiting_text") {
      if (text.length < 2 || text.length > 80) {
        await bot.sendMessage(chatId, "❌ Watermark text must be 2-80 characters.");
        return;
      }

      pendingWm.text = text;
      pendingWm.stage = "choose_size";
      pendingWatermarkActions.set(chatId, pendingWm);
      await bot.sendMessage(
        chatId,
        "Choose watermark text size first:",
        { reply_markup: buildWatermarkSizeKeyboard() }
      );
      await bot.sendMessage(chatId, "Suggestions: use short text like CONFIDENTIAL, DRAFT, INTERNAL, or your brand name.");
      return;
    }

    const pendingAction = pendingPasswordActions.get(chatId);
    if (!pendingAction) return;

    pendingPasswordActions.delete(chatId);

    await performConversion({
      chatId,
      conversion: pendingAction.conversion,
      fileId: pendingAction.fileId,
      ext: pendingAction.ext,
      resolvedTarget: pendingAction.target,
      context: { password: text }
    });
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
