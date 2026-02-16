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
import { PDFDocument } from "pdf-lib";
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
  const wb = XLSX.readFile(inputPath);
  const first = wb.SheetNames[0];
  const rows = XLSX.utils.sheet_to_json(wb.Sheets[first], { header: 1, defval: "" });
  const lines = rows.map((row) => row.join("  |  "));
  await writeTextPdf(lines.length ? lines : ["(empty sheet)"], outputPath);
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
      await fs.access(outPath);
      return outPath;
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
    await runCommand("ocrmypdf", ["--skip-text", "--optimize", "1", inputPath, outputPath]);
    return;
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
      pdf_ocr: "POST /pdf-ocr (form-data key: file)",
      scan_to_pdf: "POST /scan-to-pdf (form-data key: files[])",
      pdf_translate: "POST /pdf-translate?lang=es (form-data key: file)",
      available_translate_languages: TRANSLATION_LANGUAGES
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

  // ✅ Prevent "409 terminated by other getUpdates request" staying broken
  bot.on("polling_error", async (err) => {
    const msg = String(err?.message || "");
    console.log("polling_error:", msg);

    if (msg.includes("409") || msg.includes("terminated by other getUpdates request")) {
      try {
        await bot.stopPolling();
      } catch {}
      setTimeout(() => {
        try { bot.startPolling(); } catch {}
      }, 2500);
    }
  });

  const startText =
`👋 Welcome to File Converter Bot

Send a file and I’ll auto-detect everything you can do with it.
You’ll get smart buttons for every supported tool.

✨ Quick Examples:
• Send invoice.pdf → tap OCR / Split / Compress / Protect / Unlock
• Send report.pdf with caption: to docx
• Send 2+ PDFs together with caption: merge
• Tap Protect/Unlock → send password when asked
• Scan mode: /scanpdf → send images → /done

📦 Limits:
• Max file size: ${MAX_MB} MB

📌 Commands:
/start    - welcome
/help     - how to use
/status   - bot status
/merge    - merge PDFs (media-group OR step mode)
/done     - finish merge (step mode)
/cancel   - cancel merge mode
/split    - split PDF pages
/compress - compress PDF
/ocr      - OCR scanned PDF
/translate- translate PDF language
/scanpdf  - merge images into one PDF
/protect  - protect PDF (password)
/unlock   - unlock PDF (password)
`;

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

  async function startSplitFlow(chatId, fileId, ext) {
    pendingSplitActions.set(chatId, { fileId, ext, mode: null, pagesInput: null, stage: "mode" });
    await bot.sendMessage(
      chatId,
      "✂️ Split setup started.\nChoose split mode:\n• Simple split: split once from a page number\n• Grouped split: use ranges like 1-10,35-40",
      { reply_markup: buildSplitModeKeyboard() }
    );
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

  const helpText =
`🧠 HOW TO USE

1) Send a file (caption optional)
• I detect the type & show buttons
• Tap a button to run

2) Captions (optional)
Convert:
• to pdf / to docx / to xlsx / to txt / to csv / to json
PDF tools:
• to compress
• to ocr (requires ocrmypdf or tesseract installed on server)
• to split pages=1-3,5 OR pages=from:4 OR pages=every:2
• to translate lang=es
• to protect (then send password when asked)
• to unlock (then send password when asked)

3) Merge PDFs (2 ways)
✅ A) Media group:
• Send 2+ PDFs together
• Caption: merge

✅ B) Step mode:
/merge → send PDFs one by one → /done

4) Scan to PDF
• /scanpdf → send images one by one → /done

5) Supported types
${formatSupportedConversions(telegramConversions)}

Languages for translate: ${Object.entries(TRANSLATION_LANGUAGES).map(([k, v]) => `${k}(${v})`).join(", ")}\n\nLimit: ${MAX_MB} MB`;

  bot.setMyCommands([
    { command: "start", description: "Welcome" },
    { command: "help", description: "How to use" },
    { command: "status", description: "Bot status" },
    { command: "merge", description: "Merge PDFs (media group or step mode)" },
    { command: "done", description: "Finish merge (step mode)" },
    { command: "cancel", description: "Cancel merge mode" },
    { command: "split", description: "Split a PDF into pages" },
    { command: "compress", description: "Compress a PDF" },
    { command: "ocr", description: "OCR a scanned PDF" },
    { command: "translate", description: "Translate a PDF" },
    { command: "scanpdf", description: "Build one PDF from images" },
    { command: "protect", description: "Password-protect a PDF" },
    { command: "unlock", description: "Unlock a PDF" }
  ]);

  bot.onText(/\/start/, (msg) => bot.sendMessage(msg.chat.id, startText));
  bot.onText(/\/help/, (msg) => bot.sendMessage(msg.chat.id, helpText));
  bot.onText(/\/status/, (msg) => bot.sendMessage(msg.chat.id, "✅ Bot is running. Send a file."));
  bot.onText(/\/split/, (msg) => {
    pendingCommandFileActions.set(msg.chat.id, { target: "split" });
    bot.sendMessage(
      msg.chat.id,
      "✂️ Split mode: please send a PDF first. Then I'll ask simple/grouped split and ask for confirmation."
    );
  });
  bot.onText(/\/compress/, (msg) => bot.sendMessage(msg.chat.id, "🗜️ Compress: Send a PDF and tap “Compressed PDF” or caption: to compress"));
  bot.onText(/\/ocr/, (msg) => bot.sendMessage(msg.chat.id, "🔎 OCR: Send a scanned PDF and tap “PDF → OCR (Searchable)”. OCR requires ocrmypdf or tesseract installed on server."));
  bot.onText(/\/translate/, (msg) => {
    pendingCommandFileActions.set(msg.chat.id, { target: "translate" });
    bot.sendMessage(msg.chat.id, "🌍 Translate mode: send a PDF first. Then send a language code like en, es, fr, de, ar, hi.");
  });
  bot.onText(/\/scanpdf/, (msg) => {
    scanToPdfSessions.set(msg.chat.id, { imageFileIds: [], startedAt: Date.now() });
    bot.sendMessage(msg.chat.id, "🖼️ Scan to PDF mode ON.\nSend images one by one.\nWhen finished, type /done\nTo cancel: /cancel");
  });
  bot.onText(/\/protect/, (msg) => {
    pendingCommandFileActions.set(msg.chat.id, { target: "protect" });
    bot.sendMessage(msg.chat.id, "🔒 Protect mode: Please send the PDF file first.");
  });
  bot.onText(/\/unlock/, (msg) => {
    pendingCommandFileActions.set(msg.chat.id, { target: "unlock" });
    bot.sendMessage(msg.chat.id, "🔓 Unlock mode: Please send the PDF file first.");
  });

  bot.onText(/\/merge/, (msg) => {
    mergeSessions.set(msg.chat.id, { fileIds: [], startedAt: Date.now() });
    bot.sendMessage(
      msg.chat.id,
      "🧩 Merge mode ON.\nSend PDFs one by one now.\nWhen finished, type /done\nTo cancel: /cancel"
    );
  });

  bot.onText(/\/cancel/, (msg) => {
    mergeSessions.delete(msg.chat.id);
    scanToPdfSessions.delete(msg.chat.id);
    pendingSplitActions.delete(msg.chat.id);
    bot.sendMessage(msg.chat.id, "❌ Active batch mode cancelled.");
  });

  bot.onText(/\/done/, async (msg) => {
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
    const outputPath = path.join(os.tmpdir(), randName(conversion.outputExt));

    try {
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

      await bot.sendDocument(chatId, outputPath, { caption: `✅ ${conversion.label}` });

      if (resolvedTarget === "xlsx" && ext === ".pdf") {
        await bot.sendMessage(chatId, "Tip: Scanned PDFs may not extract tables well. Text-based PDFs work best.");
      }
    } catch (e) {
      await bot.sendMessage(chatId, `❌ Failed.\nReason: ${e.message}`);
    } finally {
      await safeUnlink(outputPath);
      await safeRmDir(workDir);
    }
  }

  bot.on("photo", async (msg) => {
    const chatId = msg.chat.id;
    const scanSession = scanToPdfSessions.get(chatId);
    if (!scanSession) return;

    const photos = msg.photo || [];
    const best = photos[photos.length - 1];
    if (!best?.file_id) return;

    scanSession.imageFileIds.push(best.file_id);
    scanToPdfSessions.set(chatId, scanSession);
    await bot.sendMessage(chatId, `✅ Added image
Total images: ${scanSession.imageFileIds.length}
Send more or type /done`);
  });

  bot.on("document", async (msg) => {
    const chatId = msg.chat.id;
    const doc = msg.document;
    if (!doc) return;

    const fileName = doc.file_name || "file";
    const ext = path.extname(fileName).toLowerCase();

    const size = doc.file_size || 0;
    if (size > MAX_BYTES) return bot.sendMessage(chatId, `❌ File too large. Max ${MAX_MB} MB.`);

    const scanSession = scanToPdfSessions.get(chatId);
    const scanImageExts = [".jpg", ".jpeg", ".png", ".tiff", ".bmp", ".webp", ".gif"];
    if (scanSession && scanImageExts.includes(ext)) {
      scanSession.imageFileIds.push(doc.file_id);
      scanToPdfSessions.set(chatId, scanSession);
      return bot.sendMessage(chatId, `✅ Added image: ${fileName}
Total images: ${scanSession.imageFileIds.length}
Send more or type /done`);
    }

    const pendingFileAction = pendingCommandFileActions.get(chatId);
    if (pendingFileAction) {
      if (ext !== ".pdf") return bot.sendMessage(chatId, "❌ Please send a PDF file for this action.");

      const conversion = telegramConversions[".pdf"]?.[pendingFileAction.target];
      if (!conversion) {
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

      await performConversion({
        chatId,
        conversion,
        fileId: doc.file_id,
        ext,
        resolvedTarget: pendingFileAction.target
      });
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
        "❌ Unsupported file type.\nSend: PDF, DOCX, PPTX, XLSX, CSV, JSON, TXT, and common image files."
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
          await bot.sendMessage(chatId, "Send the page number from which to split (example: 4). This creates 1-3 and 4-end.");
        } else {
          await bot.sendMessage(chatId, "Send grouped ranges like: 1-10,35-40");
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
    const chatId = msg.chat.id;
    const text = (msg.text || "").trim();
    if (!text || text.startsWith("/")) return;

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
        `Confirm split with: ${normalized} ?`,
        { reply_markup: buildSplitConfirmKeyboard() }
      );
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
