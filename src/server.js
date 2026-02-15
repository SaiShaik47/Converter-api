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

async function downloadTelegramFile(bot, fileId, workDir) {
  const file = await bot.getFile(fileId);
  const filePath = file?.file_path;
  if (!filePath) throw new Error("Unable to locate the file on Telegram servers.");

  const localPath = path.join(
    workDir,
    `${Date.now()}_${crypto.randomBytes(4).toString("hex")}_${path.basename(filePath)}`
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

function commandAvailable(cmd) {
  return new Promise((resolve) => {
    const probe = spawn("which", [cmd]);
    probe.on("error", () => resolve(false));
    probe.on("close", (code) => resolve(code === 0));
  });
}

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
  const lines = parsed.text.split(/\r?\n/);
  const doc = new Document({
    sections: [
      {
        properties: {},
        children: lines.map((line) => new Paragraph(line))
      }
    ]
  });
  const buffer = await Packer.toBuffer(doc);
  await fs.writeFile(docxPath, buffer);
}

async function renderPdfToPngs(pdfPath, outDir) {
  const data = new Uint8Array(await fs.readFile(pdfPath));
  const loadingTask = pdfjsLib.getDocument({ data });
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

// Excel -> PDF (LibreOffice with JS fallback)
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
      // Fall through to JS renderer
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

// PDF -> CSV (Tabula)
async function tabulaPdfToCsv(pdfPath, outCsvPath, pages = "all") {
  const jar = process.env.TABULA_JAR || "/opt/tabula/tabula.jar";
  const args = ["-jar", jar, "-p", pages, "-f", "CSV", "-o", outCsvPath, pdfPath];

  if (await commandAvailable("java")) {
    try {
      await fs.access(jar);
      await runCommand("java", args);
      return;
    } catch {
      // Fall through to JS fallback
    }
  }

  const raw = await fs.readFile(pdfPath);
  const parsed = await pdfParse(raw);
  const lines = parsed.text.split(/\r?\n/).map((line) => line.trim()).filter(Boolean);
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

  if (target === "pdf" && path.extname(inputPath).toLowerCase() === ".txt") {
    const text = await fs.readFile(inputPath, "utf8");
    const lines = text.split(/\r?\n/);
    await writeTextPdf(lines, outPath);
    return outPath;
  }

  if (target === "docx" && path.extname(inputPath).toLowerCase() === ".pdf") {
    await pdfToDocx(inputPath, outPath);
    return outPath;
  }

  throw new Error("soffice is not installed or not in PATH");
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
  if (await commandAvailable("pdftotext")) {
    try {
      await runCommand("pdftotext", [pdfPath, txtPath]);
      return;
    } catch {
      // Fall through to JS fallback
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

  if (pngPaths.length === 0) {
    throw new Error("No images were generated from the PDF.");
  }

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

function parsePageRangesInput(pagesValue, totalPages) {
  if (!pagesValue || pagesValue.toLowerCase() === "all") {
    return Array.from({ length: totalPages }, (_, idx) => [idx]);
  }

  const ranges = [];
  const parts = pagesValue.split(",").map(part => part.trim()).filter(Boolean);

  for (const part of parts) {
    const rangeMatch = part.match(/^(\d+)\s*-\s*(\d+)$/);
    if (rangeMatch) {
      const start = Math.max(1, Number(rangeMatch[1]));
      const end = Math.min(totalPages, Number(rangeMatch[2]));
      if (Number.isNaN(start) || Number.isNaN(end)) continue;
      const pages = [];
      for (let i = Math.min(start, end); i <= Math.max(start, end); i += 1) {
        pages.push(i - 1);
      }
      if (pages.length) ranges.push(pages);
    } else {
      const pageNum = Number(part);
      if (!Number.isNaN(pageNum) && pageNum >= 1 && pageNum <= totalPages) {
        ranges.push([pageNum - 1]);
      }
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
  const mergedBytes = await merged.save();
  await fs.writeFile(outputPath, mergedBytes);
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
    const outBytes = await doc.save();
    await fs.writeFile(outPath, outBytes);
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
  const outBytes = await doc.save({ useObjectStreams: false });
  await fs.writeFile(outputPath, outBytes);
}

async function protectPdf(inputPath, outputPath, password) {
  if (!password) throw new Error("Password is required to protect the PDF.");
  if (!(await commandAvailable("qpdf"))) {
    throw new Error("qpdf is required for PDF protection but is not installed.");
  }

  await runCommand("qpdf", [
    "--encrypt",
    password,
    password,
    "256",
    "--",
    inputPath,
    outputPath
  ]);
}

async function unlockPdf(inputPath, outputPath, password) {
  if (!password) throw new Error("Password is required to unlock the PDF.");
  if (!(await commandAvailable("qpdf"))) {
    throw new Error("qpdf is required for PDF unlock but is not installed.");
  }

  await runCommand("qpdf", ["--password", password, "--decrypt", "--", inputPath, outputPath]);
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
      pdf_unlock: "POST /pdf-unlock (form-data key: file, field: password)"
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
  const inputPath = req.file?.path;
  if (!inputPath) return res.status(400).json({ ok: false, error: "Upload file using key: file" });

  const ext = path.extname(req.file.originalname || "").toLowerCase();
  if (![".xlsx", ".xls"].includes(ext)) {
    await safeUnlink(inputPath);
    return res.status(400).json({ ok: false, error: "Only .xlsx or .xls allowed" });
  }

  const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "x2p-"));
  let cleaned = false;
  const cleanup = async () => {
    if (cleaned) return;
    cleaned = true;
    await safeUnlink(inputPath);
    await safeRmDir(workDir);
  };

  try {
    const pdfPath = await convertExcelToPdf(inputPath, workDir);
    await fs.access(pdfPath);

    res.setHeader("Content-Type", "application/pdf");
    res.setHeader("Content-Disposition", `attachment; filename="output.pdf"`);
    createReadStream(pdfPath).pipe(res);

    res.on("finish", cleanup);
    res.on("close", cleanup);
  } catch (e) {
    await cleanup();
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
  let cleaned = false;
  const cleanup = async () => {
    if (cleaned) return;
    cleaned = true;
    await safeUnlink(inputPath);
    await safeRmDir(workDir);
  };

  try {
    await tabulaPdfToCsv(inputPath, outCsv, pages);
    await csvToXlsx(outCsv, outXlsx);

    res.setHeader(
      "Content-Type",
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    );
    res.setHeader("Content-Disposition", `attachment; filename="output.xlsx"`);
    createReadStream(outXlsx).pipe(res);

    res.on("finish", cleanup);
    res.on("close", cleanup);
  } catch (e) {
    await cleanup();
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
  const inputPath = req.file?.path;
  if (!inputPath) return res.status(400).json({ ok: false, error: "Upload file using key: file" });

  const ext = path.extname(req.file.originalname || "").toLowerCase();
  if (ext !== ".pdf") {
    await safeUnlink(inputPath);
    return res.status(400).json({ ok: false, error: "Only .pdf allowed" });
  }

  const pagesValue = (req.query.pages || "all").toString();
  const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "psplit-"));
  let cleaned = false;
  const cleanup = async () => {
    if (cleaned) return;
    cleaned = true;
    await safeUnlink(inputPath);
    await safeRmDir(workDir);
  };

  try {
    const outputFiles = await splitPdf(inputPath, workDir, pagesValue);
    const outZip = path.join(workDir, "split-pages.zip");
    await createZip(outputFiles, outZip);
    res.setHeader("Content-Type", "application/zip");
    res.setHeader("Content-Disposition", `attachment; filename="split-pages.zip"`);
    createReadStream(outZip).pipe(res);
    res.on("finish", cleanup);
    res.on("close", cleanup);
  } catch (e) {
    await cleanup();
    res.status(500).json({ ok: false, error: e.message || "Split failed" });
  }
});

app.post("/pdf-compress", upload.single("file"), async (req, res) => {
  const inputPath = req.file?.path;
  if (!inputPath) return res.status(400).json({ ok: false, error: "Upload file using key: file" });

  const ext = path.extname(req.file.originalname || "").toLowerCase();
  if (ext !== ".pdf") {
    await safeUnlink(inputPath);
    return res.status(400).json({ ok: false, error: "Only .pdf allowed" });
  }

  const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "pcomp-"));
  let cleaned = false;
  const cleanup = async () => {
    if (cleaned) return;
    cleaned = true;
    await safeUnlink(inputPath);
    await safeRmDir(workDir);
  };

  try {
    const outPath = path.join(workDir, "compressed.pdf");
    await compressPdf(inputPath, outPath);
    res.setHeader("Content-Type", "application/pdf");
    res.setHeader("Content-Disposition", `attachment; filename="compressed.pdf"`);
    createReadStream(outPath).pipe(res);
    res.on("finish", cleanup);
    res.on("close", cleanup);
  } catch (e) {
    await cleanup();
    res.status(500).json({ ok: false, error: e.message || "Compression failed" });
  }
});

app.post("/pdf-protect", upload.single("file"), async (req, res) => {
  const inputPath = req.file?.path;
  if (!inputPath) return res.status(400).json({ ok: false, error: "Upload file using key: file" });

  const ext = path.extname(req.file.originalname || "").toLowerCase();
  if (ext !== ".pdf") {
    await safeUnlink(inputPath);
    return res.status(400).json({ ok: false, error: "Only .pdf allowed" });
  }

  const password = (req.body?.password || "").toString();
  const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "pprot-"));
  let cleaned = false;
  const cleanup = async () => {
    if (cleaned) return;
    cleaned = true;
    await safeUnlink(inputPath);
    await safeRmDir(workDir);
  };

  try {
    const outPath = path.join(workDir, "protected.pdf");
    await protectPdf(inputPath, outPath, password);
    res.setHeader("Content-Type", "application/pdf");
    res.setHeader("Content-Disposition", `attachment; filename="protected.pdf"`);
    createReadStream(outPath).pipe(res);
    res.on("finish", cleanup);
    res.on("close", cleanup);
  } catch (e) {
    await cleanup();
    res.status(500).json({ ok: false, error: e.message || "Protection failed" });
  }
});

app.post("/pdf-unlock", upload.single("file"), async (req, res) => {
  const inputPath = req.file?.path;
  if (!inputPath) return res.status(400).json({ ok: false, error: "Upload file using key: file" });

  const ext = path.extname(req.file.originalname || "").toLowerCase();
  if (ext !== ".pdf") {
    await safeUnlink(inputPath);
    return res.status(400).json({ ok: false, error: "Only .pdf allowed" });
  }

  const password = (req.body?.password || "").toString();
  const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "punlock-"));
  let cleaned = false;
  const cleanup = async () => {
    if (cleaned) return;
    cleaned = true;
    await safeUnlink(inputPath);
    await safeRmDir(workDir);
  };

  try {
    const outPath = path.join(workDir, "unlocked.pdf");
    await unlockPdf(inputPath, outPath, password);
    res.setHeader("Content-Type", "application/pdf");
    res.setHeader("Content-Disposition", `attachment; filename="unlocked.pdf"`);
    createReadStream(outPath).pipe(res);
    res.on("finish", cleanup);
    res.on("close", cleanup);
  } catch (e) {
    await cleanup();
    res.status(500).json({ ok: false, error: e.message || "Unlock failed" });
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

Send me a file and I’ll auto-detect every action you can do with it.
You’ll always get smart buttons, and captions still work for quick commands.

Examples:
• Upload invoice.pdf (tap Split / Compress / Protect / Unlock)
• Upload report.pdf with caption "to docx"
• Upload 2+ PDFs as a media group with caption "merge"

Limits:
• Max size: ${MAX_MB} MB

Commands:
/start    - welcome
/help     - how to use
/status   - check bot
/merge    - merge multiple PDFs
/split    - split a PDF
/compress - compress a PDF
/protect  - password-protect a PDF
/unlock   - unlock a PDF`;

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
    const match = caption.match(/(?:pages|split)[:=\s]+([0-9,\s-]+)/i);
    return match ? match[1].replace(/\s+/g, "") : null;
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
    },
    ".bmp": {
      pdf: { label: "Image → PDF", outputExt: ".pdf", convert: async (input, dir) => {
        const outPath = path.join(dir, "output.pdf");
        await imageToPdf(input, outPath);
        return outPath;
      }}
    }
  };

  const pendingConversions = new Map();
  const pendingMediaMerges = new Map();

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

  const helpText =
`🧠 How to use

1) Send a file (captions are optional).
   • I’ll auto-detect the type and show buttons for every valid action.
   • Tap a button to run the conversion.

2) Optional captions (auto-detected too):
   • "to pdf", "to docx", "to xlsx", "to txt", "to json", "to csv"
   • "to compress", "to split pages=1-3,5"
   • "to protect password=1234" or "to unlock pass=1234"
   • "merge" (send 2+ PDFs as one media group)

3) PDF quick actions:
   • /merge    → merge multiple PDFs (send as a media group)
   • /split    → split a PDF into pages (ZIP)
   • /compress → compress a PDF
   • /protect  → password-protect a PDF
   • /unlock   → unlock a PDF

4) Supported conversions:
${formatSupportedConversions(telegramConversions)}

5) File limit:
   • Max ${MAX_MB} MB

Pro tip:
• You can run multiple actions on the same file by tapping more buttons.`;

  bot.setMyCommands([
    { command: "start", description: "Welcome message" },
    { command: "help", description: "How to use the bot" },
    { command: "status", description: "Check bot status" },
    { command: "merge", description: "Merge multiple PDFs" },
    { command: "split", description: "Split a PDF into pages" },
    { command: "compress", description: "Compress a PDF" },
    { command: "protect", description: "Password-protect a PDF" },
    { command: "unlock", description: "Unlock a PDF" }
  ]);

  bot.onText(/\/start/, (msg) => bot.sendMessage(msg.chat.id, startText));
  bot.onText(/\/help/, (msg) => bot.sendMessage(msg.chat.id, helpText));
  bot.onText(/\/status/, (msg) => bot.sendMessage(msg.chat.id, "✅ Bot is running and ready. Send a file."));
  bot.onText(/\/merge/, (msg) => {
    bot.sendMessage(
      msg.chat.id,
      "🧩 Merge PDFs:\nSend 2+ PDFs as a single media group and add the caption \"merge\".\nI will combine them into one PDF."
    );
  });
  bot.onText(/\/split/, (msg) => {
    bot.sendMessage(msg.chat.id, "✂️ Split PDF:\nSend a PDF and tap “PDF → Split (ZIP)” or add caption \"to split pages=1-3,5\".");
  });
  bot.onText(/\/compress/, (msg) => {
    bot.sendMessage(msg.chat.id, "🗜️ Compress PDF:\nSend a PDF and tap “PDF → Compressed PDF” or add caption \"to compress\".");
  });
  bot.onText(/\/protect/, (msg) => {
    bot.sendMessage(msg.chat.id, "🔒 Protect PDF:\nSend a PDF with caption \"to protect password=1234\" or tap the Protect button.");
  });
  bot.onText(/\/unlock/, (msg) => {
    bot.sendMessage(msg.chat.id, "🔓 Unlock PDF:\nSend a PDF with caption \"to unlock pass=1234\" or tap the Unlock button.");
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
          const localPath = await downloadTelegramFile(bot, fileId, workDir);
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

        await bot.sendDocument(pending.chatId, outputPath, {
          caption: "✅ PDF merge"
        });
      } catch (e) {
        await bot.sendMessage(pending.chatId, `❌ Merge failed.\nReason: ${e.message}`);
      } finally {
        await safeUnlink(outputPath);
        await safeRmDir(workDir);
      }
    }, 1500);
  }

  async function performConversion({ chatId, conversion, fileId, ext, resolvedTarget, context = {} }) {
    const status = await bot.sendMessage(chatId, `⏳ Received: *${conversion.label}*\nDownloading...`, {
      parse_mode: "Markdown"
    });

    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "tg-"));
    const outputPath = path.join(os.tmpdir(), randName(conversion.outputExt));

    try {
      const downloadedPath = await downloadTelegramFile(bot, fileId, workDir);

      await bot.editMessageText(`⚙️ Converting: *${conversion.label}*\nPlease wait...`, {
        chat_id: chatId,
        message_id: status.message_id,
        parse_mode: "Markdown"
      });

      const convertedPath = await conversion.convert(downloadedPath, workDir, context);
      await fs.copyFile(convertedPath, outputPath);

      await bot.editMessageText(`✅ Done: *${conversion.label}*\nUploading result...`, {
        chat_id: chatId,
        message_id: status.message_id,
        parse_mode: "Markdown"
      });

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
  }

  bot.on("document", async (msg) => {
    const chatId = msg.chat.id;
    const doc = msg.document;

    const fileName = doc.file_name || "file";
    const ext = path.extname(fileName).toLowerCase();
    const target = parseTarget(msg.caption);
    const password = parsePassword(msg.caption);
    const pages = parsePages(msg.caption);

    // Telegram gives file size too
    const size = doc.file_size || 0;
    if (size > MAX_BYTES) {
      return bot.sendMessage(chatId, `❌ File too large. Max allowed is ${MAX_MB} MB.`);
    }

    if (ext === ".pdf" && msg.media_group_id) {
      const mediaGroupId = msg.media_group_id;
      const existingPending = pendingMediaMerges.get(mediaGroupId);
      const isMergeRequest = target === "merge" || existingPending?.isMergeRequest;

      if (isMergeRequest) {
        const pending = existingPending || {
          chatId,
          fileIds: [],
          timer: null,
          isMergeRequest: false
        };

        pending.isMergeRequest = true;
        pending.fileIds.push(doc.file_id);
        pendingMediaMerges.set(mediaGroupId, pending);
        scheduleMediaMerge(mediaGroupId);
        return;
      }
    }

    const options = telegramConversions[ext] || {};
    const supportedTargets = Object.keys(options);
    if (supportedTargets.length === 0) {
      return bot.sendMessage(chatId, "❌ Unsupported file type. Send a supported file (PDF, DOCX, PPTX, XLSX, CSV, JSON, TXT, JPG/PNG/BMP).");
    }

    const token = crypto.randomBytes(6).toString("hex");
    pendingConversions.set(token, { fileId: doc.file_id, ext, fileName, chatId });
    setTimeout(() => pendingConversions.delete(token), 10 * 60 * 1000);

    await bot.sendMessage(chatId, `✅ Detected *${fileName}*.\nChoose an action:`, {
      parse_mode: "Markdown",
      reply_markup: buildTargetKeyboard(options, token)
    });

    if (!target) {
      return;
    }

    const conversion = options[target];
    if (!conversion) {
      return bot.sendMessage(
        chatId,
        `❌ Unsupported target "${target}". Pick from the buttons above.`
      );
    }

    if (conversion.needsPassword && !password) {
      return bot.sendMessage(chatId, "🔐 This action requires a password. Add a caption like: 'protect password=1234' or 'unlock pass=1234'.");
    }

    await performConversion({
      chatId,
      conversion,
      fileId: doc.file_id,
      ext,
      resolvedTarget: target,
      context: { password, pages }
    });
  });

  bot.on("callback_query", async (query) => {
    const data = query.data || "";
    if (!data.startsWith("conv:")) return;

    const [, token, target] = data.split(":");
    const pending = pendingConversions.get(token);
    if (!pending) {
      await bot.answerCallbackQuery(query.id, { text: "This request expired. Please send the file again." });
      return;
    }

    const options = telegramConversions[pending.ext] || {};
    const conversion = options[target];
    if (!conversion) {
      await bot.answerCallbackQuery(query.id, { text: "That conversion is no longer available." });
      return;
    }

    pendingConversions.delete(token);
    await bot.answerCallbackQuery(query.id, { text: `Starting ${conversion.label}...` });
    const caption = query.message?.caption || "";
    const password = parsePassword(caption);
    const pages = parsePages(caption);

    if (conversion.needsPassword && !password) {
      await bot.sendMessage(pending.chatId, "🔐 This action requires a password. Re-send the PDF with caption: 'protect password=1234' or 'unlock pass=1234'.");
      return;
    }

    await performConversion({
      chatId: pending.chatId,
      conversion,
      fileId: pending.fileId,
      ext: pending.ext,
      resolvedTarget: target,
      context: { password, pages }
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
