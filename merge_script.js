/**
 * merge_script.js
 * Une datos de CSV y XLSX en el JSON de productos MongoDB.
 * - CSV: match por _id → Score.
 * - XLSX (hoja Farmer): match por nombre (Farmer - Farm) → Farm, Altitude, Temperature, Rainfall, Soil, Location, Farmer, Farmer History.
 * productDescription = fusión de ambas fuentes. Se validan las dos listas por documento.
 * Uso: node merge_script.js
 */

const fs = require("fs");
const path = require("path");
const XLSX = require("xlsx");
const { parse } = require("csv-parse/sync");

// ============ CONFIGURACIÓN DE RUTAS ============
const DIR = __dirname;
const JSON_INPUT = path.join(DIR, "cofinet_prod.products.json");
const CSV_INPUT = path.join(DIR, "cofinet_prod.products.csv");
const XLSX_INPUT = path.join(DIR, "Info_cards_database.xlsx");
const MERGED_OUTPUT = path.join(DIR, "cofinet_prod.products.merged.json");
const UNMATCHED_REPORT = path.join(DIR, "unmatched_report.txt");

// Correcciones de typo conocidos (nombre en hoja → como suele aparecer en producto)
const FARM_TYPO_FIXES = { "el despetar": "el despertar" };

// ============ NORMALIZACIÓN PARA MATCH ============
/**
 * Normaliza un string para comparación: minúsculas, sin tildes, espacios colapsados.
 */
function normalize(str) {
  if (str == null || str === "") return "";
  return String(str)
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function toVal(v) {
  if (v == null || v === "") return null;
  const s = String(v).trim();
  return s === "" ? null : s;
}

/** Carga una hoja del XLSX como array de objetos (primera fila = headers). */
function loadXlsxSheet(filePath, sheetName) {
  const content = fs.readFileSync(filePath);
  const workbook = XLSX.read(content, { type: "buffer" });
  if (!workbook.SheetNames.includes(sheetName)) return [];
  const sheet = workbook.Sheets[sheetName];
  const rows = XLSX.utils.sheet_to_json(sheet, { defval: "" });
  return rows.map((row) => {
    const cleaned = {};
    for (const [k, v] of Object.entries(row)) {
      const key = k && k.trim ? k.trim() : k;
      cleaned[key] = v != null ? String(v).trim() : "";
    }
    return cleaned;
  });
}

/** Lookup por "Farmer - Farm" (normalizado). Añade claves con typo corregido en el nombre de la finca. */
function buildFarmerLookup(rows) {
  const map = new Map();
  for (const row of rows) {
    const farmer = (row.Farmer != null ? String(row.Farmer).trim() : "") || "";
    const farm = (row.Farm != null ? String(row.Farm).trim() : "") || "";
    const key = normalize(farmer + " - " + farm);
    if (key && !map.has(key)) map.set(key, row);
    const farmNorm = normalize(farm);
    const typoFixed = FARM_TYPO_FIXES[farmNorm];
    if (typoFixed && typoFixed !== farmNorm) {
      const keyFixed = normalize(farmer + " - " + typoFixed);
      if (keyFixed && !map.has(keyFixed)) map.set(keyFixed, row);
    }
  }
  return map;
}

/** CSV: _id → { score }. Una fila por documento; match por _id para obtener el score correcto de cada uno. */
function loadCsvById(filePath) {
  if (!fs.existsSync(filePath)) return new Map();
  const content = fs.readFileSync(filePath, "utf-8");
  const rows = parse(content, { columns: true, skip_empty_lines: true, trim: true });
  const map = new Map();
  for (const row of rows) {
    const id = toVal(row._id);
    if (!id) continue;
    map.set(id, { score: toVal(row.score) });
  }
  return map;
}

/** productDescription: solo datos de finca (XLSX). El score va en la raíz del documento (CSV). */
function buildProductDescription(farmerRow) {
  return {
    farm: farmerRow ? toVal(farmerRow.Farm) : null,
    altitude: farmerRow ? toVal(farmerRow.Altitude) : null,
    temperature: farmerRow ? toVal(farmerRow.Temperature) : null,
    rainfall: farmerRow ? toVal(farmerRow.Rainfall) : null,
    soil: farmerRow ? toVal(farmerRow.Soil) : null,
    location: farmerRow ? toVal(farmerRow.Location) : null,
    farmer: farmerRow ? toVal(farmerRow.Farmer) : null,
  };
}

/** Extrae el _id string de un documento (soporta { $oid: "..." } o string). */
function getDocId(doc) {
  if (!doc._id) return null;
  const id = doc._id;
  return typeof id === "string" ? id.trim() : (id.$oid && id.$oid.trim ? id.$oid.trim() : null);
}

async function main() {
  try {
    // ============ PASO 1 — Leer JSON ============
    if (!fs.existsSync(JSON_INPUT)) {
      throw new Error(`No se encontró el archivo ${path.basename(JSON_INPUT)}`);
    }
    const jsonRaw = fs.readFileSync(JSON_INPUT, "utf-8");
    let jsonDocs;
    try {
      jsonDocs = JSON.parse(jsonRaw);
    } catch (e) {
      throw new Error("El archivo JSON no es válido: " + e.message);
    }
    if (!Array.isArray(jsonDocs)) {
      throw new Error("El JSON debe ser un array de documentos.");
    }

    // ============ PASO 2 — Cargar CSV (por _id → score) ============
    if (!fs.existsSync(CSV_INPUT)) {
      throw new Error(`No se encontró el archivo ${path.basename(CSV_INPUT)}`);
    }
    const csvById = loadCsvById(CSV_INPUT);

    // ============ PASO 3 — Cargar XLSX (hoja Farmer, por nombre) ============
    if (!fs.existsSync(XLSX_INPUT)) {
      throw new Error(`No se encontró el archivo ${path.basename(XLSX_INPUT)}`);
    }
    const farmerRows = loadXlsxSheet(XLSX_INPUT, "Farmer");
    const farmerByKey = buildFarmerLookup(farmerRows);

    // ============ PASO 4 — Unir ambas listas por documento ============
    const unmatchedFarmer = [];
    let farmerMatchCount = 0;
    let csvMatchCount = 0;
    const sampleMatches = [];

    for (const doc of jsonDocs) {
      const docId = getDocId(doc);
      const nameRaw = doc.name != null ? String(doc.name) : "";
      const nameNorm = normalize(nameRaw);

      const farmerRow = farmerByKey.get(nameNorm);
      const csvData = docId ? csvById.get(docId) : null;

      if (farmerRow) farmerMatchCount++;
      else if (nameRaw) unmatchedFarmer.push(nameRaw);
      if (csvData) csvMatchCount++;

      doc.productDescription = buildProductDescription(farmerRow);
      doc.score = csvData?.score ?? null;

      if (sampleMatches.length < 3 && (farmerRow || csvData)) {
        sampleMatches.push({
          name: nameRaw,
          pd: doc.productDescription,
          score: doc.score,
        });
      }
    }

    // ============ Resumen: validación de ambas listas ============
    console.log("\n🔍 Primeros 3 con datos (CSV + XLSX unidos):");
    for (const s of sampleMatches) {
      console.log(
        `  "${s.name}" → farm: ${s.pd.farm ?? "—"}, farmer: ${s.pd.farmer ?? "—"}, score (raíz): ${s.score ?? "—"}`
      );
    }

    fs.writeFileSync(MERGED_OUTPUT, JSON.stringify(jsonDocs, null, 2), "utf-8");
    if (unmatchedFarmer.length > 0) {
      fs.writeFileSync(UNMATCHED_REPORT, unmatchedFarmer.join("\n") + "\n", "utf-8");
    }

    console.log("\n✅ Merge completado (datos unidos de CSV + XLSX)");
    console.log("📦 Total documentos JSON:", jsonDocs.length);
    console.log("✅ Con match en CSV (score):", csvMatchCount);
    console.log("✅ Con match en XLSX (Farmer):", farmerMatchCount);
    console.log("⚠️  Sin match en XLSX:", unmatchedFarmer.length, unmatchedFarmer.length ? "(ver unmatched_report.txt)" : "");
    console.log("📄 Output guardado en:", path.basename(MERGED_OUTPUT));
  } catch (err) {
    console.error("\n❌ ERROR:", err.message);
    process.exit(1);
  }
}

main();
