// server/src/index.js
import express from "express";
import cors from "cors";
import bodyParser from "body-parser";
import multer from "multer";
import * as XLSX from "xlsx";

import { getSchema, runSQL } from "./db.js";
import {
  buildCatalog,
  getSummary,
  getProfile,
  exportCatalog,
  importCatalog
} from "./catalog.js";

const app = express();
const upload = multer({ storage: multer.memoryStorage() });

app.use(cors());
app.use(bodyParser.json({ limit: "25mb" }));
app.use(bodyParser.urlencoded({ extended: true }));

/* ---------------- Health ---------------- */
app.get("/health", async (req, res) => {
  try {
    res.json({
      ok: true,
      model: process.env.OLLAMA_MODEL || "gpt-oss:20b",
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

/* ---------------- Schema (avec fallback introspection) ---------------- */
app.get("/schema", async (req, res) => {
  try {
    let schema = getSchema();
    if (!schema || !Object.keys(schema).length) {
      // Fallback: introspection directe dans DuckDB
      const tables = await runSQL(`
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'main' AND table_type = 'BASE TABLE'
        ORDER BY table_name;
      `);
      const out = {};
      for (const t of tables) {
        const name = t.table_name;
        const cols = await runSQL(`PRAGMA table_info('${String(name).replace(/'/g,"''")}');`);
        out[name] = cols.map(c => ({ name: c.name, original: c.name }));
      }
      return res.json({ tables: out });
    }
    res.json({ tables: schema });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

/* ---------------- Upload XLSX -> DuckDB ----------------
   Front envoie:
     POST /upload
       form-data: file=<xlsx>, table=<nom_souhaité>
   Réponse JSON (succès):
     { table: "<nom_table>", rows: <n>, columns: ["col1", ...] }
-------------------------------------------------------- */
app.post("/upload", upload.single("file"), async (req, res) => {
  try {
    if (!req.file?.buffer) throw new Error("Fichier manquant.");
    const tableRaw = (req.body?.table || req.file.originalname || "xlsx")
      .replace(/\.[^.]+$/, "");
    const table = tableRaw.replace(/[^A-Za-z0-9_]+/g, "_").replace(/^(\d)/, "_$1");

    // lecture xlsx en mémoire
    const wb = XLSX.read(req.file.buffer, { type: "buffer" });
    const sheetName = wb.SheetNames?.[0];
    if (!sheetName) throw new Error("Feuille Excel introuvable.");
    const ws = wb.Sheets[sheetName];

    // JSON souple (toutes colonnes VARCHAR)
    const json = XLSX.utils.sheet_to_json(ws, { defval: null });
    if (!json.length) {
      await runSQL(`CREATE OR REPLACE TABLE "${table}" (col VARCHAR);`);
      return res.json({ table, rows: 0, columns: ["col"] });
    }

    // Colonnes
    const columns = Array.from(
      json.reduce((acc, row) => {
        Object.keys(row).forEach(k => acc.add(String(k)));
        return acc;
      }, new Set())
    );

    // Créer table
    const colsDDL = columns
      .map(c => `"${String(c).replace(/"/g, '""')}" VARCHAR`)
      .join(", ");
    await runSQL(`CREATE OR REPLACE TABLE "${table}" (${colsDDL});`);

    // Insertions (batch)
    const chunks = 100;
    for (let i = 0; i < json.length; i += chunks) {
      const part = json.slice(i, i + chunks);
      const valuesSQL = part
        .map(row => {
          const vals = columns.map(c => {
            const v = row?.[c];
            if (v === null || v === undefined) return "NULL";
            return `'${String(v).replace(/'/g, "''")}'`;
          });
          return `(${vals.join(",")})`;
        })
        .join(",\n");
      await runSQL(`INSERT INTO "${table}" (${columns.map(c => `"${c.replace(/"/g, '""')}"`).join(",")}) VALUES ${valuesSQL};`);
    }

    res.json({ table, rows: json.length, columns });
  } catch (e) {
    console.error("[/upload] error:", e);
    res.status(500).json({ error: String(e) });
  }
});

/* ---------------- Catalogue ---------------- */
app.post("/catalog/build", async (req, res) => {
  try {
    const out = await buildCatalog();
    res.json(out);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: String(e) });
  }
});

app.get("/catalog/summary", async (req, res) => {
  try {
    const out = await getSummary();
    res.json(out);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: String(e) });
  }
});

app.get("/catalog/profile", async (req, res) => {
  try {
    const subcategory = req.query.subcategory;
    const supplier = req.query.supplier;
    const out = await getProfile(subcategory, supplier);
    res.json(out);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: String(e) });
  }
});

app.get("/catalog/export", async (req, res) => {
  try {
    const out = await exportCatalog();
    res.json(out);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: String(e) });
  }
});

app.post("/catalog/import", async (req, res) => {
  try {
    const out = await importCatalog(req.body);
    res.json(out);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: String(e) });
  }
});

/* ---------------- Chat (optionnel) ---------------- */
app.post("/chat", async (req, res) => {
  try {
    // À brancher à ta logique existante si besoin
    res.json({ message: "Chat backend non implémenté ici." });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

/* ---------------- 404 JSON par défaut ---------------- */
app.use((req, res) => {
  res.status(404).json({ error: `Route non trouvée: ${req.method} ${req.originalUrl}` });
});

/* ---------------- Error handler JSON ---------------- */
app.use((err, req, res, next) => {
  console.error("[Unhandled]", err);
  res.status(500).json({ error: String(err) });
});

const PORT = process.env.PORT || 8787;
app.listen(PORT, () => {
  console.log(`[server] listening on http://localhost:${PORT}`);
});
