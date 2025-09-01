// server/src/index.js
import 'dotenv/config';
import express from 'express';
import cors from 'cors';
import multer from 'multer';

import { getSchema, safeRun, ingestXlsxBuffer } from './db.js';
import { suggestSQL } from './llm.js';
import * as catalog from './catalog.js';

const app = express();
const upload = multer();

// Middlewares
app.use(cors());
app.use(express.json({ limit: '10mb' }));

/* ---------------- Health ---------------- */
app.get('/health', async (_req, res) => {
  res.json({
    ok: true,
    model: process.env.OLLAMA_MODEL || 'gpt-oss:20b',
  });
});

/* ---------------- Schéma (tel que tenu par db.js) ---------------- */
app.get('/schema', async (_req, res) => {
  try {
    const schema = getSchema();
    res.json({ tables: schema || {} });
  } catch (e) {
    res.status(500).json({ error: String(e?.message || e) });
  }
});

/* ---------------- Upload XLSX -> DuckDB (via db.js) ----------------
   form-data:
     - file: <xlsx>
     - table: <nom_souhaité> (optionnel)
-------------------------------------------------------------------- */
app.post('/upload', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'Aucun fichier' });
    const tableNameHint = String(req.body.table || req.file.originalname || 'table')
      .replace(/\.(xlsx|xls)$/i, '');
    const out = await ingestXlsxBuffer(req.file.buffer, { tableNameHint });
    res.json(out);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: String(e?.message || e) });
  }
});

/* ---------------- Chat analytique : LLM -> SQL -> DuckDB ----------------
   - Le LLM (Ollama API OpenAI-compatible) génère une requête SQL DuckDB (SELECT…)
   - On exécute la requête avec safeRun (bloque DDL/DML)
------------------------------------------------------------------------ */
app.post('/chat', async (req, res) => {
  try {
    const question = String(req.body?.message || '').trim();
    const history = Array.isArray(req.body?.history) ? req.body.history : [];

    // Schéma courant tenu par db.js (tables/colonnes/types)
    const schema = getSchema() || {};

    // 1) Demande au LLM une requête SQL DuckDB SÉLECT uniquement
    const sql = await suggestSQL({ schema, question, history });

    // 2) Exécution sécurisée (refuse CREATE/INSERT/UPDATE/DELETE/…)
    const rows = await safeRun(sql);

    res.json({ sql, rows });
  } catch (e) {
    console.error('[/chat] error:', e);
    res.status(500).json({ error: String(e?.message || e) });
  }
});

/* ---------------- Catalogue ---------------- */
app.post('/catalog/build', async (_req, res) => {
  try {
    const out = await catalog.buildCatalog();
    res.json(out);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: String(e?.message || e) });
  }
});

app.get('/catalog/summary', async (_req, res) => {
  try {
    const out = await catalog.getSummary();
    res.json(out);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: String(e?.message || e) });
  }
});

app.get('/catalog/profile', async (req, res) => {
  try {
    const { subcategory, supplier } = req.query;
    if (!subcategory || !supplier) {
      return res.status(400).json({ error: 'Paramètres requis: subcategory & supplier' });
    }
    const out = await catalog.getProfile(String(subcategory), String(supplier));
    res.json(out);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: String(e?.message || e) });
  }
});

app.get('/catalog/export', async (_req, res) => {
  try {
    const out = await catalog.exportCatalog();
    res.json(out);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: String(e?.message || e) });
  }
});

app.post('/catalog/import', async (req, res) => {
  try {
    const out = await catalog.importCatalog(req.body);
    res.json(out);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: String(e?.message || e) });
  }
});

/* ---------------- 404 JSON ---------------- */
app.use((req, res) => {
  res.status(404).json({ error: `Route non trouvée: ${req.method} ${req.originalUrl}` });
});

/* ---------------- Error handler JSON ---------------- */
app.use((err, _req, res, _next) => {
  console.error('[Unhandled]', err);
  res.status(500).json({ error: String(err) });
});

/* ---------------- Serveur ---------------- */
const PORT = Number(process.env.PORT || 8787);
app.listen(PORT, () => {
  console.log(`[server] listening on http://localhost:${PORT}`);
});
