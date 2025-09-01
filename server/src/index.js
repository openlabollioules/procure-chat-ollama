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

// ✅ Corrige l'erreur BigInt -> JSON
app.set('json replacer', (_k, v) => (typeof v === 'bigint' ? Number(v) : v));

// Health
app.get('/health', async (_req, res) => {
  res.json({
    ok: true,
    model: process.env.OLLAMA_MODEL || 'gpt-oss:20b',
  });
});

// Ingestion Excel (données réelles ou fichiers d'intitulés)
app.post('/upload', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'Aucun fichier' });
    const tableNameHint = String(req.body.table || req.file.originalname || 'table')
      .replace(/\.(xlsx|xls)$/i, '');
    const out = await ingestXlsxBuffer(req.file.buffer, { tableNameHint });
    res.json(out);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: String(e.message || e) });
  }
});

// Schéma courant
app.get('/schema', (_req, res) => {
  res.json({ tables: getSchema() });
});

// Chat -> SQL -> Exécution
app.post('/chat', async (req, res) => {
  try {
    const { message, history = [] } = req.body || {};
    if (!message) return res.status(400).json({ error: 'message requis' });

    const schema = getSchema();
    const sql = await suggestSQL({ schema, question: message, history });
    const rows = await safeRun(sql);

    res.json({ sql, rows });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: String(e.message || e) });
  }
});

// ====== CATALOGUE (taxonomie, profils de décaissement) ======
app.post('/catalog/build', async (_req, res) => {
  try {
    const out = await catalog.buildCatalog();
    res.json({ ok: true, ...out });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: String(e.message || e) });
  }
});

app.get('/catalog/summary', async (_req, res) => {
  try {
    const out = await catalog.getSummary();
    res.json(out);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: String(e.message || e) });
  }
});

app.get('/catalog/profile', async (req, res) => {
  try {
    const { subcategory, supplier } = req.query || {};
    if (!subcategory || !supplier) {
      return res.status(400).json({ error: 'subcategory & supplier requis.' });
    }
    const out = await catalog.getProfile(String(subcategory), String(supplier));
    res.json(out);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: String(e.message || e) });
  }
});

// Serveur
const PORT = Number(process.env.PORT || 8787);
app.listen(PORT, () => {
  console.log(`[server] listening on http://localhost:${PORT}`);
});
