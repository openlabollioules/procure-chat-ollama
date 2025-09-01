
import 'dotenv/config';
import express from 'express';
import cors from 'cors';
import multer from 'multer';
import { getSchema, safeRun, ingestXlsxBuffer } from './db.js';
import { suggestSQL } from './llm.js';

const app = express();
const upload = multer();

app.use(cors());
app.use(express.json({ limit: '2mb' }));

app.get('/health', (req, res) => {
  res.json({ ok: true, model: process.env.OLLAMA_MODEL || 'gpt-oss:20b' });
});

// Retourner le schéma courant
app.get('/schema', (req, res) => {
  res.json({ tables: getSchema() });
});

// Upload d'un fichier Excel et création table
app.post('/upload', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'Aucun fichier' });
    const tableHint = (req.body.table || req.body.hint || '').trim() || req.file.originalname.split('.')[0];
    const out = await ingestXlsxBuffer(req.file.buffer, { tableNameHint: tableHint });
    res.json(out);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: String(e.message || e) });
  }
});

// Chat -> SQL -> résultats
app.post('/chat', async (req, res) => {
  try {
    const { message, history } = req.body || {};
    if (!message) return res.status(400).json({ error: 'message manquant' });

    const schema = getSchema();
    const sql = await suggestSQL({ schema, question: message, history });

    let rows;
    try {
      rows = await safeRun(sql);
    } catch (err) {
      // Une passe de rattrapage : donner l'erreur au LLM pour correction
      const repairMsg = [
        ...(history || []),
        { role: 'user', content: `Cette requête a échoué:

${sql}

Erreur: ${String(err)}
Corrige-la.` }
      ];
      const fixed = await suggestSQL({ schema, question: message, history: repairMsg });
      rows = await safeRun(fixed);
      return res.json({ sql: fixed, rows, repaired: true });
    }

    res.json({ sql, rows, repaired: false });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: String(e.message || e) });
  }
});

const PORT = Number(process.env.PORT || 8787);
app.listen(PORT, () => {
  console.log(`Server listening on http://localhost:${PORT}`);
});
