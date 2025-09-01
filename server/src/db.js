// server/src/db.js
import duckdb from 'duckdb';
import { slugifyHeader, guessType } from './utils/normalize.js';

const db = new duckdb.Database(':memory:');
const conn = db.connect();

// Catalog en mémoire du schéma + types attendus (depuis fichiers d'intitulés)
const catalog = {
  // { tableName: [{ name, type, original }] }
  tables: {},
  // { tableName: { colName -> SQLTYPE } }  (préférences depuis la colonne C "Exemple")
  expectedTypes: {},
};

function esc(id) { return id.replace(/"/g, '""'); }

export function getSchema() { return catalog.tables; }
export function getExpectedTypes() { return catalog.expectedTypes; }

export async function runSQL(sql) {
  return new Promise((resolve, reject) => {
    conn.all(sql, (err, rows) => (err ? reject(err) : resolve(rows)));
  });
}

// Exécute uniquement du SELECT (sécurisé)
export async function safeRun(sql) {
  if (/(^|\W)(create|insert|update|delete|drop|alter|attach|copy|load|truncate|vacuum|replace)(\W|$)/i.test(sql)) {
    throw new Error('SQL non autorisé (DDL/DML interdit).');
  }
  return runSQL(sql);
}

// Heuristique basée sur un "exemple" (colonne C dans les fichiers d'intitulés)
function typeFromExample(sample) {
  if (sample == null) return 'VARCHAR';
  const s = String(sample).trim();
  if (!s) return 'VARCHAR';
  // Dates fréquentes : 2024-05-31 ou 31/05/2024
  if (/^\d{4}-\d{2}-\d{2}/.test(s) || /^\d{2}\/\d{2}\/\d{4}$/.test(s)) return 'TIMESTAMP';
  if (/^(true|false|oui|non)$/i.test(s)) return 'BOOLEAN';
  if (/^-?\d+$/.test(s)) return 'BIGINT';
  if (/^-?\d+[.,]\d+$/.test(s)) return 'DOUBLE';
  return 'VARCHAR';
}

// Ingestion d'un fichier Excel (intitulés OU données réelles)
export async function ingestXlsxBuffer(buf, { tableNameHint }) {
  const { read, utils } = await import('xlsx');
  const wb = read(buf, { type: 'buffer' });
  const ws = wb.Sheets[wb.SheetNames[0]];
  const rows = utils.sheet_to_json(ws, { defval: null });

  if (!rows.length) throw new Error('Fichier vide.');

  const headers = Object.keys(rows[0]);
  const lowerHeaders = headers.map(h => String(h).toLowerCase());

  // Détection fichier "intitulés" (A/B/C = Champ/Description/Exemple)
  const maybeIntitules =
    headers.length <= 5 &&
    (
      lowerHeaders.some(h => h.includes('intitul')) ||
      (lowerHeaders[0] || '').startsWith('champ') ||
      (lowerHeaders[0] || '').startsWith('intitul')
    );

  if (maybeIntitules) {
    const colNameKey = headers[0];   // libellé officiel (A)
    const colExKey   = headers[2];   // Exemple (C) si dispo

    const cols = [];
    const expected = {};

    for (const r of rows) {
      const label = r[colNameKey];
      if (!label) continue;
      const norm = slugifyHeader(label);
      cols.push({ name: norm, type: 'VARCHAR', original: String(label) });

      if (colExKey !== undefined) {
        const exVal = r[colExKey];
        if (exVal !== undefined && exVal !== null && String(exVal).trim() !== '') {
          expected[norm] = typeFromExample(exVal);
        }
      }
    }

    const table = slugifyHeader(tableNameHint || 'table');
    catalog.tables[table] = cols;
    catalog.expectedTypes[table] = expected;

    return { table, columns: cols, created: false, intitules: true, expectedTypes: expected };
  }

  // Données réelles -> créer table + insérer
  const normHeaders = headers.map(h => slugifyHeader(h));
  const table = slugifyHeader(tableNameHint || 'table');

  // Types préférés issus d'un upload préalable d'intitulés (col. C)
  const prefer = catalog.expectedTypes[table] || {};

  // Inférer types (échantillon), en respectant les préférences si dispo
  const sample = rows.slice(0, 200);
  const types = normHeaders.map((nh, i) => {
    // 1) type préféré (depuis exemples)
    if (prefer[nh]) return prefer[nh];

    // 2) sinon heuristique automatique
    for (const r of sample) {
      const v = r[headers[i]];
      if (v !== null && v !== undefined && v !== '') {
        return guessType(v);
      }
    }
    return 'VARCHAR';
  });

  const ddl = `CREATE OR REPLACE TABLE "${esc(table)}" (${normHeaders.map((h, i) => `"${esc(h)}" ${types[i]}`).join(', ')});`;
  await runSQL(ddl);

  // INSERT ligne à ligne (pratique/robuste)
  for (const r of rows) {
    const values = normHeaders.map((h, i) => {
      const v = r[headers[i]];
      if (v === null || v === undefined || v === '') return 'NULL';
      if (types[i] === 'DOUBLE') {
        const num = Number(String(v).replace(',', '.'));
        return Number.isFinite(num) ? String(num) : 'NULL';
      }
      if (types[i] === 'BIGINT') {
        const num = Number(String(v).replace(',', '.'));
        return Number.isFinite(num) ? String(Math.trunc(num)) : 'NULL';
      }
      if (types[i] === 'BOOLEAN') {
        return /^(true|1|oui|yes)$/i.test(String(v)) ? 'TRUE' : 'FALSE';
      }
      // TIMESTAMP / VARCHAR -> quote + escape
      const s = String(v).replaceAll("'", "''");
      return `'${s}'`;
    });

    const colsList = normHeaders.map(h => `"${esc(h)}"`).join(',');
    const insert = `INSERT INTO "${esc(table)}" (${colsList}) VALUES (${values.join(',')});`;
    await runSQL(insert);
  }

  // Enregistre le schéma final (nom normalisé + type + libellé original)
  const cols = normHeaders.map((h, i) => ({ name: h, type: types[i], original: headers[i] }));
  catalog.tables[table] = cols;

  // Index auto sur colonnes clés (si trouvées)
  try {
    const keyCols = cols.filter(c =>
      /(numero|n|num).*commande/.test(c.name) || /(n|num).*ligne.*commande/.test(c.name)
    );
    for (const kc of keyCols) {
      await runSQL(`CREATE INDEX IF NOT EXISTS idx_${table}_${kc.name} ON "${esc(table)}"("${esc(kc.name)}");`);
    }
  } catch {
    /* noop */
  }

  return { table, columns: cols, created: true, intitules: false };
}
