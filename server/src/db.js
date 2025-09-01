// server/src/db.js
import duckdb from "duckdb";
import { slugifyHeader, guessType } from "./utils/normalize.js";

const db = new duckdb.Database(":memory:");
const conn = db.connect();

// { tableName: [{ name, type, original }] }
const catalog = { tables: {} };

function esc(id) { return id.replace(/"/g, '""'); }

export function getSchema() {
  return catalog.tables;
}

export async function runSQL(sql) {
  return new Promise((resolve, reject) => {
    conn.all(sql, (err, rows) => (err ? reject(err) : resolve(rows)));
  });
}

export async function safeRun(sql) {
  // Interdit DDL/DML dangereux
  if (/(^|\W)(create|insert|update|delete|drop|alter|attach|copy|load)(\W|$)/i.test(sql)) {
    throw new Error("SQL non autorisé (DDL/DML interdit).");
  }
  return runSQL(sql);
}

export async function ingestXlsxBuffer(buf, { tableNameHint }) {
  const { read, utils } = await import("xlsx");
  const wb = read(buf, { type: "buffer" });
  const wsName = wb.SheetNames[0];
  const ws = wb.Sheets[wsName];
  const rows = utils.sheet_to_json(ws, { defval: null });

  if (!rows.length) throw new Error("Fichier vide.");

  // Normaliser entêtes
  const headers = Object.keys(rows[0]);
  const lowerHeaders = headers.map(h => String(h).toLowerCase());

  // Détection fichier "intitulés" (3 colonnes typiques champ/description/exemple)
  const maybeIntitules =
    headers.length <= 4 &&
    (
      lowerHeaders.some(h => h.includes("intitul")) ||
      (lowerHeaders[0] || "").startsWith("champ") ||
      (lowerHeaders[0] || "").startsWith("intitul")
    );

  if (maybeIntitules) {
    const colNameKey = headers[0];
    const cols = rows
      .map(r => r[colNameKey])
      .filter(Boolean)
      .map(n => ({ name: slugifyHeader(n), type: "VARCHAR", original: String(n) }));

    const table = slugifyHeader(tableNameHint || "table");
    catalog.tables[table] = cols;
    return { table, columns: cols, created: false, intitules: true };
  }

  // Données réelles -> créer table + insérer
  const normHeaders = headers.map(h => slugifyHeader(h));

  // Inférer types (échantillon)
  const sample = rows.slice(0, 200);
  const types = normHeaders.map((_, i) => {
    for (const r of sample) {
      const v = r[headers[i]];
      if (v !== null && v !== undefined && v !== "") {
        return guessType(v);
      }
    }
    return "VARCHAR";
  });

  const table = slugifyHeader(tableNameHint || "table");
  const ddlCols = normHeaders.map((h, i) => `"${esc(h)}" ${types[i]}`).join(", ");
  await runSQL(`CREATE OR REPLACE TABLE "${esc(table)}" (${ddlCols});`);

  // INSERT ligne à ligne (simple et robuste)
  for (const r of rows) {
    const values = normHeaders.map((_, i) => {
      const v = r[headers[i]];
      if (v === null || v === undefined) return "NULL";
      if (types[i] === "DOUBLE" || types[i] === "BIGINT") {
        const num = Number(v);
        return Number.isFinite(num) ? String(num) : "NULL";
      }
      if (types[i] === "BOOLEAN") {
        return /^(true|1|oui|yes)$/i.test(String(v)) ? "TRUE" : "FALSE";
      }
      const s = String(v).replaceAll("'", "''");
      return `'${s}'`;
    });

    const colsList = normHeaders.map(h => `"${esc(h)}"`).join(",");
    const insert = `INSERT INTO "${esc(table)}" (${colsList}) VALUES (${values.join(",")});`;
    await runSQL(insert);
  }

  // Mettre à jour le catalogue
  const cols = normHeaders.map((h, i) => ({ name: h, type: types[i], original: headers[i] }));
  catalog.tables[table] = cols;

  // Index auto si colonnes clés détectées
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
