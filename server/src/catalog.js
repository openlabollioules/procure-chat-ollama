// server/src/catalog.js
import OpenAI from "openai";
import { getSchema, runSQL } from "./db.js";

// --- Intitulés exacts extraits des fichiers -intitulés.xlsx (colonne A) ---
// Ces libellés sont utilisés en priorité pour résoudre les colonnes dans DuckDB.
const COLUMN_ALIASES = {
  achats: {
    order_no: [
      "Nº de commande", "N° de commande", "N° Commande"
    ],
    line_no: [
      "Nº de ligne de commande", "N° ligne commande", "N° Ligne Commande"
    ],
    type_ligne: [
      "Type de la ligne de commande"
    ],
    desc_cmd: [
      "Description de la commande"
    ],
    desc_line: [
      "Description de la ligne", "Description Ligne"
    ],
    fourn: [
      "Nom du fournisseur", "N° du fournisseur" // priorité au nom
    ],
    // Pas de "Date de commande" explicite : on prend une date proxy
    date_cmd: [
      "Date d'approbation", "Date de validation", "Date de création", "Date promise"
    ]
  },
  decs: {
    order_no: [
      "N° commande", "N° Commande"
    ],
    line_no: [
      "N° ligne commande", "N° Ligne Commande"
    ],
    date_pay: [
      "Date règlement"
    ],
    montant: [
      "Montant règlement"
    ]
  },
  details: {
    // Fallback si la date de commande est absente d'Achats
    order_no: [
      "N° Commande", "N° commande"
    ],
    line_no: [
      "N° Ligne Commande", "N° ligne commande"
    ],
    // Dates utiles (on privilégie Date engagement)
    date_cmd_candidates: [
      "Date engagement", "Date promesse", "Date estimée règlement"
    ],
    desc_line: [
      "Description Ligne"
    ]
  }
};

// ---- Client LLM (Ollama via API OpenAI-compatible) ----
const client = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY || "ollama",
  baseURL: process.env.OPENAI_BASE_URL || "http://127.0.0.1:11434/v1",
});
const MODEL = process.env.OLLAMA_MODEL || "gpt-oss:20b";

// ---- État en mémoire ----
const state = {
  taxonomy: /** @type {Array<{category:string, subcategories:string[]}>} */ ([]),
  tables: { achats: null, decs: null, details: null },
  cols: { achats: {}, decs: {}, details: {} },
  builtAt: null,
};

// ---- Helpers ----
function esc(id) {
  return id.replace(/"/g, '""');
}
function q(v) {
  if (v === null || v === undefined) return "NULL";
  const s = String(v).replace(/'/g, "''");
  return `'${s}'`;
}

// Choisit la meilleure table en fonction d'indices
function findTable(schema, nameHints = [], requiredColsRegex = []) {
  const tables = Object.keys(schema || {});
  if (!tables.length) return null;
  const scored = tables
    .map((t) => {
      const n = t.toLowerCase();
      let score = 0;
      for (const h of nameHints) if (n.includes(h)) score += 2;
      const cols = schema[t] || [];
      const names =
        cols.map((c) => c.name).join("|") +
        "|" +
        cols.map((c) => String(c.original || "")).join("|");
      for (const rx of requiredColsRegex) if (rx.test(names.toLowerCase())) score += 1;
      return { t, score };
    })
    .sort((a, b) => b.score - a.score);
  return (scored[0]?.score || 0) > 0 ? scored[0].t : tables[0];
}

// Cherche une colonne par libellé officiel (match exact sur "original" ou le nom normalisé)
function resolveByAliases(schema, table, aliases) {
  if (!table || !aliases?.length) return null;
  const cols = (schema[table] || []).map(c => ({
    norm: (c.name || "").toLowerCase(),
    orig: String(c.original || "").toLowerCase(),
    name: c.name
  }));
  for (const alias of aliases) {
    const a = String(alias).toLowerCase();
    const hit = cols.find(c => c.orig === a || c.norm === a);
    if (hit) return hit.name;
  }
  return null;
}

async function llmJSON(messages) {
  const resp = await client.chat.completions.create({
    model: MODEL,
    messages,
    temperature: 0.2,
  });
  let txt = resp.choices?.[0]?.message?.content?.trim() || "";
  const m = txt.match(/```json\s*([\s\S]*?)```/i);
  if (m) txt = m[1].trim();
  try {
    return JSON.parse(txt);
  } catch {
    throw new Error("Le LLM n'a pas renvoyé un JSON valide.");
  }
}

// ---- Construction du catalogue (taxonomie + mapping + paiements) ----
export async function buildCatalog() {
  const schema = getSchema();

  // 1) Identification des tables
  const achatsTable = findTable(schema, ["achat","command"], [/type.*ligne/, /description.*commande/, /description.*ligne/]);
  const decsTable   = findTable(schema, ["decaisse","regle","règle","paiem","reglement","règlement"], [/date.*(dec|reg|pai)/, /montant/]);
  const detailsTable = findTable(schema, ["detail","ligne","lines"], [/n.*commande/, /ligne.*commande/]);

  if (!achatsTable) throw new Error("Table Achats introuvable.");
  if (!decsTable) throw new Error("Table Décaissements introuvable.");

  // 2) Colonnes nécessaires dans Achats — via intitulés exacts
  const A = {};
  A.order_no   = resolveByAliases(schema, achatsTable, COLUMN_ALIASES.achats.order_no);
  A.line_no    = resolveByAliases(schema, achatsTable, COLUMN_ALIASES.achats.line_no);
  A.type_ligne = resolveByAliases(schema, achatsTable, COLUMN_ALIASES.achats.type_ligne);
  A.desc_cmd   = resolveByAliases(schema, achatsTable, COLUMN_ALIASES.achats.desc_cmd);
  A.desc_line  = resolveByAliases(schema, achatsTable, COLUMN_ALIASES.achats.desc_line);
  A.fourn      = resolveByAliases(schema, achatsTable, COLUMN_ALIASES.achats.fourn);
  A.date_cmd   = resolveByAliases(schema, achatsTable, COLUMN_ALIASES.achats.date_cmd); // peut être null

  // 3) Colonnes nécessaires dans Décaissements — via intitulés exacts
  const D = {};
  D.order_no = resolveByAliases(schema, decsTable, COLUMN_ALIASES.decs.order_no);
  D.line_no  = resolveByAliases(schema, decsTable, COLUMN_ALIASES.decs.line_no);
  D.montant  = resolveByAliases(schema, decsTable, COLUMN_ALIASES.decs.montant);
  D.date_pay = resolveByAliases(schema, decsTable, COLUMN_ALIASES.decs.date_pay);

  // Vérifs minimales (sauf date_cmd qu'on tolère)
  const requiredA = ["order_no","line_no","type_ligne","desc_cmd","desc_line","fourn"];
  for (const k of requiredA) if (!A[k]) throw new Error(`Colonne Achats manquante: ${k}`);
  const requiredD = ["order_no","line_no","montant","date_pay"];
  for (const k of requiredD) if (!D[k]) throw new Error(`Colonne Décaissements manquante: ${k}`);

  // Fallback date de commande via Détails (si dispo)
  let dateFallback = null;
  if (!A.date_cmd && detailsTable) {
    const detOrder = resolveByAliases(schema, detailsTable, COLUMN_ALIASES.details.order_no);
    const detLine  = resolveByAliases(schema, detailsTable, COLUMN_ALIASES.details.line_no);
    const detDate  = resolveByAliases(schema, detailsTable, COLUMN_ALIASES.details.date_cmd_candidates);
    if (detOrder && detLine && detDate) {
      dateFallback = { table: detailsTable, order_no: detOrder, line_no: detLine, col: detDate };
    }
  }

  // 4) Échantillon Achats pour construire la taxonomie (max 500 lignes)
  const sample = await runSQL(`
    SELECT "${esc(A.type_ligne)}" AS type_ligne,
           "${esc(A.desc_cmd)}"   AS desc_cmd,
           "${esc(A.desc_line)}"  AS desc_line
    FROM "${esc(achatsTable)}"
    WHERE "${esc(A.type_ligne)}" IS NOT NULL
       OR "${esc(A.desc_cmd)}"   IS NOT NULL
       OR "${esc(A.desc_line)}"  IS NOT NULL
    LIMIT 500;
  `);

  const systemTaxo = `Tu es un classificateur d'achats en français.
Produis UNIQUEMENT un JSON strict de la forme:
{"taxonomy":[{"category":"<cat>", "subcategories":["<sub1>","<sub2>", ...]}, ...]}
Règles:
- Regroupe par natures métiers (ex: "Matériel informatique" > "Postes de travail", "Réseau", ...).
- Pas de doublons, pas de texte superflu ni d'explications.
- 15-30 catégories, chacune avec 3-15 sous-catégories, selon l'échantillon.`;

  const userTaxo = `Voici un échantillon de lignes d'achats avec 3 champs:
- Type de ligne de commande
- Description de la commande
- Description de la ligne

Echantillon:
${sample
  .map(
    (r) => `- [${r.type_ligne || ""}] ${r.desc_cmd || ""} | ${r.desc_line || ""}`
  )
  .join("\n")
  .slice(0, 8000)}

Rends uniquement le JSON demandé.`;

  const taxoJson = await llmJSON([
    { role: "system", content: systemTaxo },
    { role: "user", content: userTaxo },
  ]);

  const taxonomy = (taxoJson.taxonomy || [])
    .map((t) => ({
      category: String(t.category || "").trim(),
      subcategories: (t.subcategories || [])
        .map((s) => String(s || "").trim())
        .filter(Boolean),
    }))
    .filter((t) => t.category && t.subcategories.length);

  if (!taxonomy.length) throw new Error("Taxonomie vide renvoyée par le LLM.");

  // 5) Classification des lignes Achats -> (category, subcategory)
  await runSQL(`
    CREATE TABLE IF NOT EXISTS catalog_line_map (
      order_no VARCHAR,
      line_no VARCHAR,
      category VARCHAR,
      subcategory VARCHAR,
      fournisseur VARCHAR
    );
  `);
  await runSQL(`DELETE FROM catalog_line_map;`);

  const lines = await runSQL(`
    SELECT
      CAST("${esc(A.order_no)}" AS VARCHAR) AS order_no,
      CAST("${esc(A.line_no)}" AS VARCHAR)  AS line_no,
      "${esc(A.type_ligne)}"                AS type_ligne,
      "${esc(A.desc_cmd)}"                  AS desc_cmd,
      "${esc(A.desc_line)}"                 AS desc_line,
      "${esc(A.fourn)}"                     AS fournisseur
    FROM "${esc(achatsTable)}";
  `);

  const taxoText = JSON.stringify(taxonomy);
  const batchSize = 120;

  for (let i = 0; i < lines.length; i += batchSize) {
    const chunk = lines.slice(i, i + batchSize);
    const items = chunk.map((r) => ({
      key: `${r.order_no}|||${r.line_no}`,
      type_ligne: r.type_ligne || "",
      desc_cmd: r.desc_cmd || "",
      desc_line: r.desc_line || "",
      fournisseur: r.fournisseur || "",
    }));

    const systemClass = `Tu classes des lignes d'achats en fonction d'une taxonomie fournie.
Tu dois retourner pour chaque item un JSON: [{"key":"<order|||line>", "category":"<cat>", "subcategory":"<sub>"}...]
Contraintes:
- Utilise STRICTEMENT la taxonomie donnée (pas de nouvelles catégories).
- Si ambigu, choisis la meilleure sous-catégorie.
- Pas d'autre texte que le JSON.`;

    const userClass = `Taxonomie:
${taxoText}

Items à classer:
${JSON.stringify(items).slice(0, 12000)}

Rends UNIQUEMENT le JSON demandé.`;

    const res = await llmJSON([
      { role: "system", content: systemClass },
      { role: "user", content: userClass },
    ]);

    const rowsToInsert = Array.isArray(res) ? res : res?.items;
    if (!Array.isArray(rowsToInsert)) continue;

    for (const m of rowsToInsert) {
      const [order_no, line_no] = String(m.key || "").split("|||");
      const category = String(m.category || "").trim();
      const subcategory = String(m.subcategory || "").trim();
      if (!order_no || !line_no || !category || !subcategory) continue;
      const fournisseur =
        chunk.find((r) => `${r.order_no}|||${r.line_no}` === m.key)?.fournisseur || "";

      const insertSQL = `
        INSERT INTO catalog_line_map (order_no, line_no, category, subcategory, fournisseur)
        VALUES (${q(order_no)}, ${q(line_no)}, ${q(category)}, ${q(subcategory)}, ${q(fournisseur)});
      `;
      await runSQL(insertSQL);
    }
  }

  // 6) Calcul des paiements liés + délais (date pay - date commande)
  await runSQL(`
    CREATE TABLE IF NOT EXISTS catalog_payments (
      category VARCHAR,
      subcategory VARCHAR,
      fournisseur VARCHAR,
      order_no VARCHAR,
      line_no VARCHAR,
      order_date DATE,
      payment_date DATE,
      montant DOUBLE,
      delay_days INTEGER
    );
  `);
  await runSQL(`DELETE FROM catalog_payments;`);

  await runSQL(`
    INSERT INTO catalog_payments
    SELECT
      m.category,
      m.subcategory,
      a."${esc(A.fourn)}" AS fournisseur,
      CAST(a."${esc(A.order_no)}" AS VARCHAR) AS order_no,
      CAST(a."${esc(A.line_no)}"  AS VARCHAR) AS line_no,
      ${
        A.date_cmd
          ? `CAST(a."${esc(A.date_cmd)}" AS DATE)`
          : dateFallback
            ? `(SELECT CAST(MIN(dtl."${esc(dateFallback.col)}") AS DATE)
                FROM "${esc(dateFallback.table)}" dtl
                WHERE CAST(dtl."${esc(dateFallback.order_no)}" AS VARCHAR) = CAST(a."${esc(A.order_no)}" AS VARCHAR)
                  AND CAST(dtl."${esc(dateFallback.line_no)}"  AS VARCHAR) = CAST(a."${esc(A.line_no)}"  AS VARCHAR))`
            : `NULL`
      } AS order_date,
      CAST(d."${esc(D.date_pay)}" AS DATE)    AS payment_date,
      CAST(d."${esc(D.montant)}"  AS DOUBLE)  AS montant,
      ${
        A.date_cmd
          ? `datediff('day', CAST(a."${esc(A.date_cmd)}" AS DATE), CAST(d."${esc(D.date_pay)}" AS DATE))`
          : dateFallback
            ? `datediff('day',
                 (SELECT CAST(MIN(dtl."${esc(dateFallback.col)}") AS DATE)
                  FROM "${esc(dateFallback.table)}" dtl
                  WHERE CAST(dtl."${esc(dateFallback.order_no)}" AS VARCHAR) = CAST(a."${esc(A.order_no)}" AS VARCHAR)
                    AND CAST(dtl."${esc(dateFallback.line_no)}"  AS VARCHAR) = CAST(a."${esc(A.line_no)}"  AS VARCHAR)
                 ),
                 CAST(d."${esc(D.date_pay)}" AS DATE)
               )`
            : `NULL`
      } AS delay_days
    FROM "${esc(achatsTable)}" a
    JOIN catalog_line_map m
      ON CAST(a."${esc(A.order_no)}" AS VARCHAR) = m.order_no
     AND CAST(a."${esc(A.line_no)}"  AS VARCHAR) = m.line_no
    JOIN "${esc(decsTable)}" d
      ON CAST(a."${esc(A.order_no)}" AS VARCHAR) = CAST(d."${esc(D.order_no)}" AS VARCHAR)
     AND CAST(a."${esc(A.line_no)}"  AS VARCHAR) = CAST(d."${esc(D.line_no)}"  AS VARCHAR)
    WHERE d."${esc(D.date_pay)}" IS NOT NULL
      AND d."${esc(D.montant)}"  IS NOT NULL;
  `);

  // 7) Mettre à jour l'état
  state.taxonomy = taxonomy;
  state.tables.achats = achatsTable;
  state.tables.decs = decsTable;
  state.tables.details = detailsTable;
  state.cols.achats = A;
  state.cols.decs = D;
  state.cols.details = dateFallback || {};
  state.builtAt = new Date();

  const counts = await runSQL(`
    SELECT subcategory, CAST(COUNT(*) AS INT) AS n
    FROM catalog_line_map
    GROUP BY 1
    ORDER BY n DESC;
  `);

  return {
    taxonomy,
    tables: state.tables,
    cols: state.cols,
    counts,
    builtAt: state.builtAt,
  };
}

// ---- Résumé pour le frontend (taxonomie + fournisseurs par sous-catégorie) ----
export async function getSummary() {
  if (!state.taxonomy?.length) {
    return { taxonomy: [], suppliers: [], byCategory: {}, bySubcategorySupplier: {}, counts: {} };
  }
  const taxo = state.taxonomy;
  const byCategory = Object.fromEntries(taxo.map((t) => [t.category, t.subcategories]));

  const suppliersRows = await runSQL(`
    SELECT DISTINCT subcategory, COALESCE(fournisseur,'') AS f
    FROM catalog_line_map
    WHERE fournisseur IS NOT NULL;
  `);
  const bySub = {};
  for (const r of suppliersRows) {
    const s = r.subcategory;
    if (!bySub[s]) bySub[s] = [];
    if (r.f && !bySub[s].includes(r.f)) bySub[s].push(r.f);
  }

  const suppliersAll = await runSQL(`
    SELECT DISTINCT fournisseur
    FROM catalog_line_map
    WHERE fournisseur IS NOT NULL
    ORDER BY 1;
  `);

  const counts = await runSQL(`
    SELECT subcategory, CAST(COUNT(*) AS INT) AS n
    FROM catalog_line_map
    GROUP BY 1;
  `);
  const countMap = Object.fromEntries(counts.map((c) => [c.subcategory, Number(c.n)]));

  return {
    taxonomy: taxo,
    suppliers: suppliersAll.map((s) => s.fournisseur).filter(Boolean),
    byCategory,
    bySubcategorySupplier: bySub,
    counts: countMap,
  };
}

// ---- Profil d'écoulement par (sous-catégorie, fournisseur) ----
export async function getProfile(subcategory, supplier) {
  if (!subcategory || !supplier) throw new Error("Paramètres requis: subcategory & supplier.");

  const series = await runSQL(`
    SELECT delay_days, SUM(montant) AS montant_total
    FROM catalog_payments
    WHERE subcategory = ${q(subcategory)}
      AND fournisseur = ${q(supplier)}
    GROUP BY 1
    ORDER BY 1;
  `);

  const points = await runSQL(`
    SELECT delay_days, montant, payment_date, order_no, line_no
    FROM catalog_payments
    WHERE subcategory = ${q(subcategory)}
      AND fournisseur = ${q(supplier)}
    ORDER BY payment_date;
  `);

  const totRow = await runSQL(`
    SELECT COALESCE(SUM(montant),0) AS s
    FROM catalog_payments
    WHERE subcategory = ${q(subcategory)}
      AND fournisseur = ${q(supplier)};
  `);
  const total = Number(totRow?.[0]?.s || 0);

  const cumulative = [];
  let acc = 0;
  for (const r of series) {
    const amt = Number(r.montant_total || 0);
    acc += amt;
    cumulative.push({
      delay_days: r.delay_days,
      cum_amount: acc,
      share: total ? acc / total : 0,
    });
  }

  const stats =
    (await runSQL(`
      SELECT
        CAST(COUNT(*) AS INT) AS n_payments,
        COALESCE(SUM(montant),0) AS total,
        CAST(quantile_cont(delay_days, 0.5) AS INT)  AS median_delay,
        CAST(quantile_cont(delay_days, 0.25) AS INT) AS p25,
        CAST(quantile_cont(delay_days, 0.75) AS INT) AS p75
      FROM catalog_payments
      WHERE subcategory = ${q(subcategory)}
        AND fournisseur = ${q(supplier)};
    `))?.[0] || { n_payments: 0, total: 0, median_delay: 0, p25: 0, p75: 0 };

  return {
    series,
    points,
    cumulative,
    stats: {
      n_payments: Number(stats.n_payments || 0),
      total: Number(stats.total || 0),
      median_delay: Number(stats.median_delay || 0),
      p25: Number(stats.p25 || 0),
      p75: Number(stats.p75 || 0),
    },
  };
}
