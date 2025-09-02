// server/src/catalog.js
import OpenAI from "openai";
import { getSchema, runSQL } from "./db.js";

/* =========================================================
   Intitulés & alias (élargis)
   ========================================================= */
const COLUMN_ALIASES = {
  achats: {
    order_no: ["Nº de commande", "N° de commande", "N° Commande", "No Commande", "Numero de commande", "Numéro de commande", "N° commande", "Commande"],
    line_no: ["Nº de ligne de commande", "N° ligne commande", "N° Ligne Commande", "No Ligne Commande", "Ligne", "N° ligne", "N° Ligne", "N° ligne de commande"],
    type_ligne: ["Type de la ligne de commande", "Type ligne", "Type de ligne", "Nature de ligne"], // optionnel
    desc_cmd: ["Description de la commande", "Description commande", "Description", "Objet", "Objet de la commande", "Intitulé", "Intitulé de la commande"], // optionnel
    desc_line: ["Description de la ligne", "Description Ligne", "Détail de ligne", "Libellé de ligne"], // optionnel
    fourn: ["Nom du fournisseur", "Fournisseur", "Nom fournisseur", "Raison sociale fournisseur", "N° du fournisseur", "Code fournisseur"], // optionnel
    date_cmd: ["Date d'approbation", "Date de validation", "Date de création", "Date promise", "Date commande", "Date d'engagement"] // optionnel
  },
  decs: {
    order_no: ["N° Commande", "N° commande", "No Commande", "Commande", "Numero de commande"],
    line_no: ["N° Ligne Commande", "N° ligne commande", "No Ligne Commande", "Ligne", "N° ligne"],
    date_pay: ["Date règlement", "Date reglement", "Date de règlement", "Date de reglement", "Date paiement", "Date de paiement"],
    montant: ["Montant règlement", "Montant reglement", "Montant réglé", "Montant payé", "Montant paiement", "Montant"]
  },
  details: {
    order_no: ["N° Commande", "N° commande", "No Commande", "Commande"],
    line_no: ["N° Ligne Commande", "N° ligne commande", "No Ligne Commande", "Ligne"],
    date_cmd_candidates: ["Date engagement", "Date promesse", "Date estimée règlement", "Date estimée reglement", "Date prévue règlement", "Date prévue reglement", "Date commande", "Date de commande"],
    desc_line: ["Description Ligne", "Description de la ligne", "Libellé de ligne", "Détail de ligne"]
  }
};

/* =========================================================
   Client LLM (Ollama via API OpenAI-compatible)
   ========================================================= */
const client = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY || "ollama",
  baseURL: process.env.OPENAI_BASE_URL || "http://127.0.0.1:11434/v1",
});
const MODEL = process.env.OLLAMA_MODEL || "gpt-oss:20b";

/* =========================================================
   État
   ========================================================= */
const state = {
  taxonomy: /** @type {Array<{category:string, subcategories:string[]}>} */ ([]),
  tables: { achats: null, decs: null, details: null },
  cols: { achats: {}, decs: {}, details: {} },
  builtAt: null,
};

/* =========================================================
   Helpers
   ========================================================= */
function esc(id) { return String(id).replace(/"/g, '""'); }
function q(v) { if (v == null) return "NULL"; return `'${String(v).replace(/'/g, "''")}'`; }
const deaccent = (s) => String(s || "").normalize("NFD").replace(/[\u0300-\u036f]/g, "");
const lowerDeaccent = (s) => deaccent(s).toLowerCase();

function resolveByAliases(schema, table, aliases) {
  if (!table || !aliases?.length) return null;
  const cols = (schema[table] || []).map(c => ({
    norm: lowerDeaccent(c.name || ""),
    orig: lowerDeaccent(String(c.original || "")),
    name: c.name
  }));
  for (const alias of aliases) {
    const a = lowerDeaccent(alias);
    const hit =
      cols.find(c => c.orig === a) ||
      cols.find(c => c.norm === a) ||
      cols.find(c => c.orig.includes(a)) ||
      cols.find(c => c.norm.includes(a));
    if (hit) return hit.name;
  }
  return null;
}

async function llmJSON(messages) {
  const resp = await client.chat.completions.create({ model: MODEL, messages, temperature: 0.2 });
  let txt = resp.choices?.[0]?.message?.content?.trim() || "";
  const m = txt.match(/```json\s*([\s\S]*?)```/i); if (m) txt = m[1].trim();
  return JSON.parse(txt || "{}");
}

/* ---------- Normalisation clés ---------- */
const normAlnum = (expr) => `
  NULLIF( regexp_replace(upper(CAST(${expr} AS VARCHAR)), '[^0-9A-Z]+', '', 'g'), '' )
`;

// numérique "digits only" (en retirant les zéros de tête)
const normNum = (expr) => `
  NULLIF(
    regexp_replace(
      regexp_replace(CAST(${expr} AS VARCHAR), '[^0-9]+', '', 'g'),
      '^0+',
      '',
      'g'
    ),
    ''
  )
`;

// entier de ligne robuste (1.0 -> 1, "Ligne 01" -> 1)
const normLineInt = (expr) => `
  TRY_CAST(
    regexp_extract(CAST(${expr} AS VARCHAR), '([0-9]+)', 1)
  AS BIGINT)
`;

// *** NOUVEAU : entier de commande robuste (PO-6903033 / 6903033.0 / 006903033 -> 6903033)
const normOrderInt = (expr) => `
  TRY_CAST(
    regexp_extract(CAST(${expr} AS VARCHAR), '([0-9]+)', 1)
  AS BIGINT)
`;

/* ---------- Dates robustes ---------- */
function sqlDateFromAny(expr) {
  return `
    COALESCE(
      TRY_CAST(${expr} AS DATE),
      CAST(TRY_CAST(${expr} AS TIMESTAMP) AS DATE),
      CAST(TRY_STRPTIME(CAST(${expr} AS VARCHAR), '%Y-%m-%d') AS DATE),
      CAST(TRY_STRPTIME(CAST(${expr} AS VARCHAR), '%d/%m/%Y') AS DATE),
      CAST(TRY_STRPTIME(CAST(${expr} AS VARCHAR), '%d-%m-%Y') AS DATE),
      DATE '1899-12-30' + CAST(ROUND(CAST(${expr} AS DOUBLE)) AS INTEGER)
    )
  `;
}

/* ---------- SELECT dynamiques ---------- */
const selOrNull = (col, alias) => col ? `"${esc(col)}" AS ${alias}` : `NULL AS ${alias}`;

/* =========================================================
   Détection des tables par signature
   ========================================================= */
function scoreTableForRole(schema, table, roleAliases) {
  const neededGroups = Object.entries(roleAliases)
    .filter(([k]) => ["order_no", "line_no", "date_cmd", "date_pay", "montant", "desc_cmd", "desc_line", "type_ligne"].includes(k));
  let score = 0;
  const found = {};
  for (const [key, aliases] of neededGroups) {
    const col = resolveByAliases(schema, table, aliases);
    if (col) { score += 2; found[key] = col; }
  }
  if (found.montant) score += 2;
  if (found.date_pay) score += 1;
  if (found.desc_cmd || found.desc_line) score += 1;
  return { score, found };
}

function pickTablesBySignature(schema) {
  const tables = Object.keys(schema || {});
  if (!tables.length) throw new Error("Aucune table en mémoire.");

  const scored = tables.map(t => ({
    t,
    achats: scoreTableForRole(schema, t, COLUMN_ALIASES.achats),
    decs: scoreTableForRole(schema, t, COLUMN_ALIASES.decs),
    details: scoreTableForRole(schema, t, COLUMN_ALIASES.details),
  }));

  const sortDesc = (arr, key) => arr.slice().sort((a, b) => b[key].score - a[key].score);
  const takeBest = (arr, key, used) => arr.find(x => !used.has(x.t) && x[key].score > 0);

  const used = new Set();
  const sA = sortDesc(scored, "achats"); const bestA = takeBest(sA, "achats", used); if (bestA) used.add(bestA.t);
  const sD = sortDesc(scored, "decs"); const bestD = takeBest(sD, "decs", used); if (bestD) used.add(bestD.t);
  const sT = sortDesc(scored, "details"); const bestT = takeBest(sT, "details", used); if (bestT) used.add(bestT.t);

  const res = {
    achats: bestA ? { table: bestA.t, cols: bestA.achats.found, score: bestA.achats.score } : null,
    decs: bestD ? { table: bestD.t, cols: bestD.decs.found, score: bestD.decs.score } : null,
    details: bestT ? { table: bestT.t, cols: bestT.details.found, score: bestT.details.score } : null,
  };

  const hasAchatsKeys = res.achats && res.achats.cols.order_no && res.achats.cols.line_no;
  const hasDecsKeys = res.decs && res.decs.cols.order_no && res.decs.cols.line_no && res.decs.cols.montant && res.decs.cols.date_pay;

  if (!hasAchatsKeys) throw new Error("Impossible d’identifier la table Achats (colonnes N° commande + N° ligne).");
  if (!hasDecsKeys) throw new Error("Impossible d’identifier la table Décaissements (commande/ligne + montant + date paiement).");

  return res;
}

/* =========================================================
   Build: classification + paiements
   ========================================================= */
export async function buildCatalog() {
  const schema = getSchema() || {};
  const picked = pickTablesBySignature(schema);

  const achatsTable = picked.achats.table;
  const decsTable = picked.decs.table;
  const detailsTable = picked.details?.table || null;

  const A = {
    order_no: picked.achats.cols.order_no || resolveByAliases(schema, achatsTable, COLUMN_ALIASES.achats.order_no),
    line_no: picked.achats.cols.line_no || resolveByAliases(schema, achatsTable, COLUMN_ALIASES.achats.line_no),
    type_ligne: resolveByAliases(schema, achatsTable, COLUMN_ALIASES.achats.type_ligne),
    desc_cmd: picked.achats.cols.desc_cmd || resolveByAliases(schema, achatsTable, COLUMN_ALIASES.achats.desc_cmd),
    desc_line: picked.achats.cols.desc_line || resolveByAliases(schema, achatsTable, COLUMN_ALIASES.achats.desc_line),
    fourn: picked.achats.cols.fourn || resolveByAliases(schema, achatsTable, COLUMN_ALIASES.achats.fourn),
    date_cmd: picked.achats.cols.date_cmd || resolveByAliases(schema, achatsTable, COLUMN_ALIASES.achats.date_cmd),
  };

  const D = {
    order_no: picked.decs.cols.order_no || resolveByAliases(schema, decsTable, COLUMN_ALIASES.decs.order_no),
    line_no: picked.decs.cols.line_no || resolveByAliases(schema, decsTable, COLUMN_ALIASES.decs.line_no),
    montant: picked.decs.cols.montant || resolveByAliases(schema, decsTable, COLUMN_ALIASES.decs.montant),
    date_pay: picked.decs.cols.date_pay || resolveByAliases(schema, decsTable, COLUMN_ALIASES.decs.date_pay),
  };

  // Fallback éventuel pour la date de commande via table "détails"
  let dateFallback = null;
  if (detailsTable) {
    const detOrder = picked.details.cols.order_no || resolveByAliases(schema, detailsTable, COLUMN_ALIASES.details.order_no);
    const detLine = picked.details.cols.line_no || resolveByAliases(schema, detailsTable, COLUMN_ALIASES.details.line_no);
    const detDate = picked.details.cols.date_cmd || resolveByAliases(schema, detailsTable, COLUMN_ALIASES.details.date_cmd_candidates);
    if (detOrder && detLine && detDate) {
      dateFallback = { table: detailsTable, order_no: detOrder, line_no: detLine, col: detDate };
    }
  }

  if (!A.order_no || !A.line_no) throw new Error("Colonnes clés manquantes dans Achats (N° commande / N° ligne).");
  if (!D.order_no || !D.line_no || !D.montant || !D.date_pay) {
    throw new Error("Colonnes clés manquantes dans Décaissements (commande/ligne + montant + date paiement).");
  }

  const hasAnyTextField = Boolean(A.type_ligne || A.desc_cmd || A.desc_line);
  if (!hasAnyTextField) {
    throw new Error(
      `Impossible de construire le catalogue : aucune colonne descriptive trouvée dans '${achatsTable}'. ` +
      `Ajoute au moins l'une de ces colonnes : Type de la ligne / Description de la commande / Description de la ligne.`
    );
  }

  // Table de mapping (recréée)
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

  // Lignes Achats à classifier
  const lines = await runSQL(`
    SELECT
      CAST("${esc(A.order_no)}" AS VARCHAR) AS order_no,
      CAST("${esc(A.line_no)}"  AS VARCHAR) AS line_no,
      ${selOrNull(A.type_ligne, "type_ligne")},
      ${selOrNull(A.desc_cmd, "desc_cmd")},
      ${selOrNull(A.desc_line, "desc_line")},
      ${selOrNull(A.fourn, "fournisseur")}
    FROM "${esc(achatsTable)}";
  `);

  // Classification incrémentale via LLM
  let canon = /** @type {{category:string, subcategories:string[]}[]} */ ([]);
  const batchSize = 120;

  for (let i = 0; i < lines.length; i += batchSize) {
    const chunk = lines.slice(i, i + batchSize);
    const items = chunk.map(r => ({
      key: `${r.order_no}|||${r.line_no}`,
      type_ligne: r.type_ligne || "",
      desc_cmd: r.desc_cmd || "",
      desc_line: r.desc_line || "",
      fournisseur: r.fournisseur || "",
    }));

    const systemClass = `Tu construis un catalogue de catégories d'achats en français de manière incrémentale.
RÈGLES IMPORTANTES:
- Tu NE dois PAS inventer de catégories hors contexte : uniquement pertinentes pour les items fournis.
- Réutilise une catégorie existante si elle convient; sinon crée-en une nouvelle justifiée.
- Fusionne les doublons évidents via "aliases".
- Réponds UNIQUEMENT en JSON.`;

    const userClass = `Catalogue actuel (canonique):
${JSON.stringify(canon)}

Items à classifier (retourne un array "assignments"):
${JSON.stringify(items).slice(0, 12000)}

FORMAT JSON STRICT attendu:
{
  "assignments": [
    {"key":"<order|||line>","category":"<cat>","subcategory":"<sub>"} , ...
  ],
  "aliases": [
    {"from":{"category":"X","subcategory":"Y"},"to":{"category":"X'","subcategory":"Y'"}}
  ],
  "new_categories": [
    {"category":"<cat>","subcategories":["<sub1>","<sub2>", "..."]}
  ]
}`;

    let out;
    try {
      out = await llmJSON([
        { role: "system", content: systemClass },
        { role: "user", content: userClass },
      ]);
    } catch {
      out = {
        assignments: items.map(it => ({ key: it.key, category: "Autre", subcategory: "Autre" })),
        aliases: [],
        new_categories: []
      };
    }

    const assignments = Array.isArray(out?.assignments) ? out.assignments : [];
    const aliases = Array.isArray(out?.aliases) ? out.aliases : [];
    const newCats = Array.isArray(out?.new_categories) ? out.new_categories : [];

    for (const a of aliases) {
      const toC = String(a?.to?.category || "").trim();
      const toS = String(a?.to?.subcategory || "").trim();
      if (toC && toS) ensureInCanon(canon, toC, toS);
    }
    for (const nc of newCats) {
      const cat = String(nc?.category || "").trim();
      const subs = (nc?.subcategories || []).map(s => String(s || "").trim()).filter(Boolean);
      if (!cat || !subs.length) continue;
      for (const sub of subs) ensureInCanon(canon, cat, sub);
    }
    for (const asg of assignments) {
      const cat = String(asg?.category || "").trim();
      const sub = String(asg?.subcategory || "").trim();
      if (cat && sub) ensureInCanon(canon, cat, sub);
    }

    for (const asg of assignments) {
      const [order_no, line_no] = String(asg.key || "").split("|||");
      const category = String(asg.category || "").trim();
      const subcategory = String(asg.subcategory || "").trim();
      if (!order_no || !line_no || !category || !subcategory) continue;
      const fournisseur = (chunk.find(r => `${r.order_no}|||${r.line_no}` === asg.key)?.fournisseur) || "";

      await runSQL(`
        INSERT INTO catalog_line_map (order_no, line_no, category, subcategory, fournisseur)
        VALUES (${q(order_no)}, ${q(line_no)}, ${q(category)}, ${q(subcategory)}, ${q(fournisseur)});
      `);
    }
  }

  // Paiements liés + délais
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

  // --- DEBUG : décaissements parsés OK (date & montant)
  const dbgDecsRaw = await runSQL(`SELECT COUNT(*) AS n FROM "${esc(decsTable)}";`);
  const dbgDecsParsed = await runSQL(`
    SELECT COUNT(*) AS n
    FROM "${esc(decsTable)}" d
    WHERE ${sqlDateFromAny(`d."${esc(D.date_pay)}"`)} IS NOT NULL
      AND TRY_CAST(
            REPLACE(
              regexp_replace(CAST(d."${esc(D.montant)}" AS VARCHAR), '[^0-9,.\-]', '', 'g'),
              ',',
              '.'
            )
          AS DOUBLE
          ) IS NOT NULL
  `);
  console.log("[buildCatalog] decs total:", Number(dbgDecsRaw?.[0]?.n || 0), "parsedOK:", Number(dbgDecsParsed?.[0]?.n || 0));

  const dbgDecsSample = await runSQL(`
    SELECT
      CAST("${esc(D.order_no)}" AS VARCHAR) AS order_no_raw,
      CAST("${esc(D.line_no)}"  AS VARCHAR) AS line_no_raw,
      ${normLineInt(`"${esc(D.line_no)}"`)} AS line_int_norm,
      ${sqlDateFromAny(`"${esc(D.date_pay)}"`)} AS date_pay_norm,
      TRY_CAST(REPLACE(regexp_replace(CAST("${esc(D.montant)}" AS VARCHAR), '[^0-9,.\-]', '', 'g'), ',', '.') AS DOUBLE) AS montant_norm
    FROM "${esc(decsTable)}"
    LIMIT 5;
  `);
  console.log("[buildCatalog] d_norm sample:", dbgDecsSample);

  // --- DEBUG AVANT INSERT : combien de lignes seront joignables ?
  const dbgJoinable = await runSQL(`
  WITH
  d_norm AS (
    SELECT
      ${normAlnum(`d."${esc(D.order_no)}"`)} AS k_order_alnum,
      ${normNum(`d."${esc(D.order_no)}"`)}   AS k_order_num,
      ${normOrderInt(`d."${esc(D.order_no)}"`)} AS k_order_int,
      ${normAlnum(`d."${esc(D.line_no)}"`)}  AS k_line_alnum,
      ${normNum(`d."${esc(D.line_no)}"`)}    AS k_line_num,
      ${normLineInt(`d."${esc(D.line_no)}"`)} AS k_line_int,
      CAST(d."${esc(D.order_no)}" AS VARCHAR) AS order_no_raw,
      CAST(d."${esc(D.line_no)}"  AS VARCHAR) AS line_no_raw,
      CAST(${sqlDateFromAny(`d."${esc(D.date_pay)}"`)} AS DATE) AS payment_date,
      TRY_CAST(
        REPLACE(
          regexp_replace(CAST(d."${esc(D.montant)}" AS VARCHAR), '[^0-9,.\-]', '', 'g'),
          ',',
          '.'
        )
        AS DOUBLE
      ) AS montant
    FROM "${esc(decsTable)}" d
    WHERE ${sqlDateFromAny(`d."${esc(D.date_pay)}"`)} IS NOT NULL
      AND TRY_CAST(
            REPLACE(
              regexp_replace(CAST(d."${esc(D.montant)}" AS VARCHAR), '[^0-9,.\-]', '', 'g'),
              ',',
              '.'
            )
          AS DOUBLE
          ) IS NOT NULL
  ),
  a_norm AS (
    SELECT
      ${normAlnum(`a."${esc(A.order_no)}"`)} AS k_order_alnum,
      ${normNum(`a."${esc(A.order_no)}"`)}   AS k_order_num,
      ${normOrderInt(`a."${esc(A.order_no)}"`)} AS k_order_int,
      ${normAlnum(`a."${esc(A.line_no)}"`)}  AS k_line_alnum,
      ${normNum(`a."${esc(A.line_no)}"`)}    AS k_line_num,
      ${normLineInt(`a."${esc(A.line_no)}"`)} AS k_line_int,
      CAST(a."${esc(A.order_no)}" AS VARCHAR) AS order_no_raw,
      CAST(a."${esc(A.line_no)}"  AS VARCHAR) AS line_no_raw
    FROM "${esc(achatsTable)}" a
    JOIN catalog_line_map m
      ON CAST(a."${esc(A.order_no)}" AS VARCHAR) = m.order_no
     AND CAST(a."${esc(A.line_no)}"  AS VARCHAR) = m.line_no
  )
  SELECT
    (SELECT COUNT(*) FROM a_norm) AS a_rows,
    (SELECT COUNT(*) FROM d_norm) AS d_rows,
    COUNT(*) AS n
  FROM a_norm
  JOIN d_norm
    ON (
          (a_norm.k_order_alnum IS NOT NULL AND d_norm.k_order_alnum IS NOT NULL AND a_norm.k_order_alnum = d_norm.k_order_alnum)
       OR (a_norm.k_order_num   IS NOT NULL AND d_norm.k_order_num   IS NOT NULL AND
           (
              a_norm.k_order_num = d_norm.k_order_num
           OR RIGHT(d_norm.k_order_num, LENGTH(a_norm.k_order_num)) = a_norm.k_order_num
           OR RIGHT(a_norm.k_order_num, LENGTH(d_norm.k_order_num)) = d_norm.k_order_num
           )
          )
       OR (a_norm.order_no_raw = d_norm.order_no_raw)
       OR (a_norm.k_order_int IS NOT NULL AND d_norm.k_order_int IS NOT NULL AND a_norm.k_order_int = d_norm.k_order_int)
       )
   AND (
          (d_norm.k_line_alnum IS NOT NULL AND a_norm.k_line_alnum IS NOT NULL AND a_norm.k_line_alnum = d_norm.k_line_alnum)
       OR (d_norm.k_line_num   IS NOT NULL AND a_norm.k_line_num   IS NOT NULL AND a_norm.k_line_num   = d_norm.k_line_num)
       OR (a_norm.line_no_raw = d_norm.line_no_raw)
       OR (a_norm.k_line_int IS NOT NULL AND d_norm.k_line_int IS NOT NULL AND a_norm.k_line_int = d_norm.k_line_int)
       OR (d_norm.k_line_alnum IS NULL AND d_norm.k_line_num IS NULL AND d_norm.k_line_int IS NULL)
       );
`);
  console.log("[buildCatalog] joinable (pré-insert):", Number(dbgJoinable?.[0]?.n || 0), "a_rows:", Number(dbgJoinable?.[0]?.a_rows || 0), "d_rows:", Number(dbgJoinable?.[0]?.d_rows || 0));

  // INSERT principal
  await runSQL(`
    INSERT INTO catalog_payments
    WITH
    d_norm AS (
      SELECT
        ${normAlnum(`d."${esc(D.order_no)}"`)} AS k_order_alnum,
        ${normNum(`d."${esc(D.order_no)}"`)}   AS k_order_num,
        ${normOrderInt(`d."${esc(D.order_no)}"`)} AS k_order_int,
        ${normAlnum(`d."${esc(D.line_no)}"`)}  AS k_line_alnum,
        ${normNum(`d."${esc(D.line_no)}"`)}    AS k_line_num,
        ${normLineInt(`d."${esc(D.line_no)}"`)} AS k_line_int,
        CAST(d."${esc(D.order_no)}" AS VARCHAR) AS order_no_raw,
        CAST(d."${esc(D.line_no)}"  AS VARCHAR) AS line_no_raw,
        CAST(${sqlDateFromAny(`d."${esc(D.date_pay)}"`)} AS DATE) AS payment_date,
        TRY_CAST(
          REPLACE(
            regexp_replace(CAST(d."${esc(D.montant)}" AS VARCHAR), '[^0-9,.\-]', '', 'g'),
            ',',
            '.'
          )
          AS DOUBLE
        ) AS montant
      FROM "${esc(decsTable)}" d
      WHERE ${sqlDateFromAny(`d."${esc(D.date_pay)}"`)} IS NOT NULL
        AND TRY_CAST(
              REPLACE(
                regexp_replace(CAST(d."${esc(D.montant)}" AS VARCHAR), '[^0-9,.\-]', '', 'g'),
                ',',
                '.'
              )
            AS DOUBLE
            ) IS NOT NULL
    ),
    a_norm AS (
      SELECT
        ${normAlnum(`a."${esc(A.order_no)}"`)} AS k_order_alnum,
        ${normNum(`a."${esc(A.order_no)}"`)}   AS k_order_num,
        ${normOrderInt(`a."${esc(A.order_no)}"`)} AS k_order_int,
        ${normAlnum(`a."${esc(A.line_no)}"`)}  AS k_line_alnum,
        ${normNum(`a."${esc(A.line_no)}"`)}    AS k_line_num,
        ${normLineInt(`a."${esc(A.line_no)}"`)} AS k_line_int,
        ${selOrNull(A.fourn, "fournisseur")},
        CAST(a."${esc(A.order_no)}" AS VARCHAR) AS order_no_raw,
        CAST(a."${esc(A.line_no)}"  AS VARCHAR) AS line_no_raw,
        COALESCE(
          ${A.date_cmd ? `${sqlDateFromAny(`a."${esc(A.date_cmd)}"`)}`
      : `NULL`},
          ${dateFallback ? `(
                   SELECT ${sqlDateFromAny(`dtl."${esc(dateFallback.col)}"`)}
                   FROM "${esc(dateFallback.table)}" dtl
                   WHERE ${normAlnum(`dtl."${esc(dateFallback.order_no)}"`)} = ${normAlnum(`a."${esc(A.order_no)}"`)}
                     AND ${normAlnum(`dtl."${esc(dateFallback.line_no)}"`)}  = ${normAlnum(`a."${esc(A.line_no)}"`)}
                   ORDER BY ${sqlDateFromAny(`dtl."${esc(dateFallback.col)}"`)} ASC
                   LIMIT 1
                 )`
      : `NULL`},
          (
            SELECT MIN(CAST(${sqlDateFromAny(`dd."${esc(D.date_pay)}"`)} AS DATE))
            FROM "${esc(decsTable)}" dd
            WHERE 
              (
                ${normAlnum(`dd."${esc(D.order_no)}"`)} = ${normAlnum(`a."${esc(A.order_no)}"`)}
                OR ${normNum(`dd."${esc(D.order_no)}"`)} = ${normNum(`a."${esc(A.order_no)}"`)}
                OR CAST(dd."${esc(D.order_no)}" AS VARCHAR) = CAST(a."${esc(A.order_no)}" AS VARCHAR)
              )
              AND (
                ${normAlnum(`dd."${esc(D.line_no)}"`)} = ${normAlnum(`a."${esc(A.line_no)}"`)}
                OR ${normNum(`dd."${esc(D.line_no)}"`)} = ${normNum(`a."${esc(A.line_no)}"`)}
                OR CAST(dd."${esc(D.line_no)}" AS VARCHAR) = CAST(a."${esc(A.line_no)}" AS VARCHAR)
                OR (${normAlnum(`dd."${esc(D.line_no)}"`)} IS NULL AND ${normNum(`dd."${esc(D.line_no)}"`)} IS NULL)
              )
          )
        ) AS order_date
      FROM "${esc(achatsTable)}" a
      JOIN catalog_line_map m
        ON CAST(a."${esc(A.order_no)}" AS VARCHAR) = m.order_no
       AND CAST(a."${esc(A.line_no)}"  AS VARCHAR) = m.line_no
    )
    SELECT
      m.category,
      m.subcategory,
      a_norm.fournisseur,
      a_norm.order_no_raw AS order_no,
      a_norm.line_no_raw  AS line_no,
      a_norm.order_date,
      d_norm.payment_date,
      d_norm.montant,
      datediff('day', a_norm.order_date, d_norm.payment_date) AS delay_days
    FROM a_norm
    JOIN catalog_line_map m
      ON a_norm.order_no_raw = m.order_no
     AND a_norm.line_no_raw  = m.line_no
    JOIN d_norm
      ON (
            (a_norm.k_order_alnum IS NOT NULL AND d_norm.k_order_alnum IS NOT NULL AND a_norm.k_order_alnum = d_norm.k_order_alnum)
         OR (a_norm.k_order_num   IS NOT NULL AND d_norm.k_order_num   IS NOT NULL AND
             (
                a_norm.k_order_num = d_norm.k_order_num
             OR RIGHT(d_norm.k_order_num, LENGTH(a_norm.k_order_num)) = a_norm.k_order_num
             OR RIGHT(a_norm.k_order_num, LENGTH(d_norm.k_order_num)) = d_norm.k_order_num
             )
            )
         OR (a_norm.order_no_raw = d_norm.order_no_raw)
         OR (a_norm.k_order_int IS NOT NULL AND d_norm.k_order_int IS NOT NULL AND a_norm.k_order_int = d_norm.k_order_int)
         )
     AND (
            (d_norm.k_line_alnum IS NOT NULL AND a_norm.k_line_alnum IS NOT NULL AND a_norm.k_line_alnum = d_norm.k_line_alnum)
         OR (d_norm.k_line_num   IS NOT NULL AND a_norm.k_line_num   IS NOT NULL AND a_norm.k_line_num   = d_norm.k_line_num)
         OR (a_norm.line_no_raw = d_norm.line_no_raw)
         OR (a_norm.k_line_int IS NOT NULL AND d_norm.k_line_int IS NOT NULL AND a_norm.k_line_int = d_norm.k_line_int)
         OR (d_norm.k_line_alnum IS NULL AND d_norm.k_line_num IS NULL AND d_norm.k_line_int IS NULL)
         );
  `);

  // État (canon)
  const used = await runSQL(`SELECT DISTINCT category, subcategory FROM catalog_line_map ORDER BY 1,2;`);
  const byCat = new Map();
  for (const r of used) {
    if (!byCat.has(r.category)) byCat.set(r.category, new Set());
    byCat.get(r.category).add(r.subcategory);
  }
  state.taxonomy = Array.from(byCat.entries()).map(([category, set]) => ({
    category,
    subcategories: Array.from(set)
  }));

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

  const payCount = await runSQL(`SELECT COUNT(*) AS n FROM catalog_payments;`);
  console.log("[buildCatalog] catalog_payments rows:", Number(payCount?.[0]?.n || 0));

  return {
    taxonomy: state.taxonomy,
    tables: state.tables,
    cols: state.cols,
    counts,
    builtAt: state.builtAt
  };
}

/* =========================================================
   Résumé pour frontend
   ========================================================= */
export async function getSummary() {
  if (!state.taxonomy?.length) {
    return { taxonomy: [], suppliers: [], byCategory: {}, bySubcategorySupplier: {}, counts: {} };
  }
  const taxo = state.taxonomy;
  const byCategory = Object.fromEntries(taxo.map(t => [t.category, t.subcategories]));

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
  const countMap = Object.fromEntries(counts.map(c => [c.subcategory, Number(c.n)]));

  return {
    taxonomy: taxo,
    suppliers: suppliersAll.map(s => s.fournisseur).filter(Boolean),
    byCategory,
    bySubcategorySupplier: bySub,
    counts: countMap,
  };
}

/* =========================================================
   Profil d'écoulement (quartiles + debug)
   ========================================================= */
export async function getProfile(subcategory, supplier) {
  if (!subcategory || !supplier) throw new Error("Paramètres requis: subcategory & supplier.");

  const dbgCountAll = await runSQL(`
    SELECT COUNT(*) AS n FROM catalog_payments
    WHERE subcategory = ${q(subcategory)} AND fournisseur = ${q(supplier)};
  `);

  const series = await runSQL(`
    SELECT delay_days, SUM(montant) AS montant_total
    FROM catalog_payments
    WHERE subcategory = ${q(subcategory)}
      AND fournisseur = ${q(supplier)}
      AND delay_days IS NOT NULL
    GROUP BY 1
    ORDER BY 1;
  `);

  const points = await runSQL(`
    SELECT delay_days, montant, payment_date, order_no, line_no
    FROM catalog_payments
    WHERE subcategory = ${q(subcategory)}
      AND fournisseur = ${q(supplier)}
      AND delay_days IS NOT NULL
    ORDER BY payment_date;
  `);

  const totalRow = await runSQL(`
    SELECT COALESCE(SUM(montant),0) AS s
    FROM catalog_payments
    WHERE subcategory = ${q(subcategory)}
      AND fournisseur = ${q(supplier)}
      AND delay_days IS NOT NULL;
  `);
  const total = Number((totalRow && totalRow.length ? totalRow[0].s : 0) || 0);

  const cumulative = [];
  let acc = 0;
  for (const r of series) {
    const amt = Number(r.montant_total || 0);
    const d = r.delay_days != null ? Number(r.delay_days) : null;
    if (d == null) continue;
    acc += amt;
    cumulative.push({
      delay_days: d,
      cum_amount: acc,
      share: total ? acc / total : 0,
    });
  }

  const statsRows = await runSQL(`
    SELECT
      CAST(COUNT(*) AS INT)                AS n_payments,
      COALESCE(SUM(montant),0)             AS total,
      CAST(quantile_cont(delay_days, 0.5)  AS INT) AS median_delay,
      CAST(quantile_cont(delay_days, 0.25) AS INT) AS p25,
      CAST(quantile_cont(delay_days, 0.75) AS INT) AS p75
    FROM catalog_payments
    WHERE subcategory = ${q(subcategory)}
      AND fournisseur = ${q(supplier)}
      AND delay_days IS NOT NULL;
  `);
  const stats = (statsRows && statsRows.length)
    ? statsRows[0]
    : { n_payments: 0, total: 0, median_delay: 0, p25: 0, p75: 0 };

  const quartiles = {};
  const thresholds = [0.25, 0.5, 0.75, 1.0];
  for (const t of thresholds) {
    const qpoint = cumulative.find(c => c.share >= t);
    if (qpoint) quartiles[t.toFixed(2).replace(/0+$/, '').replace(/\.$/, '')] = {
      delay_days: Number(qpoint.delay_days),
      cum_amount: Number(qpoint.cum_amount)
    };
  }

  console.log(`[getProfile] sub=${subcategory} | supplier=${supplier} | rows(all)=${Number(dbgCountAll?.[0]?.n ?? 0)} | rows(series)=${series.length}`);
  if (series.length) {
    const head = series.slice(0, 5).map(r => ({ d: r.delay_days, m: Number(r.montant_total || 0) }));
    console.log("[getProfile] head(series):", head);
  } else {
    console.log("[getProfile] series is empty (no delay_days != NULL). Check order_date availability during build.");
  }
  console.log("[getProfile] quartiles:", quartiles);

  return {
    series: series.map(r => ({ delay_days: Number(r.delay_days), montant_total: Number(r.montant_total || 0) })),
    points: points.map(p => ({
      delay_days: Number(p.delay_days),
      montant: Number(p.montant || 0),
      payment_date: String(p.payment_date || ""),
      order_no: String(p.order_no || ""),
      line_no: String(p.line_no || "")
    })),
    cumulative,
    debug: {
      totalPaymentsAll: Number(dbgCountAll?.[0]?.n || 0),
      totalPaymentsWithDelay: series.length,
      sampleSeriesHead: series.slice(0, 5).map(r => ({ delay_days: r.delay_days, montant_total: Number(r.montant_total || 0) })),
    },
    stats: {
      n_payments: Number(stats.n_payments || 0),
      total: Number(stats.total || 0),
      median_delay: Number(stats.median_delay || 0),
      p25: Number(stats.p25 || 0),
      p75: Number(stats.p75 || 0),
    },
    quartiles
  };
}

/* =========================================================
   Export / Import catalogue
   ========================================================= */
export async function exportCatalog() {
  const mappings = await runSQL(`SELECT * FROM catalog_line_map;`);
  return {
    taxonomy: state.taxonomy,
    mappings,
    builtAt: state.builtAt
  };
}

export async function importCatalog(data) {
  if (!data || !Array.isArray(data.taxonomy)) throw new Error("Catalogue invalide");

  await runSQL(`
    CREATE TABLE IF NOT EXISTS catalog_line_map (
      order_no VARCHAR,
      line_no VARCHAR,
      category VARCHAR,
      subcategory VARCHAR,
      fournisseur VARCHAR
    );
  `);

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

  state.taxonomy = data.taxonomy;
  state.builtAt = new Date();

  if (Array.isArray(data.mappings)) {
    await runSQL(`DELETE FROM catalog_line_map;`);
    for (const m of data.mappings) {
      await runSQL(`
        INSERT INTO catalog_line_map (order_no, line_no, category, subcategory, fournisseur)
        VALUES (${q(m.order_no)}, ${q(m.line_no)}, ${q(m.category)}, ${q(m.subcategory)}, ${q(m.fournisseur)});
      `);
    }
  }
  return { ok: true };
}

/* =========================================================
   Utilitaire : garantir présence (cat, sub) dans canon
   ========================================================= */
function ensureInCanon(canon, category, subcategory) {
  if (!category || !subcategory) return;
  const idx = canon.findIndex(c => c.category === category);
  if (idx === -1) {
    canon.push({ category, subcategories: [subcategory] });
  } else {
    const subs = canon[idx].subcategories;
    if (!subs.includes(subcategory)) subs.push(subcategory);
  }
}
