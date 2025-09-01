// server/src/llm.js
import OpenAI from "openai";

const client = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY || "ollama",
  baseURL: process.env.OPENAI_BASE_URL || "http://127.0.0.1:11434/v1",
});

const MODEL = process.env.OLLAMA_MODEL || "gpt-oss:20b";

export async function suggestSQL({ schema, question, history }) {
  const system = `Tu es un expert SQL (DuckDB). Tu écris uniquement du SQL sécurisé (SELECT ...).
Règles:
- Base: DuckDB en mémoire.
- Schéma disponible (tables, colonnes avec types). N'utilise QUE ces tables/colonnes.
- Pas de DDL/DML (pas de CREATE/INSERT/UPDATE/DELETE).
- Préfère des noms normalisés (snake_case).
- Formate la requête.
- Ajoute un court commentaire /*explication*/ en première ligne.`;

  const schemaText = Object.entries(schema).map(([table, cols]) => {
    const lines = cols.map(c => `  - ${c.name} ${c.type}${c.original ? `  -- original: ${c.original}` : ""}`).join("\n");
    return `TABLE ${table}:\n${lines}`;
  }).join("\n\n");

  const user = `Question: ${question}

Schéma:
${schemaText}

Renvoie UNIQUEMENT le SQL, rien d'autre.`;

  const messages = [
    { role: "system", content: system },
    ...(history || []).slice(-4),
    { role: "user", content: user },
  ];

  const resp = await client.chat.completions.create({
    model: MODEL,
    messages,
    temperature: 0.2,
  });

  const text = resp.choices?.[0]?.message?.content || "";
  const m = text.match(/```sql([\s\S]*?)```/i);
  const sql = (m ? m[1] : text).trim();
  return sql;
}
