import { useEffect, useMemo, useRef, useState } from "react";
import {
  Database,
  FileSpreadsheet,
  UploadCloud,
  MessageSquare,
  Send,
  PlayCircle,
  Cpu,
  CheckCircle2,
  TriangleAlert,
  Table,
  Loader2,
  Copy,
  Download,
  RefreshCw,
  ChevronDown,
  ChevronUp,
} from "lucide-react";

/**
 * Frontend Revamp for Procure Chat (client/src/App.tsx)
 * - Modern UI with iconography (lucide-react)
 * - Drag & drop multi-upload (Excel)
 * - Live backend health/model badge
 * - Rich chat bubbles + SQL badge + result table with sticky header
 * - CSV export & copy-to-clipboard
 * - Collapsible schema viewer
 * - Subtle animations and focus states
 *
 * Install missing deps from the client folder if needed:
 *   npm i lucide-react
 *
 * Optional (font & reset in index.html head):
 *   <link rel="preconnect" href="https://fonts.googleapis.com">
 *   <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
 *   <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
 */

const API = import.meta.env.VITE_API_URL || "http://localhost:8787";

type ChatItem = { role: "user" | "assistant"; content: string };
type Row = Record<string, any>;

type Health = {
  ok?: boolean;
  model?: string;
};

function Badge({ color = "#e5e7eb", text }: { color?: string; text: string }) {
  return (
    <span
      style={{
        display: "inline-flex",
        alignItems: "center",
        gap: 6,
        fontSize: 12,
        padding: "4px 8px",
        borderRadius: 999,
        background: color,
        color: "#111827",
        fontWeight: 600,
      }}
    >
      {text}
    </span>
  );
}

function subtleShadow(alpha = 0.08) {
  return `0 1px 2px rgba(0,0,0,${alpha}), 0 8px 24px rgba(0,0,0,${alpha})`;
}

export default function App() {
  const [health, setHealth] = useState<Health>({});
  const [messages, setMessages] = useState<ChatItem[]>([]);
  const [input, setInput] = useState("");
  const [schema, setSchema] = useState<any>({});
  const [rows, setRows] = useState<Row[] | null>(null);
  const [lastSQL, setLastSQL] = useState<string>("");
  const [loadingChat, setLoadingChat] = useState(false);
  const [schemaOpen, setSchemaOpen] = useState(false);

  // Upload state
  const [isDragging, setIsDragging] = useState(false);
  const [uploads, setUploads] = useState<{ name: string; status: "idle" | "ok" | "err"; msg?: string }[]>([]);
  const fileRef = useRef<HTMLInputElement>(null);

  async function refreshSchema() {
    const r = await fetch(`${API}/schema`);
    const j = await r.json();
    setSchema(j.tables || {});
  }

  async function getHealth() {
    try {
      const r = await fetch(`${API}/health`);
      const j = await r.json();
      setHealth(j);
    } catch (e) {
      setHealth({ ok: false });
    }
  }

  useEffect(() => {
    getHealth();
    refreshSchema();
  }, []);

  async function onSend() {
    if (!input.trim() || loadingChat) return;
    const question = input.trim();
    setInput("");
    setLoadingChat(true);
    const newMessages = [...messages, { role: "user" as const, content: question }];
    setMessages(newMessages);

    try {
      const r = await fetch(`${API}/chat`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ message: question, history: messages }),
      });
      const j = await r.json();
      if (j.error) throw new Error(j.error);
      setLastSQL(j.sql || "");
      setRows(j.rows || null);
      setMessages((m) => [
        ...m,
        {
          role: "assistant",
          content: j.sql ? `SQL exécuté:\n${j.sql}` : "Requête exécutée.",
        },
      ]);
    } catch (e: any) {
      setMessages((m) => [
        ...m,
        { role: "assistant", content: `Erreur: ${e?.message || e}` },
      ]);
    } finally {
      setLoadingChat(false);
    }
  }

  function onDrop(e: React.DragEvent<HTMLDivElement>) {
    e.preventDefault();
    e.stopPropagation();
    setIsDragging(false);
    const files = Array.from(e.dataTransfer.files || []).filter((f) => /\.xlsx?$/i.test(f.name));
    if (files.length) handleUpload(files);
  }

  function onFilesPicked(e: React.ChangeEvent<HTMLInputElement>) {
    const files = Array.from(e.target.files || []);
    if (files.length) handleUpload(files);
    if (fileRef.current) fileRef.current.value = "";
  }

  async function handleUpload(files: File[]) {
    // optimistic UI cards
    setUploads((u) => [
      ...u,
      ...files.map((f) => ({ name: f.name, status: "idle" as const })),
    ]);

    for (const file of files) {
      const fd = new FormData();
      fd.append("file", file);
      fd.append("table", file.name.replace(/\.xlsx?$/i, ""));
      try {
        const r = await fetch(`${API}/upload`, { method: "POST", body: fd });
        const j = await r.json();
        if (j.error) throw new Error(j.error);
        setUploads((u) =>
          u.map((it) =>
            it.name === file.name ? { ...it, status: "ok", msg: `Table ${j.table} (${j.columns?.length || 0} colonnes)` } : it
          )
        );
      } catch (e: any) {
        setUploads((u) =>
          u.map((it) => (it.name === file.name ? { ...it, status: "err", msg: e?.message || String(e) } : it))
        );
      }
    }

    await refreshSchema();
  }

  const csv = useMemo(() => {
    if (!rows || !rows.length) return "";
    const headers = Object.keys(rows[0]);
    const esc = (v: any) => {
      const s = String(v ?? "");
      return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
    };
    const body = rows.map((r) => headers.map((h) => esc(r[h])).join(",")).join("\n");
    return `${headers.join(",")}\n${body}`;
  }, [rows]);

  function copyCSV() {
    if (!csv) return;
    navigator.clipboard.writeText(csv);
  }

  function downloadCSV() {
    if (!csv) return;
    const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "resultats.csv";
    a.click();
    URL.revokeObjectURL(url);
  }

  return (
    <div
      style={{
        minHeight: "100vh",
        background: "linear-gradient(180deg, #f8fafc 0%, #ffffff 40%)",
        color: "#0f172a",
        fontFamily: "Inter, system-ui, -apple-system, Segoe UI, Roboto, Ubuntu, Cantarell, Noto Sans, Helvetica Neue, Arial, \"Apple Color Emoji\", \"Segoe UI Emoji\"",
      }}
    >
      {/* HEADER */}
      <header
        style={{
          position: "sticky",
          top: 0,
          zIndex: 10,
          backdropFilter: "saturate(180%) blur(6px)",
          background: "rgba(255,255,255,0.7)",
          borderBottom: "1px solid #e5e7eb",
        }}
      >
        <div style={{ maxWidth: 1200, margin: "0 auto", padding: "12px 20px", display: "flex", alignItems: "center", gap: 12 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
            <Database size={22} />
            <strong style={{ fontSize: 16 }}>Procure Chat</strong>
          </div>
          <div style={{ marginLeft: "auto", display: "flex", alignItems: "center", gap: 10 }}>
            <Badge color={health?.ok ? "#dcfce7" : "#fee2e2"} text={health?.ok ? "Backend OK" : "Backend KO"} />
            {health?.model && (
              <span style={{ display: "inline-flex", alignItems: "center", gap: 6, fontSize: 12, color: "#475569" }}>
                <Cpu size={16} /> {health.model}
              </span>
            )}
          </div>
        </div>
      </header>

      {/* MAIN */}
      <main style={{ maxWidth: 1200, margin: "0 auto", padding: "24px 20px", display: "grid", gap: 16 }}>
        {/* UPLOAD + SCHEMA ROW */}
        <section style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 }}>
          {/* Upload card */}
          <div
            onDragOver={(e) => {
              e.preventDefault();
              setIsDragging(true);
            }}
            onDragLeave={() => setIsDragging(false)}
            onDrop={onDrop}
            style={{
              border: `2px dashed ${isDragging ? "#2563eb" : "#cbd5e1"}`,
              background: isDragging ? "#eff6ff" : "#ffffff",
              padding: 20,
              borderRadius: 16,
              boxShadow: subtleShadow(),
              transition: "all .15s ease",
            }}
          >
            <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 8 }}>
              <UploadCloud />
              <div>
                <div style={{ fontWeight: 700 }}>Importer vos Excel</div>
                <div style={{ fontSize: 13, color: "#475569" }}>
                  Glissez-déposez ou choisissez des fichiers *.xlsx (Achats, Commandes, Décaissements)
                </div>
              </div>
            </div>
            <div style={{ display: "flex", gap: 8 }}>
              <button
                onClick={() => fileRef.current?.click()}
                style={{
                  display: "inline-flex",
                  alignItems: "center",
                  gap: 8,
                  background: "#111827",
                  color: "white",
                  border: 0,
                  borderRadius: 10,
                  padding: "10px 14px",
                  cursor: "pointer",
                }}
              >
                <FileSpreadsheet size={18} /> Choisir des fichiers
              </button>
              <button
                onClick={refreshSchema}
                title="Rafraîchir schéma"
                style={{
                  display: "inline-flex",
                  alignItems: "center",
                  gap: 8,
                  background: "#e5e7eb",
                  color: "#111827",
                  border: 0,
                  borderRadius: 10,
                  padding: "10px 14px",
                  cursor: "pointer",
                }}
              >
                <RefreshCw size={16} /> Schéma
              </button>
              <input
                ref={fileRef}
                type="file"
                accept=".xlsx"
                multiple
                onChange={onFilesPicked}
                style={{ display: "none" }}
              />
            </div>

            {!!uploads.length && (
              <div style={{ marginTop: 14, display: "grid", gap: 8 }}>
                {uploads.map((u, i) => (
                  <div
                    key={i}
                    style={{
                      display: "flex",
                      alignItems: "center",
                      gap: 10,
                      background: "#f8fafc",
                      border: "1px solid #e2e8f0",
                      padding: 10,
                      borderRadius: 10,
                    }}
                  >
                    <FileSpreadsheet size={16} />
                    <div style={{ flex: 1 }}>
                      <div style={{ fontSize: 13, fontWeight: 600 }}>{u.name}</div>
                      {u.msg && (
                        <div style={{ fontSize: 12, color: u.status === "err" ? "#b91c1c" : "#334155" }}>{u.msg}</div>
                      )}
                    </div>
                    {u.status === "ok" && <CheckCircle2 color="#16a34a" size={18} />}
                    {u.status === "err" && <TriangleAlert color="#b91c1c" size={18} />}
                    {u.status === "idle" && <Loader2 className="spin" size={18} />}
                  </div>
                ))}
              </div>
            )}
          </div>

          {/* Schema card */}
          <div
            style={{
              background: "#ffffff",
              borderRadius: 16,
              boxShadow: subtleShadow(),
              border: "1px solid #e5e7eb",
              padding: 16,
            }}
          >
            <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 8 }}>
              <Table />
              <div style={{ fontWeight: 700 }}>Schéma détecté</div>
              <button
                onClick={() => setSchemaOpen((s) => !s)}
                style={{ marginLeft: "auto", background: "transparent", border: 0, cursor: "pointer" }}
                title={schemaOpen ? "Replier" : "Déplier"}
              >
                {schemaOpen ? <ChevronUp /> : <ChevronDown />}
              </button>
            </div>
            {schemaOpen && (
              <div
                style={{
                  maxHeight: 260,
                  overflow: "auto",
                  background: "#f8fafc",
                  border: "1px solid #e2e8f0",
                  borderRadius: 12,
                  padding: 12,
                }}
              >
                <pre style={{ margin: 0, whiteSpace: "pre-wrap" }}>{JSON.stringify(schema, null, 2)}</pre>
              </div>
            )}
            {!schemaOpen && (
              <div style={{ fontSize: 13, color: "#475569" }}>
                {Object.keys(schema).length ? (
                  <span>
                    {Object.keys(schema).length} table(s) chargée(s).
                  </span>
                ) : (
                  <span>Aucune table pour le moment.</span>
                )}
              </div>
            )}
          </div>
        </section>

        {/* CHAT */}
        <section
          style={{
            background: "#ffffff",
            borderRadius: 16,
            boxShadow: subtleShadow(),
            border: "1px solid #e5e7eb",
            padding: 16,
          }}
        >
          <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 10 }}>
            <MessageSquare />
            <div style={{ fontWeight: 700 }}>Chat analytique</div>
          </div>

          <div style={{ display: "grid", gap: 8, marginBottom: 12 }}>
            {messages.map((m, i) => (
              <div
                key={i}
                style={{
                  background: m.role === "user" ? "#eff6ff" : "#f8fafc",
                  border: "1px solid #e2e8f0",
                  padding: 12,
                  borderRadius: 12,
                }}
              >
                <div style={{ fontSize: 12, fontWeight: 700, opacity: 0.7, marginBottom: 4 }}>{m.role.toUpperCase()}</div>
                <div style={{ whiteSpace: "pre-wrap" }}>{m.content}</div>
              </div>
            ))}
            {loadingChat && (
              <div style={{ display: "flex", alignItems: "center", gap: 8, color: "#475569" }}>
                <Loader2 className="spin" size={16} /> Génération de la requête…
              </div>
            )}
          </div>

          <div style={{ display: "flex", gap: 8 }}>
            <div style={{ position: "relative", flex: 1 }}>
              <input
                value={input}
                onChange={(e) => setInput(e.target.value)}
                placeholder="Ex: Top 20 des décaissements 2024 par fournisseur avec n° de commande"
                onKeyDown={(e) => {
                  if (e.key === "Enter" && !e.shiftKey) onSend();
                }}
                style={{
                  width: "100%",
                  padding: "12px 44px 12px 12px",
                  border: "1px solid #e5e7eb",
                  borderRadius: 12,
                  outline: "none",
                  fontSize: 14,
                }}
              />
              <Send
                size={18}
                onClick={onSend}
                title="Envoyer"
                style={{ position: "absolute", right: 12, top: 10, cursor: "pointer", opacity: 0.9 }}
              />
            </div>
            <button
              onClick={onSend}
              disabled={loadingChat}
              style={{
                display: "inline-flex",
                alignItems: "center",
                gap: 8,
                background: loadingChat ? "#94a3b8" : "#2563eb",
                color: "white",
                border: 0,
                borderRadius: 12,
                padding: "10px 14px",
                cursor: loadingChat ? "not-allowed" : "pointer",
              }}
            >
              <PlayCircle size={18} /> Poser la question
            </button>
          </div>
        </section>

        {/* SQL & RESULTS */}
        <section style={{ display: "grid", gridTemplateColumns: "1fr", gap: 16 }}>
          {lastSQL && (
            <div
              style={{
                background: "#ffffff",
                borderRadius: 16,
                boxShadow: subtleShadow(),
                border: "1px solid #e5e7eb",
                padding: 16,
              }}
            >
              <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 8 }}>
                <Badge color="#dbeafe" text="SQL" />
              </div>
              <pre
                style={{
                  whiteSpace: "pre-wrap",
                  margin: 0,
                  background: "#0b1220",
                  color: "#e2e8f0",
                  padding: 12,
                  borderRadius: 12,
                  overflow: "auto",
                }}
              >
                {lastSQL}
              </pre>
            </div>
          )}

          {rows && (
            <div
              style={{
                background: "#ffffff",
                borderRadius: 16,
                boxShadow: subtleShadow(),
                border: "1px solid #e5e7eb",
                padding: 16,
              }}
            >
              <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 8 }}>
                <Table />
                <div style={{ fontWeight: 700 }}>Résultats ({rows.length})</div>
                <div style={{ marginLeft: "auto", display: "flex", gap: 8 }}>
                  <button
                    onClick={copyCSV}
                    title="Copier CSV"
                    style={{ background: "#e5e7eb", border: 0, borderRadius: 10, padding: "8px 10px", cursor: "pointer" }}
                  >
                    <Copy size={16} />
                  </button>
                  <button
                    onClick={downloadCSV}
                    title="Télécharger CSV"
                    style={{ background: "#e5e7eb", border: 0, borderRadius: 10, padding: "8px 10px", cursor: "pointer" }}
                  >
                    <Download size={16} />
                  </button>
                </div>
              </div>

              <div style={{ overflow: "auto", border: "1px solid #e5e7eb", borderRadius: 12 }}>
                <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 14 }}>
                  <thead style={{ position: "sticky", top: 0, zIndex: 1, background: "#f8fafc" }}>
                    <tr>
                      {Object.keys(rows[0] || {}).map((k) => (
                        <th key={k} style={{ textAlign: "left", padding: 8, borderBottom: "1px solid #e2e8f0", whiteSpace: "nowrap" }}>
                          {k}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {rows.map((r, i) => (
                      <tr key={i}>
                        {Object.keys(rows[0] || {}).map((k) => (
                          <td key={k} style={{ padding: 8, borderBottom: "1px solid #f1f5f9", whiteSpace: "nowrap" }}>
                            {String(r[k])}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </section>
      </main>

      {/* tiny CSS helpers */}
      <style>{`
        .spin { animation: spin 1s linear infinite; }
        @keyframes spin { from { transform: rotate(0deg);} to { transform: rotate(360deg);} }
        ::selection { background: #bfdbfe; }
        button:focus-visible, input:focus-visible { outline: 2px solid #93c5fd; outline-offset: 2px; }
      `}</style>
    </div>
  );
}
