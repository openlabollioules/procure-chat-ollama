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
  FolderTree,
  Wand2,
  Building2,
} from "lucide-react";
import {
  ResponsiveContainer,
  BarChart,
  Bar,
  CartesianGrid,
  XAxis,
  YAxis,
  Tooltip,
  LineChart,
  Line,
  Legend,
} from "recharts";

/**
 * Procure Chat — Frontend Revamp + Catalogue de catégories & profils de décaissement
 * - Onglets Chat / Catalogue
 * - Construction du catalogue (LLM) via /catalog/build
 * - Sélection Catégorie > Sous-catégorie > Fournisseur
 * - Graphique des paiements (montant par délai en jours) + stats
 *
 * Dépendances supplémentaires (client/):
 *   npm i lucide-react recharts
 */

const API = import.meta.env.VITE_API_URL || "http://localhost:8787";

type ChatItem = { role: "user" | "assistant"; content: string };
type Row = Record<string, any>;

type Health = { ok?: boolean; model?: string };

type TaxoNode = { category: string; subcategories: string[] };

type CatalogSummary = {
  taxonomy: TaxoNode[];
  suppliers: string[];
  byCategory: Record<string, string[]>; // category -> subcategories
  bySubcategorySupplier: Record<string, string[]>; // subcategory -> suppliers
  counts?: Record<string, number>; // optional usage counts per subcategory
};

type ProfileResp = {
  points: { delay_days: number; montant: number; payment_date: string; order_no: string; line_no: string }[];
  series: { delay_days: number; montant_total: number }[];
  cumulative: { delay_days: number; cum_amount: number; share: number }[];
  stats: { n_payments: number; total: number; median_delay: number; p25: number; p75: number };
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
  // Health + schema
  const [health, setHealth] = useState<Health>({});
  const [schema, setSchema] = useState<any>({});

  // Tabs
  const [tab, setTab] = useState<"chat" | "catalog">("chat");

  // Chat state
  const [messages, setMessages] = useState<ChatItem[]>([]);
  const [input, setInput] = useState("");
  const [rows, setRows] = useState<Row[] | null>(null);
  const [lastSQL, setLastSQL] = useState<string>("");
  const [loadingChat, setLoadingChat] = useState(false);

  // Upload state
  const [isDragging, setIsDragging] = useState(false);
  const [uploads, setUploads] = useState<{ name: string; status: "idle" | "ok" | "err"; msg?: string }[]>([]);
  const fileRef = useRef<HTMLInputElement>(null);

  // Catalog state
  const [building, setBuilding] = useState(false);
  const [summary, setSummary] = useState<CatalogSummary | null>(null);
  const [cat, setCat] = useState<string>("");
  const [sub, setSub] = useState<string>("");
  const [sup, setSup] = useState<string>("");
  const [profile, setProfile] = useState<ProfileResp | null>(null);

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
    fetchSummary();
  }, []);

  // -----------------
  // CHAT
  // -----------------
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
        { role: "assistant", content: j.sql ? `SQL exécuté:\n${j.sql}` : "Requête exécutée." },
      ]);
    } catch (e: any) {
      setMessages((m) => [...m, { role: "assistant", content: `Erreur: ${e?.message || e}` }]);
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
    setUploads((u) => [...u, ...files.map((f) => ({ name: f.name, status: "idle" as const }))]);

    for (const file of files) {
      const fd = new FormData();
      fd.append("file", file);
      fd.append("table", file.name.replace(/\.xlsx?$/i, ""));
      try {
        const r = await fetch(`${API}/upload`, { method: "POST", body: fd });
        const j = await r.json();
        if (j.error) throw new Error(j.error);
        setUploads((u) =>
          u.map((it) => (it.name === file.name ? { ...it, status: "ok", msg: `Table ${j.table} (${j.columns?.length || 0} colonnes)` } : it))
        );
      } catch (e: any) {
        setUploads((u) => u.map((it) => (it.name === file.name ? { ...it, status: "err", msg: e?.message || String(e) } : it)));
      }
    }

    await refreshSchema();
    await fetchSummary();
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

  // -----------------
  // CATALOGUE
  // -----------------
  async function buildCatalog() {
    setBuilding(true);
    try {
      const r = await fetch(`${API}/catalog/build`, { method: "POST" });
      const j = await r.json();
      if (j.error) throw new Error(j.error);
      await fetchSummary();
    } catch (e: any) {
      alert(e?.message || String(e));
    } finally {
      setBuilding(false);
    }
  }

  async function fetchSummary() {
    try {
      const r = await fetch(`${API}/catalog/summary`);
      const j: CatalogSummary = await r.json();
      if ((j as any).error) throw new Error((j as any).error);
      setSummary(j);
      // reset selections if obsolete
      if (j && cat && !(j.byCategory[cat]?.length)) {
        setCat(""); setSub(""); setSup("");
      }
    } catch (e) {
      // ignore silently if not built yet
    }
  }

  async function fetchProfile() {
    if (!sub || !sup) return;
    const q = new URLSearchParams({ subcategory: sub, supplier: sup });
    const r = await fetch(`${API}/catalog/profile?${q.toString()}`);
    const j: any = await r.json();
    if (j.error) { alert(j.error); return; }
    setProfile(j as ProfileResp);
  }

  useEffect(() => {
    if (sub && sup) fetchProfile();
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sub, sup]);

  const subcats = useMemo(() => (cat && summary ? summary.byCategory[cat] || [] : []), [cat, summary]);
  const suppliers = useMemo(() => (sub && summary ? summary.bySubcategorySupplier[sub] || [] : []), [sub, summary]);

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
        style={{ position: "sticky", top: 0, zIndex: 10, backdropFilter: "saturate(180%) blur(6px)", background: "rgba(255,255,255,0.7)", borderBottom: "1px solid #e5e7eb" }}
      >
        <div style={{ maxWidth: 1200, margin: "0 auto", padding: "12px 20px", display: "flex", alignItems: "center", gap: 12 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
            <Database size={22} />
            <strong style={{ fontSize: 16 }}>Procure Chat</strong>
          </div>
          <nav style={{ marginLeft: 24, display: "flex", gap: 8 }}>
            <button onClick={() => setTab("chat")} style={{ padding: "8px 10px", borderRadius: 10, border: 0, background: tab === "chat" ? "#111827" : "transparent", color: tab === "chat" ? "#fff" : "#111827", cursor: "pointer" }}>Chat</button>
            <button onClick={() => setTab("catalog")} style={{ padding: "8px 10px", borderRadius: 10, border: 0, background: tab === "catalog" ? "#111827" : "transparent", color: tab === "catalog" ? "#fff" : "#111827", cursor: "pointer" }}>Catalogue</button>
          </nav>
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
        {tab === "chat" && (
          <>
            {/* UPLOAD + SCHEMA ROW */}
            <section style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 }}>
              {/* Upload card */}
              <div
                onDragOver={(e) => { e.preventDefault(); setIsDragging(true); }}
                onDragLeave={() => setIsDragging(false)}
                onDrop={onDrop}
                style={{ border: `2px dashed ${isDragging ? "#2563eb" : "#cbd5e1"}`, background: isDragging ? "#eff6ff" : "#ffffff", padding: 20, borderRadius: 16, boxShadow: subtleShadow(), transition: "all .15s ease" }}
              >
                <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 8 }}>
                  <UploadCloud />
                  <div>
                    <div style={{ fontWeight: 700 }}>Importer vos Excel</div>
                    <div style={{ fontSize: 13, color: "#475569" }}>Glissez-déposez ou choisissez des fichiers *.xlsx (Achats, Commandes, Décaissements)</div>
                  </div>
                </div>
                <div style={{ display: "flex", gap: 8 }}>
                  <button onClick={() => fileRef.current?.click()} style={{ display: "inline-flex", alignItems: "center", gap: 8, background: "#111827", color: "white", border: 0, borderRadius: 10, padding: "10px 14px", cursor: "pointer" }}>
                    <FileSpreadsheet size={18} /> Choisir des fichiers
                  </button>
                  <button onClick={refreshSchema} title="Rafraîchir schéma" style={{ display: "inline-flex", alignItems: "center", gap: 8, background: "#e5e7eb", color: "#111827", border: 0, borderRadius: 10, padding: "10px 14px", cursor: "pointer" }}>
                    <RefreshCw size={16} /> Schéma
                  </button>
                  <input ref={fileRef} type="file" accept=".xlsx" multiple onChange={onFilesPicked} style={{ display: "none" }} />
                </div>
                {!!uploads.length && (
                  <div style={{ marginTop: 14, display: "grid", gap: 8 }}>
                    {uploads.map((u, i) => (
                      <div key={i} style={{ display: "flex", alignItems: "center", gap: 10, background: "#f8fafc", border: "1px solid #e2e8f0", padding: 10, borderRadius: 10 }}>
                        <FileSpreadsheet size={16} />
                        <div style={{ flex: 1 }}>
                          <div style={{ fontSize: 13, fontWeight: 600 }}>{u.name}</div>
                          {u.msg && <div style={{ fontSize: 12, color: u.status === "err" ? "#b91c1c" : "#334155" }}>{u.msg}</div>}
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
              <div style={{ background: "#ffffff", borderRadius: 16, boxShadow: subtleShadow(), border: "1px solid #e5e7eb", padding: 16 }}>
                <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 8 }}>
                  <Table />
                  <div style={{ fontWeight: 700 }}>Schéma détecté</div>
                </div>
                <div style={{ maxHeight: 260, overflow: "auto", background: "#f8fafc", border: "1px solid #e2e8f0", borderRadius: 12, padding: 12 }}>
                  <pre style={{ margin: 0, whiteSpace: "pre-wrap" }}>{JSON.stringify(schema, null, 2)}</pre>
                </div>
              </div>
            </section>

            {/* CHAT */}
            <section style={{ background: "#ffffff", borderRadius: 16, boxShadow: subtleShadow(), border: "1px solid #e5e7eb", padding: 16 }}>
              <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 10 }}>
                <MessageSquare />
                <div style={{ fontWeight: 700 }}>Chat analytique</div>
              </div>

              <div style={{ display: "grid", gap: 8, marginBottom: 12 }}>
                {messages.map((m, i) => (
                  <div key={i} style={{ background: m.role === "user" ? "#eff6ff" : "#f8fafc", border: "1px solid #e2e8f0", padding: 12, borderRadius: 12 }}>
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
                    onKeyDown={(e) => { if (e.key === "Enter" && !e.shiftKey) onSend(); }}
                    style={{ width: "100%", padding: "12px 44px 12px 12px", border: "1px solid #e5e7eb", borderRadius: 12, outline: "none", fontSize: 14 }}
                  />
                  <button
                    type="button"
                    onClick={onSend}
                    aria-label="Envoyer"
                    style={{
                      position: "absolute",
                      right: 8,
                      top: 6,
                      background: "transparent",
                      border: 0,
                      padding: 6,
                      cursor: "pointer",
                    }}
                  >
                    <Send size={18} />
                  </button>
                </div>
                <button onClick={onSend} disabled={loadingChat} style={{ display: "inline-flex", alignItems: "center", gap: 8, background: loadingChat ? "#94a3b8" : "#2563eb", color: "white", border: 0, borderRadius: 12, padding: "10px 14px", cursor: loadingChat ? "not-allowed" : "pointer" }}>
                  <PlayCircle size={18} /> Poser la question
                </button>
              </div>
            </section>

            {/* SQL & RESULTS */}
            <section style={{ display: "grid", gridTemplateColumns: "1fr", gap: 16 }}>
              {lastSQL && (
                <div style={{ background: "#ffffff", borderRadius: 16, boxShadow: subtleShadow(), border: "1px solid #e5e7eb", padding: 16 }}>
                  <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 8 }}>
                    <Badge color="#dbeafe" text="SQL" />
                  </div>
                  <pre style={{ whiteSpace: "pre-wrap", margin: 0, background: "#0b1220", color: "#e2e8f0", padding: 12, borderRadius: 12, overflow: "auto" }}>{lastSQL}</pre>
                </div>
              )}

              {rows && (
                <div style={{ background: "#ffffff", borderRadius: 16, boxShadow: subtleShadow(), border: "1px solid #e5e7eb", padding: 16 }}>
                  <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 8 }}>
                    <Table />
                    <div style={{ fontWeight: 700 }}>Résultats ({rows.length})</div>
                    <div style={{ marginLeft: "auto", display: "flex", gap: 8 }}>
                      <button onClick={copyCSV} title="Copier CSV" style={{ background: "#e5e7eb", border: 0, borderRadius: 10, padding: "8px 10px", cursor: "pointer" }}>
                        <Copy size={16} />
                      </button>
                      <button onClick={downloadCSV} title="Télécharger CSV" style={{ background: "#e5e7eb", border: 0, borderRadius: 10, padding: "8px 10px", cursor: "pointer" }}>
                        <Download size={16} />
                      </button>
                    </div>
                  </div>

                  <div style={{ overflow: "auto", border: "1px solid #e5e7eb", borderRadius: 12 }}>
                    <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 14 }}>
                      <thead style={{ position: "sticky", top: 0, zIndex: 1, background: "#f8fafc" }}>
                        <tr>
                          {Object.keys(rows[0] || {}).map((k) => (
                            <th key={k} style={{ textAlign: "left", padding: 8, borderBottom: "1px solid #e2e8f0", whiteSpace: "nowrap" }}>{k}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {rows.map((r, i) => (
                          <tr key={i}>
                            {Object.keys(rows[0] || {}).map((k) => (
                              <td key={k} style={{ padding: 8, borderBottom: "1px solid #f1f5f9", whiteSpace: "nowrap" }}>{String(r[k])}</td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
            </section>
          </>
        )}

        {tab === "catalog" && (
          <>
            {/* BUILD */}
            <section style={{ background: "#ffffff", borderRadius: 16, boxShadow: subtleShadow(), border: "1px solid #e5e7eb", padding: 16 }}>
              <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                <FolderTree />
                <div style={{ fontWeight: 700 }}>Catalogue de catégories</div>
                <div style={{ marginLeft: "auto", display: "flex", gap: 8 }}>
                  <button onClick={buildCatalog} disabled={building} style={{ display: "inline-flex", alignItems: "center", gap: 8, background: building ? "#94a3b8" : "#111827", color: "#fff", border: 0, borderRadius: 10, padding: "10px 14px", cursor: building ? "not-allowed" : "pointer" }}>
                    <Wand2 size={16} /> {building ? "Construction…" : "Construire / Mettre à jour"}
                  </button>
                </div>
              </div>
              <div style={{ marginTop: 10, fontSize: 13, color: "#475569" }}>
                Le LLM analyse l'historique Achats (type ligne, description commande, description ligne), crée une hiérarchie catégories → sous-catégories, et associe les lignes. Les profils de décaissement sont ensuite calculés par sous-catégorie × fournisseur à partir des Décaissements (liens par N° commande et N° ligne commande).
              </div>
            </section>

            {/* PICKERS */}
            <section style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 }}>
              <div style={{ background: "#ffffff", borderRadius: 16, boxShadow: subtleShadow(), border: "1px solid #e5e7eb", padding: 16 }}>
                <div style={{ display: "grid", gap: 10 }}>
                  <label style={{ fontWeight: 700 }}>Catégorie</label>
                  <select value={cat} onChange={(e) => { setCat(e.target.value); setSub(""); setSup(""); }} style={{ padding: 10, border: "1px solid #e5e7eb", borderRadius: 10 }}>
                    <option value="">— choisir —</option>
                    {summary?.taxonomy.map((t) => (
                      <option key={t.category} value={t.category}>{t.category}</option>
                    ))}
                  </select>

                  <label style={{ fontWeight: 700 }}>Sous-catégorie</label>
                  <select value={sub} onChange={(e) => { setSub(e.target.value); setSup(""); }} style={{ padding: 10, border: "1px solid #e5e7eb", borderRadius: 10 }}>
                    <option value="">— choisir —</option>
                    {subcats.map((s) => (
                      <option key={s} value={s}>{s}</option>
                    ))}
                  </select>

                  <label style={{ fontWeight: 700 }}>Fournisseur</label>
                  <select value={sup} onChange={(e) => setSup(e.target.value)} style={{ padding: 10, border: "1px solid #e5e7eb", borderRadius: 10 }}>
                    <option value="">— choisir —</option>
                    {suppliers.map((s) => (
                      <option key={s} value={s}>{s}</option>
                    ))}
                  </select>
                </div>
              </div>

              <div style={{ background: "#ffffff", borderRadius: 16, boxShadow: subtleShadow(), border: "1px solid #e5e7eb", padding: 16 }}>
                <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 8 }}>
                  <Building2 />
                  <div style={{ fontWeight: 700 }}>Aperçu du catalogue</div>
                </div>
                <div style={{ maxHeight: 280, overflow: "auto", background: "#f8fafc", border: "1px solid #e2e8f0", borderRadius: 12, padding: 12 }}>
                  {!summary ? (
                    <div style={{ color: "#64748b" }}>Aucun catalogue encore construit.</div>
                  ) : (
                    <ul style={{ margin: 0, paddingLeft: 18 }}>
                      {summary.taxonomy.map((t) => (
                        <li key={t.category}>
                          <b>{t.category}</b>
                          <ul>
                            {t.subcategories.map((s) => (
                              <li key={s}>{s} {summary.counts && summary.counts[s] ? <span style={{ color: "#64748b" }}>({summary.counts[s]})</span> : null}</li>
                            ))}
                          </ul>
                        </li>
                      ))}
                    </ul>
                  )}
                </div>
              </div>
            </section>

            {/* CHART */}
            <section style={{ background: "#ffffff", borderRadius: 16, boxShadow: subtleShadow(), border: "1px solid #e5e7eb", padding: 16 }}>
              <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 8 }}>
                <Badge color="#dcfce7" text="Profils de décaissement" />
                {!!profile && (
                  <div style={{ marginLeft: "auto", display: "flex", gap: 16, color: "#334155", fontSize: 13 }}>
                    <div>Obs: <b>{profile.stats.n_payments}</b></div>
                    <div>Total: <b>{profile.stats.total.toLocaleString()}</b></div>
                    <div>Md: <b>{profile.stats.median_delay} j</b></div>
                    <div>P25/P75: <b>{profile.stats.p25} / {profile.stats.p75} j</b></div>
                  </div>
                )}
              </div>

              {!sub || !sup ? (
                <div style={{ color: "#64748b" }}>Sélectionnez une sous-catégorie et un fournisseur pour visualiser le profil.</div>
              ) : !profile ? (
                <div style={{ display: "flex", alignItems: "center", gap: 8, color: "#475569" }}>
                  <Loader2 className="spin" size={16} /> Chargement du profil…
                </div>
              ) : (
                <div style={{ height: 360 }}>
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={profile.series} margin={{ top: 10, right: 20, bottom: 10, left: 0 }}>
                      <CartesianGrid strokeDasharray="3 3" />
                      <XAxis dataKey="delay_days" label={{ value: "Délai (jours)", position: "insideBottom", offset: -5 }} />
                      <YAxis label={{ value: "Montant", angle: -90, position: "insideLeft" }} />
                      <Tooltip />
                      <Legend />
                      <Bar dataKey="montant_total" name="Montant par délai" />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              )}
            </section>
          </>
        )}
      </main>

      {/* tiny CSS helpers */}
      <style>{`
        .spin { animation: spin 1s linear infinite; }
        @keyframes spin { from { transform: rotate(0deg);} to { transform: rotate(360deg);} }
        ::selection { background: #bfdbfe; }
        button:focus-visible, input:focus-visible, select:focus-visible { outline: 2px solid #93c5fd; outline-offset: 2px; }
      `}</style>
    </div>
  );
}
