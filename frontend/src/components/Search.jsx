import { useEffect, useState } from "react";
import { fetchJSON, fmtInt, relTime } from "../api.js";

export default function Search() {
  const [q, setQ] = useState("");
  const [language, setLanguage] = useState("");
  const [bot, setBot] = useState("");
  const [anon, setAnon] = useState("");
  const [result, setResult] = useState(null);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState(null);

  useEffect(() => {
    run();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  async function run(e) {
    e?.preventDefault();
    setLoading(true);
    setErr(null);
    try {
      const params = new URLSearchParams({ size: "50" });
      if (q) params.set("q", q);
      if (language) params.set("language", language);
      if (bot) params.set("bot", bot);
      if (anon) params.set("anonymous", anon);
      const r = await fetchJSON(`/api/live/search?${params}`);
      setResult(r);
    } catch (ex) {
      setErr(ex.message);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="panel">
      <h3>Live search · Elasticsearch (within retention window)</h3>
      <form className="controls" onSubmit={run}>
        <input type="text" placeholder="Search title, user, or comment…" value={q} onChange={(e) => setQ(e.target.value)} />
        <input type="text" placeholder="language (e.g. en)" value={language} onChange={(e) => setLanguage(e.target.value)} style={{ maxWidth: 140 }} />
        <select value={bot} onChange={(e) => setBot(e.target.value)}>
          <option value="">any user</option>
          <option value="true">bots only</option>
          <option value="false">humans only</option>
        </select>
        <select value={anon} onChange={(e) => setAnon(e.target.value)}>
          <option value="">anon+registered</option>
          <option value="true">anonymous only</option>
          <option value="false">registered only</option>
        </select>
        <button type="submit">{loading ? "Searching…" : "Search"}</button>
      </form>

      {err && <div className="error-box">{err}</div>}
      {result?.error && <div className="error-box">Elasticsearch: {result.error}</div>}

      <div className="muted" style={{ marginBottom: 8 }}>
        {result ? `${fmtInt(result.total)} hits · ${result.took_ms ?? "—"} ms` : ""}
      </div>

      <table>
        <thead>
          <tr>
            <th>Time</th>
            <th>Lang</th>
            <th>Title</th>
            <th>User</th>
            <th className="num">+B</th>
            <th className="num">−B</th>
          </tr>
        </thead>
        <tbody>
          {(result?.hits || []).map((h, i) => (
            <tr key={`${h.event_id || i}`}>
              <td className="muted">{relTime(h.timestamp || h["@timestamp"])}</td>
              <td>{h.language}</td>
              <td>{h.title}</td>
              <td>
                {h.bot && <span className="badge-bot">bot </span>}
                {h.user}
              </td>
              <td className="num">{h.bytes_added ? fmtInt(h.bytes_added) : "—"}</td>
              <td className="num">{h.bytes_removed ? fmtInt(h.bytes_removed) : "—"}</td>
            </tr>
          ))}
          {result && !result.hits.length && (
            <tr>
              <td colSpan={6}>
                <div className="empty">No results</div>
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
}
