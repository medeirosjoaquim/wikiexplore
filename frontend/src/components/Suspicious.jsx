import { useEffect, useState } from "react";
import { fetchJSON, relTime } from "../api.js";

function scoreClass(s) {
  if (s >= 0.75) return "high";
  if (s >= 0.5) return "mid";
  return "low";
}

export default function Suspicious() {
  const [rows, setRows] = useState([]);
  const [err, setErr] = useState(null);

  useEffect(() => {
    async function load() {
      try {
        setRows(await fetchJSON("/api/suspicious?limit=100"));
        setErr(null);
      } catch (e) {
        setErr(e.message);
      }
    }
    load();
    const id = setInterval(load, 15000);
    return () => clearInterval(id);
  }, []);

  return (
    <div className="panel">
      <h3>Suspicious edits · vandalism detection candidates</h3>
      {err && <div className="error-box">{err}</div>}
      <table>
        <thead>
          <tr>
            <th>Detected</th>
            <th>Lang</th>
            <th>Page</th>
            <th>User</th>
            <th>Score</th>
            <th>Reasons</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr key={r.id}>
              <td className="muted">{relTime(r.detected_at)}</td>
              <td>{r.language}</td>
              <td>{r.page_title}</td>
              <td>{r.username || <span className="muted">anonymous</span>}</td>
              <td>
                <span className={`score ${scoreClass(r.score)}`}>{r.score.toFixed(2)}</span>
              </td>
              <td className="muted kv">{r.reason}</td>
            </tr>
          ))}
          {!rows.length && (
            <tr>
              <td colSpan={6}>
                <div className="empty">No suspicious edits detected yet</div>
              </td>
            </tr>
          )}
        </tbody>
      </table>
      <div className="panel-foot">
        Scored by the vandalism consumer from ``wiki.vandalism`` · threshold configurable via VANDALISM_THRESHOLD.
      </div>
    </div>
  );
}
