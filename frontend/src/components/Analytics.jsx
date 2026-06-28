import { useEffect, useState } from "react";
import { fetchJSON, fmtInt } from "../api.js";
import {
  BarChart,
  Bar,
  LineChart,
  Line,
  ResponsiveContainer,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
  PieChart,
  Pie,
  Cell,
  Legend,
} from "recharts";

const PIE_COLORS = ["#3b82f6", "#22d3ee", "#a855f7", "#22c55e", "#f59e0b", "#ef4444", "#ec4899", "#14b8a6", "#f97316", "#8b5cf6"];

export default function Analytics() {
  const [hours, setHours] = useState(24);
  const [data, setData] = useState(null);
  const [err, setErr] = useState(null);

  useEffect(() => {
    async function load() {
      try {
        const [ts, langs, pages, users] = await Promise.all([
          fetchJSON(`/api/analytics/timeseries?hours=${hours}`),
          fetchJSON(`/api/analytics/languages?hours=${hours}&limit=10`),
          fetchJSON(`/api/top-pages?limit=12`),
          fetchJSON(`/api/top-users?limit=12`),
        ]);
        setData({ ts, langs, pages, users });
        setErr(null);
      } catch (e) {
        setErr(e.message);
      }
    }
    load();
    const id = setInterval(load, 30000);
    return () => clearInterval(id);
  }, [hours]);

  if (err) return <div className="error-box">{err}</div>;
  if (!data) return <div className="empty">Loading analytics…</div>;

  const tsData = data.ts.map((p) => ({ hour: p.hour.slice(11, 16), edits: p.total_edits, minor: p.minor_edits }));
  const langData = data.langs.map((l) => ({ name: l.language, value: l.edits }));

  return (
    <div className="grid cols-2">
      <div className="panel" style={{ gridColumn: "1 / -1" }}>
        <div className="controls">
          <label className="muted">Window:</label>
          <select value={hours} onChange={(e) => setHours(Number(e.target.value))}>
            <option value={6}>last 6h</option>
            <option value={24}>last 24h</option>
            <option value={72}>last 72h</option>
            <option value={168}>last 7d</option>
          </select>
        </div>
      </div>

      <div className="panel">
        <h3>Edits over time</h3>
        <div style={{ height: 240 }}>
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={tsData} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#1f2a3a" />
              <XAxis dataKey="hour" stroke="#8aa0bd" fontSize={11} />
              <YAxis stroke="#8aa0bd" fontSize={11} />
              <Tooltip contentStyle={{ background: "#131a26", border: "1px solid #1f2a3a", borderRadius: 8 }} />
              <Line type="monotone" dataKey="edits" stroke="#22d3ee" strokeWidth={2} dot={false} />
              <Line type="monotone" dataKey="minor" stroke="#a855f7" strokeWidth={1.5} dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </div>
        <div className="legend">
          <span style={{ color: "#22d3ee" }}>total</span>
          <span style={{ color: "#a855f7" }}>minor</span>
        </div>
      </div>

      <div className="panel">
        <h3>Edits by language</h3>
        <div style={{ height: 240 }}>
          <ResponsiveContainer width="100%" height="100%">
            <PieChart>
              <Pie data={langData} dataKey="value" nameKey="name" cx="50%" cy="50%" outerRadius={85} innerRadius={45} paddingAngle={2}>
                {langData.map((_, i) => (
                  <Cell key={i} fill={PIE_COLORS[i % PIE_COLORS.length]} />
                ))}
              </Pie>
              <Tooltip contentStyle={{ background: "#131a26", border: "1px solid #1f2a3a", borderRadius: 8 }} />
              <Legend wrapperStyle={{ fontSize: 11, color: "#8aa0bd" }} />
            </PieChart>
          </ResponsiveContainer>
        </div>
      </div>

      <div className="panel">
        <h3>Top pages</h3>
        <div style={{ height: 240 }}>
          <ResponsiveContainer width="100%" height="100%">
            <BarChart
              data={data.pages.slice(0, 8).map((p) => ({ name: p.page_title.slice(0, 18), edits: p.edits }))}
              layout="vertical"
              margin={{ left: 8, right: 16 }}
            >
              <CartesianGrid strokeDasharray="3 3" stroke="#1f2a3a" horizontal={false} />
              <XAxis type="number" stroke="#8aa0bd" fontSize={11} />
              <YAxis type="category" dataKey="name" stroke="#8aa0bd" fontSize={10} width={110} />
              <Tooltip contentStyle={{ background: "#131a26", border: "1px solid #1f2a3a", borderRadius: 8 }} />
              <Bar dataKey="edits" fill="#3b82f6" radius={[0, 4, 4, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      <div className="panel">
        <h3>Top users</h3>
        <table>
          <thead>
            <tr>
              <th>User</th>
              <th>Type</th>
              <th className="num">Edits</th>
            </tr>
          </thead>
          <tbody>
            {data.users.map((u, i) => (
              <tr key={`${u.username}-${i}`}>
                <td>{u.username}</td>
                <td className="muted">{u.is_bot ? "bot" : "human"}</td>
                <td className="num">{fmtInt(u.edits)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
