import { useEffect, useState } from "react";
import { fetchJSON, fmtInt, fmtBytes, relTime } from "../api.js";
import { AreaChart, Area, ResponsiveContainer, XAxis, YAxis, Tooltip, CartesianGrid } from "recharts";

export default function Overview({ liveEvents, liveRate }) {
  const [overview, setOverview] = useState(null);
  const [ts, setTs] = useState([]);
  const [langs, setLangs] = useState([]);
  const [err, setErr] = useState(null);

  useEffect(() => {
    async function load() {
      try {
        const [ov, series, top] = await Promise.all([
          fetchJSON("/api/overview"),
          fetchJSON("/api/analytics/timeseries?hours=24"),
          fetchJSON("/api/top-languages?limit=6"),
        ]);
        setOverview(ov);
        setTs(series.map((p) => ({ hour: p.hour.slice(11, 16), edits: p.total_edits })));
        setLangs(top);
        setErr(null);
      } catch (e) {
        setErr(e.message);
      }
    }
    load();
    const id = setInterval(load, 30000);
    return () => clearInterval(id);
  }, []);

  return (
    <div className="grid cols-4">
      <Tile label="Total edits (latest hour)" value={fmtInt(overview?.total_edits)} sub={`hour ${relTime(overview?.hour)}`} />
      <Tile label="Active users" value={fmtInt(overview?.active_users)} sub={`distinct in hour`} />
      <Tile label="Active pages" value={fmtInt(overview?.active_pages)} sub={`distinct in hour`} />
      <Tile label="Languages" value={fmtInt(overview?.distinct_languages)} sub={`distinct in hour`} />
      <Tile label="Minor edits" value={fmtInt(overview?.minor_edits)} sub="latest hour" />
      <Tile label="Bytes added" value={fmtBytes(overview?.total_bytes_added)} sub="latest hour" />
      <Tile label="Bytes removed" value={fmtBytes(overview?.total_bytes_removed)} sub="latest hour" />
      <Tile label="Live edits/min" value={fmtInt(liveRate)} sub="client estimate" accent />

      <div className="panel" style={{ gridColumn: "1 / -1" }}>
        <h3>Edits per hour · last 24h</h3>
        {err && <div className="error-box">{err}</div>}
        <div style={{ height: 240 }}>
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={ts} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
              <defs>
                <linearGradient id="g" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="#22d3ee" stopOpacity={0.6} />
                  <stop offset="100%" stopColor="#22d3ee" stopOpacity={0.02} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="#1f2a3a" />
              <XAxis dataKey="hour" stroke="#8aa0bd" fontSize={11} />
              <YAxis stroke="#8aa0bd" fontSize={11} />
              <Tooltip
                contentStyle={{ background: "#131a26", border: "1px solid #1f2a3a", borderRadius: 8 }}
                labelStyle={{ color: "#8aa0bd" }}
              />
              <Area type="monotone" dataKey="edits" stroke="#22d3ee" strokeWidth={2} fill="url(#g)" />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      </div>

      <div className="panel" style={{ gridColumn: "1 / -1" }}>
        <h3>Top languages</h3>
        <div className="legend">
          {langs.map((l) => (
            <span key={l.language} style={{ color: "#e6edf6" }}>
              {l.language} · {fmtInt(l.edits)}
            </span>
          ))}
          {!langs.length && <span className="empty">No data yet</span>}
        </div>
      </div>
    </div>
  );
}

function Tile({ label, value, sub, accent }) {
  return (
    <div className="tile">
      <div className="label">{label}</div>
      <div className="value" style={{ color: accent ? "#22d3ee" : undefined }}>
        {value}
      </div>
      {sub && <div className="delta">{sub}</div>}
    </div>
  );
}
