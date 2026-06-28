import { useState } from "react";
import { fmtInt, relTime } from "../api.js";

const LANG_COLORS = ["#22d3ee", "#3b82f6", "#a855f7", "#22c55e", "#f59e0b", "#ef4444"];

export default function LiveFeed({ events }) {
  const [langFilter, setLangFilter] = useState("");
  const shown = langFilter ? events.filter((e) => e.language === langFilter) : events;
  const langs = [...new Set(events.map((e) => e.language))].slice(0, 20);

  return (
    <div className="grid cols-2">
      <div className="panel">
        <h3>Live edit stream</h3>
        <div className="controls">
          <select value={langFilter} onChange={(e) => setLangFilter(e.target.value)}>
            <option value="">All languages ({events.length})</option>
            {langs.map((l) => (
              <option key={l} value={l}>
                {l}
              </option>
            ))}
          </select>
        </div>
        <div className="feed">
          {shown.length === 0 && <div className="empty">Waiting for live events…</div>}
          {shown.map((e, i) => (
            <div className="feed-row" key={`${e.event_id}-${i}`}>
              <span className="lang">{e.language}</span>
              <span className="type">{e.event_type}</span>
              <span className="title" title={e.title}>
                {e.title}
              </span>
              <span className="meta">
                {e.bot && <span className="badge-bot">bot </span>}
                {e.is_anonymous && <span className="badge-anon">anon </span>}
                {e.user}
              </span>
            </div>
          ))}
        </div>
        <div className="panel-foot">Streaming over WebSocket from Kafka (best-effort, rate-limited).</div>
      </div>

      <div className="panel">
        <h3>Recent activity breakdown</h3>
        <LanguageBars events={events} />
      </div>
    </div>
  );
}

function LanguageBars({ events }) {
  const counts = {};
  for (const e of events) counts[e.language] = (counts[e.language] || 0) + 1;
  const rows = Object.entries(counts)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 10);
  const max = rows.length ? rows[0][1] : 1;
  return (
    <div>
      {rows.length === 0 && <div className="empty">No events yet</div>}
      {rows.map(([lang, n], i) => (
        <div key={lang} style={{ marginBottom: 10 }}>
          <div style={{ display: "flex", justifyContent: "space-between", fontSize: 12, marginBottom: 4 }}>
            <span>{lang}</span>
            <span className="muted kv">{fmtInt(n)}</span>
          </div>
          <div style={{ height: 8, background: "#0f1620", borderRadius: 4, overflow: "hidden" }}>
            <div
              style={{
                height: "100%",
                width: `${(n / max) * 100}%`,
                background: LANG_COLORS[i % LANG_COLORS.length],
                borderRadius: 4,
              }}
            />
          </div>
        </div>
      ))}
      <div className="panel-foot">Aggregated from the in-memory live buffer · {relTime(events[0]?.timestamp)}</div>
    </div>
  );
}
