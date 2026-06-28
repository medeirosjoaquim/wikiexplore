import { useEffect, useState } from "react";
import { fetchJSON, fmtInt } from "../api.js";

export default function Monitoring() {
  const [health, setHealth] = useState(null);
  const [system, setSystem] = useState(null);
  const [err, setErr] = useState(null);

  useEffect(() => {
    async function load() {
      try {
        const [h, s] = await Promise.all([fetchJSON("/health"), fetchJSON("/api/system")]);
        setHealth(h);
        setSystem(s);
        setErr(null);
      } catch (e) {
        setErr(e.message);
      }
    }
    load();
    const id = setInterval(load, 10000);
    return () => clearInterval(id);
  }, []);

  if (err) return <div className="error-box">{err}</div>;
  if (!health) return <div className="empty">Loading system status…</div>;

  const checks = [
    { name: "PostgreSQL", ok: health.postgres === "healthy", detail: health.postgres },
    { name: "Kafka", ok: health.kafka === "healthy", detail: health.kafka },
    { name: "Elasticsearch", ok: health.elasticsearch === "healthy", detail: health.elasticsearch },
    { name: "Migrations", ok: health.migrations === "up_to_date", detail: health.migrations },
  ];

  return (
    <div className="grid cols-2">
      <div className="panel">
        <h3>Infrastructure health</h3>
        <div className="health-row">
          <div className="name">
            Overall status
            <small>{health.status}</small>
          </div>
          <span className={`dot ${health.status === "healthy" ? "connected" : "disconnected"}`} />
        </div>
        {checks.map((c) => (
          <div className="health-row" key={c.name}>
            <div className="name">
              {c.name}
              <small>{c.detail}</small>
            </div>
            <span className={`dot ${c.ok ? "connected" : "disconnected"}`} />
          </div>
        ))}
        <div className="panel-foot">
          ES aliases: read={String(health.live_read_alias)} write={String(health.live_write_alias)}
        </div>
      </div>

      <div className="panel">
        <h3>Consolidation lifecycle</h3>
        <div className="health-row">
          <div className="name">
            Last window
            <small>
              {system?.consolidation?.last_window_start} → {system?.consolidation?.last_window_end}
            </small>
          </div>
          <span className="muted kv">{system?.consolidation?.last_status || "—"}</span>
        </div>
        <div className="health-row">
          <div className="name">
            Completed at
            <small>{system?.consolidation?.last_completed_at || "never"}</small>
          </div>
        </div>
        <div className="health-row">
          <div className="name">Rows consolidated (last window)</div>
          <span className="muted kv">{fmtInt(system?.consolidation?.last_rows_consolidated)}</span>
        </div>
        <div className="panel-foot">
          Window size: {system?.consolidation_window_hours}h · persists aggregates every cycle
        </div>
      </div>

      <div className="panel">
        <h3>Storage usage (PostgreSQL rows)</h3>
        {Object.entries(system?.storage || {}).map(([k, v]) => (
          <div className="health-row" key={k}>
            <div className="name kv">{k}</div>
            <span className="muted kv">{fmtInt(v)}</span>
          </div>
        ))}
        <div className="panel-foot">
          Hourly retention {system?.retention?.hourly_days}d · consolidated {system?.retention?.consolidated_days}d
        </div>
      </div>

      <div className="panel">
        <h3>Retention &amp; data lifecycle</h3>
        <div className="health-row">
          <div className="name">Live ES retention</div>
          <span className="muted kv">{system?.live_retention_hours}h</span>
        </div>
        <div className="health-row">
          <div className="name">
            Lifecycle
            <small>
              raw → Kafka → ES live → {system?.consolidation_window_hours}h consolidation → PG aggregates → cleanup →
              deleted
            </small>
          </div>
        </div>
        <div className="panel-foot">Historical aggregates persist; raw live events are bounded by retention.</div>
      </div>
    </div>
  );
}
