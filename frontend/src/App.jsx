import { useCallback, useEffect, useRef, useState } from "react";
import { fetchJSON, fmtInt, useLiveFeed } from "./api.js";
import Overview from "./components/Overview.jsx";
import LiveFeed from "./components/LiveFeed.jsx";
import Search from "./components/Search.jsx";
import Analytics from "./components/Analytics.jsx";
import Monitoring from "./components/Monitoring.jsx";
import Suspicious from "./components/Suspicious.jsx";

const TABS = [
  { id: "overview", label: "Overview" },
  { id: "live", label: "Live Stream" },
  { id: "analytics", label: "Analytics" },
  { id: "search", label: "Search" },
  { id: "suspicious", label: "Suspicious" },
  { id: "monitoring", label: "Monitoring" },
];

export default function App() {
  const [tab, setTab] = useState("overview");
  const [wsStatus, setWsStatus] = useState("disconnected");
  const [liveEvents, setLiveEvents] = useState([]);
  const [liveRate, setLiveRate] = useState(0);
  const seenRef = useRef(0);

  const onEvent = useCallback((ev) => {
    seenRef.current += 1;
    setLiveEvents((prev) => [ev, ...prev].slice(0, 120));
  }, []);

  useLiveFeed(onEvent, setWsStatus);

  // Rolling edits-per-minute from the live feed (client-side estimate).
  useEffect(() => {
    const start = Date.now();
    const base = seenRef.current;
    const id = setInterval(() => {
      const mins = (Date.now() - start) / 60000;
      if (mins > 0.05) setLiveRate(Math.round((seenRef.current - base) / mins));
    }, 2000);
    return () => clearInterval(id);
  }, []);

  return (
    <div className="app">
      <header className="header">
        <div className="brand">
          <div className="logo">📡</div>
          <div>
            <h1>WikiPulse</h1>
            <p className="sub">Real-time Wikipedia edit analytics &amp; observability</p>
          </div>
        </div>
        <div style={{ display: "flex", gap: 10, alignItems: "center", flexWrap: "wrap" }}>
          <span className="status-pill">
            <span className={`dot ${wsStatus}`} /> ws {wsStatus}
          </span>
          <span className="status-pill">~{fmtInt(liveRate)} edits/min</span>
        </div>
      </header>

      <nav className="tabs">
        {TABS.map((t) => (
          <button
            key={t.id}
            className={`tab ${tab === t.id ? "active" : ""}`}
            onClick={() => setTab(t.id)}
          >
            {t.label}
          </button>
        ))}
      </nav>

      {tab === "overview" && <Overview liveEvents={liveEvents} liveRate={liveRate} />}
      {tab === "live" && <LiveFeed events={liveEvents} />}
      {tab === "analytics" && <Analytics />}
      {tab === "search" && <Search />}
      {tab === "suspicious" && <Suspicious />}
      {tab === "monitoring" && <Monitoring />}
    </div>
  );
}
