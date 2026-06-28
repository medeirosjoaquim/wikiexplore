import React from "react";

// API + WebSocket helpers for the WikiPulse dashboard.
// In dev, Vite proxies /api, /health, /ws to the backend (see vite.config.js).

export async function fetchJSON(url, { timeout = 8000 } = {}) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), timeout);
  try {
    const res = await fetch(url, { signal: ctrl.signal, headers: { Accept: "application/json" } });
    if (!res.ok) throw new Error(`${url} -> ${res.status}`);
    return await res.json();
  } finally {
    clearTimeout(t);
  }
}

export function fmtInt(n) {
  if (n === null || n === undefined) return "—";
  return new Intl.NumberFormat("en-US").format(Number(n) || 0);
}

export function fmtBytes(n) {
  if (!n) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let v = n;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return `${v.toFixed(v >= 100 ? 0 : 1)} ${units[i]}`;
}

export function relTime(iso) {
  if (!iso) return "—";
  const d = new Date(iso);
  const s = Math.round((Date.now() - d.getTime()) / 1000);
  if (s < 60) return `${s}s ago`;
  if (s < 3600) return `${Math.round(s / 60)}m ago`;
  if (s < 86400) return `${Math.round(s / 3600)}h ago`;
  return d.toLocaleString();
}

// WebSocket hook: connects to /ws/live and calls onEvent for each event payload.
export function useLiveFeed(onEvent, onStatus) {
  const ref = React.useRef(null);
  const connect = React.useCallback(() => {
    const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
    const ws = new WebSocket(`${proto}//${window.location.host}/ws/live`);
    ref.current = ws;
    ws.onopen = () => onStatus?.("connected");
    ws.onclose = () => {
      onStatus?.("disconnected");
      setTimeout(connect, 3000); // auto-reconnect
    };
    ws.onerror = () => onStatus?.("error");
    ws.onmessage = (e) => {
      try {
        const msg = JSON.parse(e.data);
        if (msg.type === "event") onEvent?.(msg.data);
      } catch {
        /* ignore non-JSON keepalives */
      }
    };
  }, [onEvent, onStatus]);
  React.useEffect(() => {
    connect();
    return () => ref.current?.close();
  }, [connect]);
}
