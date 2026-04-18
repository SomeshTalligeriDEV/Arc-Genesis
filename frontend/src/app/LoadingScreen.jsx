"use client";
import { useState, useEffect } from "react";

const MESSAGES = [
  "Analyzing query patterns...",
  "Mapping data lineage...",
  "Evaluating risk signals...",
  "Initializing decision engine...",
  "Establishing secure pipeline...",
  "Loading observability layer...",
];

export default function LoadingScreen() {
  const [msgIdx, setMsgIdx] = useState(0);
  const [visible, setVisible] = useState(true);

  useEffect(() => {
    const id = setInterval(() => {
      setVisible(false);
      setTimeout(() => {
        setMsgIdx(i => (i + 1) % MESSAGES.length);
        setVisible(true);
      }, 200);
    }, 1600);
    return () => clearInterval(id);
  }, []);

  return (
    <div className="ls-shell">
      <div className="ls-orb ls-orb-1" />
      <div className="ls-orb ls-orb-2" />
      <div className="ls-grid" />

      <div className="ls-center">
        <div className="ls-icon">
          <svg width="44" height="44" viewBox="0 0 48 48" fill="none">
            <rect x="8"  y="8"  width="14" height="14" rx="3" fill="#6366f1" opacity="0.9"/>
            <rect x="26" y="8"  width="14" height="14" rx="3" fill="#6366f1" opacity="0.5"/>
            <rect x="8"  y="26" width="14" height="14" rx="3" fill="#6366f1" opacity="0.5"/>
            <rect x="26" y="26" width="14" height="14" rx="3" fill="#6366f1" opacity="0.25"/>
          </svg>
        </div>

        <h1 className="ls-title">Arc Genesis</h1>
        <p className="ls-subtitle">Initializing System...</p>

        {/* Indeterminate progress bar — no fake timer */}
        <div className="ls-bar-track">
          <div className="ls-bar-indeterminate" />
        </div>

        <p className="ls-status" style={{ opacity: visible ? 1 : 0 }}>
          {MESSAGES[msgIdx]}
        </p>
      </div>

      <div className="ls-nodes">
        {[...Array(5)].map((_, i) => (
          <div key={i} className="ls-node" style={{ animationDelay: `${i * 0.25}s` }} />
        ))}
      </div>
    </div>
  );
}
