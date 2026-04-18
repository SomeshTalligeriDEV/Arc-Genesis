"use client";
import { useRouter } from "next/navigation";
import dynamic from "next/dynamic";
import TrueFocus from "./TrueFocus";

const Beams = dynamic(() => import("./Beams"), { ssr: false });

const FEATURES = [
  {
    title: "Real-time Query Analysis",
    desc: "Every SQL query is analyzed the moment it arrives — security, cost, and correctness in under 3 seconds.",
  },
  {
    title: "AI-Powered Decisions",
    desc: "Groq LLM evaluates each query and returns APPROVE, REJECT, or WARNING with a plain-English explanation.",
  },
  {
    title: "Injection Detection",
    desc: "Detects tautology, UNION-based, time-based, and stacked-query injection patterns before they reach your warehouse.",
  },
  {
    title: "Impact Mapping",
    desc: "Traces which downstream services and dashboards are affected by each query — before it runs.",
  },
  {
    title: "Live Observability Feed",
    desc: "A real-time stream of every query ingested, analyzed, and decided — with filters and drill-down.",
  },
  {
    title: "Query Diff & Auto-Fix",
    desc: "Side-by-side before/after view with a suggested fix generated automatically for every rejected query.",
  },
];

const STATS = [
  { value: "< 3s",  label: "Analysis latency" },
  { value: "10+",   label: "Anti-patterns detected" },
  { value: "100%",  label: "Queries intercepted" },
  { value: "0",     label: "Warehouse connections needed" },
];

export default function LandingPage() {
  const router = useRouter();

  return (
    <div className="lp-shell">
      {/* Background */}
      <div className="lp-bg-orb lp-orb-1" />
      <div className="lp-bg-orb lp-orb-2" />

      {/* Beams background */}
      <div style={{ position: "fixed", inset: 0, zIndex: 0, pointerEvents: "none" }}>
        <Beams
          beamWidth={2}
          beamHeight={15}
          beamNumber={10}
          lightColor="#ffffff"
          speed={1.5}
          noiseIntensity={1.5}
          scale={0.2}
          rotation={0}
        />
      </div>

      {/* Nav */}
      <nav className="lp-nav">
        <div className="lp-nav-inner">
          <div className="lp-nav-brand">
            <svg width="26" height="26" viewBox="0 0 48 48" fill="none">
              <rect x="8"  y="8"  width="14" height="14" rx="3" fill="#6366f1" opacity="0.9"/>
              <rect x="26" y="8"  width="14" height="14" rx="3" fill="#6366f1" opacity="0.5"/>
              <rect x="8"  y="26" width="14" height="14" rx="3" fill="#6366f1" opacity="0.5"/>
              <rect x="26" y="26" width="14" height="14" rx="3" fill="#6366f1" opacity="0.25"/>
            </svg>
            <span>Arc Genesis</span>
          </div>
          <div className="lp-nav-links">
            <a className="lp-nav-link" href="#features">Features</a>
            <a className="lp-nav-link" href="#how-it-works">How it works</a>
            <a className="lp-nav-link" onClick={() => router.push("/dashboard")}>Dashboard</a>
          </div>
          <button className="lp-btn-primary" onClick={() => router.push("/dashboard")}>
            Get Started →
          </button>
        </div>
      </nav>

      {/* Hero */}
      <section className="lp-hero">
        <div className="lp-badge">Real-time · AI-powered · Production-ready</div>

        <TrueFocus
          className="lp-true-focus-title"
          sentence="Stop bad SQL|before it runs."
          separator="|"
          manualMode={false}
          blurAmount={2}
          borderColor="#6366f1"
          glowColor="rgba(99,102,241,0.6)"
          animationDuration={0.7}
          pauseBetweenAnimations={0.45}
        />

        <TrueFocus
          className="lp-true-focus-sub"
          sentence="Arc Genesis intercepts every query, analyzes it with AI, and returns a decision in real time — before it touches your warehouse."
          manualMode={false}
          blurAmount={1.5}
          borderColor="rgba(255,255,255,0.8)"
          glowColor="rgba(255,255,255,0.35)"
          animationDuration={0.5}
          pauseBetweenAnimations={0.1}
        />
        <div className="lp-hero-actions">
          <button className="lp-btn-primary lp-btn-lg" onClick={() => router.push("/dashboard")}>
            Open Dashboard
          </button>
          <button className="lp-btn-ghost lp-btn-lg" onClick={() => router.push("/dashboard")}>
            Run Demo
          </button>
        </div>

        {/* Stats */}
        <div className="lp-stats">
          {STATS.map(s => (
            <div key={s.label} className="lp-stat">
              <div className="lp-stat-value">{s.value}</div>
              <div className="lp-stat-label">{s.label}</div>
            </div>
          ))}
        </div>
      </section>

      {/* Features */}
      <section className="lp-features">
        <h2 className="lp-section-title">Everything you need to protect your warehouse</h2>
        <div className="lp-feature-grid">
          {FEATURES.map(f => (
            <div key={f.title} className="lp-feature-card">
              <div className="lp-feature-dot" />
              <h3 className="lp-feature-title">{f.title}</h3>
              <p className="lp-feature-desc">{f.desc}</p>
            </div>
          ))}
        </div>
      </section>

      {/* CTA */}
      <section className="lp-cta">
        <h2 className="lp-cta-title">Ready to see it live?</h2>
        <p className="lp-cta-sub">Open the dashboard and run the demo — three queries, three decisions, in real time.</p>
        <button className="lp-btn-primary lp-btn-lg" onClick={() => router.push("/dashboard")}>
          Open Dashboard →
        </button>
      </section>

      <footer className="lp-footer">Arc Genesis · Real-time AI SQL Observability</footer>
    </div>
  );
}
