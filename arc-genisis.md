# 🚀 ARC GENESIS — FULL PROJECT DOCUMENTATION

---

## 🧠 OVERVIEW

Arc Genesis is an AI-powered PR reviewer for SQL queries that prevents costly, risky, and incorrect queries before execution.

It combines:

* Altimate (deterministic intelligence)
* AI (reasoning)
* Security layer

---

## 💥 PROBLEM

Data engineers:

* write complex queries
* rely on manual testing
* miss:

  * cost issues
  * broken joins
  * PII exposure
  * downstream impact

Problems are discovered AFTER execution.

---

## ⚡ SOLUTION

Arc Genesis:

* analyzes queries before execution
* predicts impact
* gives decision

---

## ⚙️ ARCHITECTURE

Next.js UI
→ FastAPI Backend
→ Altimate CLI
→ Gemini API
→ Response

---

## 🔌 INTEGRATIONS

### Altimate

* SQL analysis
* lineage
* cost
* PII

### Gemini

* decision making
* explanation
* fix generation

---

## 🧩 FEATURES

### 1. SQL PR Review

* approve / reject
* issue detection
* fix suggestion

---

### 2. Security Layer

* blocks destructive queries
* role simulation

---

### 3. Visualization

* cost indicator
* lineage graph
* risk level

---

## 🎬 DEMO FLOW

1. Input bad query
2. System rejects
3. Shows issues + fix
4. Run fixed query
5. Show approval
6. Show security block

---

## 📦 HACKATHON SUBMISSION

### Required:

* traces.zip
* source code
* screenshots

---

### Commands:

```bash
altimate-code run "Analyze query"
altimate-code trace list
zip traces.zip ~/.local/share/altimate-code/traces/*.json
```

---

## 👥 TEAM STRUCTURE

* AI Engineer → prompts + logic
* Backend → FastAPI + Altimate
* Frontend → UI

---

## ⏱️ TIMELINE

* Setup → 1h
* Backend → 3h
* Frontend → 2h
* Integration → 2h
* Demo → 1h

---

## 📊 WHY THIS WINS

* clear problem
* strong demo
* deep Altimate usage
* simple execution

---

## ⚠️ RISKS

* overcomplication
* weak explanation
* bad demo

---

## 🏁 FINAL PITCH

Arc Genesis is a PR review system for data — preventing dangerous and costly queries before they run.

---

## 💀 FINAL PRINCIPLE

We do NOT execute queries.
We decide if they should run.
