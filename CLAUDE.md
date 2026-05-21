# CLAUDE.md (root)

Monorepo con due codebase:
- `client/` — desktop Python (v1088.1)
- `server/` — FastAPI (v0.2.3, deployata su NAS)

**Prima di lavorare leggi nell'ordine:**
1. `docs/CLAUDE.md` — onboarding completo (questo file è solo l'indice)
2. `docs/CONTEXT.md` — architettura tecnica
3. `docs/ROADMAP.md` — cosa è prossimo
4. `docs/UPGRADES.md` — storia decisioni (ultime ~200 righe)

**Convenzioni veloci:**
- Versioning client: `client/version.py` + `client/version_info.txt` (allineati)
- Versioning server: `server/app/config.py::APP_VERSION`
- Niente `.env`, secrets, password in commit/deliverable
- Modifiche client+server correlate → 1 commit (vantaggio monorepo)