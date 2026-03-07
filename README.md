# FARL Module 1: Immutable Event Ledger

## Deploy this on Railway

1. Replace the repo files with the patched versions in this folder.
2. Ensure the Railway service has a Postgres database attached, or remove `DATABASE_URL` to fall back to SQLite.
3. Set the Start Command in Railway to:

   gunicorn app:app

4. Redeploy the service.

## Why this patch exists

This patch hardens Railway deployment by:
- adding the PostgreSQL driver (`psycopg2-binary`)
- normalizing `postgres://` to `postgresql://` for SQLAlchemy
- keeping the existing `/health`, `/log`, `/latest`, `/entries`, and `/verify` routes
- adding boot logs so Railway startup failures are visible immediately

## After deploy

Test these directly:
- GET /health
- GET /latest

Then rerun Orion:
- HEALTH_CHECK
- STATUS_CHECK
- LEDGER_WRITE
- GET_LATEST_RESULT
