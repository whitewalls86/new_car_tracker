# Plan 67: n8n Credential Automation

**Status:** Not started
**Priority:** Low — only matters on fresh install; depends on Plan 29

**Depends on:** Plan 29 (n8n API foundation)

Currently a fresh install requires manually creating the Postgres credential in the n8n UI before any workflow that touches Postgres will run. The workflows import successfully, giving no indication anything is wrong — the failure only surfaces on first execution.

## Fix
Extend `setup.ps1` to call the n8n API (`POST /credentials`) to create the Postgres credential programmatically as part of the setup sequence. The credential values come from the same `.env` file used by everything else.

## Notes
- Blocked until Plan 29 establishes the API client and key management
- Small change once Plan 29 is in place — primarily a setup script addition
- Should include a check: if credential already exists, skip creation rather than error
