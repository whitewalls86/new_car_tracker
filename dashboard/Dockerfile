FROM python:3.11-slim

WORKDIR /app

COPY dashboard/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY dashboard/ .
COPY shared/ ./shared/

EXPOSE 8501

# --server.baseUrlPath is what stops Streamlit believing it owns the origin root.
#
# Without it Streamlit serves its SPA shell for *every* unrecognised path, and
# that shell references its assets relatively (`./static/js/...`). Two failures
# follow, and Plan 138 Stage 2 hit both on 2026-09-02:
#
#   /dashboard/  -> the relative asset resolves to /dashboard/static/js/..., which
#                   Streamlit answers with the shell again (text/html, 11 KB
#                   instead of the 451 KB bundle). The browser refuses to execute
#                   HTML as a module and the app never boots.
#   /dashboard   -> the asset resolves to /static/js/... and loads via Caddy's
#                   catch-all, so the app boots -- and then Streamlit's client
#                   router does not recognise /dashboard as a page and navigates
#                   to what it thinks is the app root, `/`.
#
# The second one worked before Stage 2 only because `/` was also Streamlit.
# Moving `/` to the landing page took that fallback away. With the base path set,
# Streamlit serves the app, its assets, its websocket and its health endpoint all
# under /dashboard/, which is exactly what the Caddyfile's /dashboard* block
# already routes -- and the catch-all stops being load-bearing.
#
# The Compose healthcheck moves with it: /_stcore/health becomes
# /dashboard/_stcore/health.
CMD ["streamlit", "run", "app.py", "--server.address", "0.0.0.0", "--server.port", "8501", "--server.baseUrlPath", "dashboard", "--browser.gatherUsageStats", "false"]
