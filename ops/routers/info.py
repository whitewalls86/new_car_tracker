"""
Public /info route — renders README.md as a styled landing page.
No authentication required; Caddy routes /info without forward_auth.
"""
import os

from fastapi import APIRouter
from fastapi.responses import HTMLResponse

router = APIRouter()

_README_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "README.md")


@router.get("/info", response_class=HTMLResponse)
def info_page():
    try:
        with open(_README_PATH, "r", encoding="utf-8") as f:
            raw_md = f.read()
    except FileNotFoundError:
        raw_md = "# CarTracker\n\nREADME not found."

    # Escape backticks and template literals so the JS string stays intact
    escaped = raw_md.replace("\\", "\\\\").replace("`", "\\`").replace("${", "\\${")

    return f"""<!DOCTYPE html>
<html lang="en" data-theme="light">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>CarTracker</title>
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@picocss/pico@2/css/pico.min.css">
    <style>
        body {{ padding: 2rem 0; }}
        .prose {{ max-width: 860px; margin: 0 auto; }}
        pre {{ overflow-x: auto; }}
        pre code {{
            display: block;
            padding: 1rem;
            font-size: 0.85rem;
            line-height: 1.5;
        }}
        table {{ width: 100%; }}
        h1, h2, h3 {{ margin-top: 2rem; }}
        h1 {{ border-bottom: 2px solid var(--pico-muted-border-color); padding-bottom: 0.4rem; }}
        h2 {{ border-bottom: 1px solid var(--pico-muted-border-color); padding-bottom: 0.3rem; }}
        .top-bar {{
            max-width: 860px;
            margin: 0 auto 1.5rem auto;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}
        .top-bar a {{ text-decoration: none; }}
    </style>
</head>
<body>
    <main class="container">
        <div class="top-bar">
            <a href="/request-access" role="button">Request Access</a>
            <a href="https://github.com/whitewalls86/new_car_tracker"
               target="_blank" rel="noopener">GitHub →</a>
        </div>
        <article class="prose" id="content"></article>
    </main>
    <script src="https://cdn.jsdelivr.net/npm/marked@12/marked.min.js"></script>
    <script>
        const md = `{escaped}`;
        document.getElementById("content").innerHTML = marked.parse(md);
    </script>
</body>
</html>"""
