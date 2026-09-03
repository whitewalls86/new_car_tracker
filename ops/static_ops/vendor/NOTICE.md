# Third-party assets served from this directory

Plan 138 Stage 3c brought these on-origin. Before it, the landing page fetched
one stylesheet from `cdn.jsdelivr.net` and twelve icon requests from
`cdn.simpleicons.org` on every load, which is what stopped the page running
under a `default-src 'none'` CSP and what made a third party's availability a
dependency of a portfolio page.

Vendored, not built. There is no package manifest and no bundler here: the files
were downloaded once at the pinned versions below and committed. Refreshing one
means downloading it again at a stated version and updating this file.

## Pico CSS — `pico.min.css`

| | |
|---|---|
| Version | 2.1.1 |
| Source | `https://cdn.jsdelivr.net/npm/@picocss/pico@2.1.1/css/pico.min.css` |
| Downloaded | 2026-09-02 |
| Licence | MIT |
| Copyright | 2019-2025 Lucas Larroche |
| Upstream | https://picocss.com — https://github.com/picocss/pico |

The MIT notice ships inside the file, in the banner comment the minifier
preserves. It is not repeated here so that the two cannot disagree; read the
first three lines of `pico.min.css`.

The template pinned `@2`, a floating major-version tag, so the page's stylesheet
could change without a commit. The vendored copy is pinned to the exact version
that tag resolved to on the date above.

## Simple Icons — `icons/*.svg`

| | |
|---|---|
| Version | 16.29.0, as served by `cdn.simpleicons.org` on 2026-09-02 |
| Source | `https://cdn.simpleicons.org/<slug>/<hex>` |
| Licence | CC0 1.0 Universal |
| Upstream | https://simpleicons.org — https://github.com/simple-icons/simple-icons |

Eight files, one per service the landing page names. The colour in each `fill`
is the one the template already requested from the CDN, so the rendered page is
unchanged:

| File | Slug | Colour |
|---|---|---|
| `icons/apacheairflow.svg` | `apacheairflow` | `#017CEE` |
| `icons/caddy.svg` | `caddy` | `#22D3EE` |
| `icons/duckdb.svg` | `duckdb` | `#FFF000` |
| `icons/fastapi.svg` | `fastapi` | `#009688` |
| `icons/grafana.svg` | `grafana` | `#F46800` |
| `icons/minio.svg` | `minio` | `#C72E49` |
| `icons/postgresql.svg` | `postgresql` | `#4169E1` |
| `icons/streamlit.svg` | `streamlit` | `#FF4B4B` |

**The CC0 waiver covers Simple Icons' own work, not the marks it draws.** Each
icon depicts a trademark of the project or company it names, and those remain
their owners'. They are used here to identify the software this system actually
runs, which is nominative use, and no endorsement is implied or claimed.

Simple Icons states the same thing in its own terms, and its wording governs:
https://github.com/simple-icons/simple-icons/blob/develop/DISCLAIMER.md

## Not vendored

`../dbt-bit-standalone.png` is the dbt Labs mark and was already served from
this origin before Stage 3c. It sits beside the other authored assets rather
than here, and it carries the same trademark position as the icons above:
dbt Labs', used to name the tool this system runs.
