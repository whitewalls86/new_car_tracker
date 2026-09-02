"""Plan 138 Stage 1e: project the weekly recaps onto the public site, at build time.

Emits ``ops/static_ops/generated/recaps/`` -- one HTML page per published recap plus a
newest-first ``index.html`` -- from the Markdown in ``docs/recaps/``. This is
Stage 1d's pattern pointed at a second directory: a deterministic projection of
source-controlled Markdown into a committed static artifact, with a ``--check``
mode CI runs. Nothing parses Markdown in a request and no Markdown reaches the
browser.

**Publication is an editorial decision, and it is recorded per file.** Each
recap carries a ``**Publish:** true|false`` line in its header block, and that
marker is the whole of what this script reads. It is required, not defaulted:
a recap with no marker is a recap nobody decided about, and both defaults are
wrong -- ``true`` publishes an unread week the moment it lands, ``false`` drops
a week off the site and nothing says so. Absent is a build failure.

The initial values were set by commit count: eleven of the first thirty-one
weeks hold no commits, and a "Nothing shipped" page is a real record that is
not a page worth advertising. That was the seeding rule, not the policy. The
marker is the policy, and a week with commits that should stay internal is a
``false`` someone writes deliberately.

**Why per-file rather than a published-from date.** A date is one decision and
self-maintaining, but it cannot say "not this week" about a gap in the middle,
and the empty weeks are scattered through the corpus rather than bunched at the
start. A central allow-list says that, but it lives away from the thing it
describes and needs an edit every week, which is exactly the kind of second
place that goes stale. The marker travels with the file that ``plan-week``
writes, so the decision is made where the week is written.

**Link rewriting is a correctness rule, not a convenience.** Recaps link
relatively into the repository, and a relative path left alone becomes a 404 on
a public site whose directory layout is nothing like ``docs/``. So every link is
classified on its ``docs/``-relative resolution:

- an internal anchor stays internal;
- an absolute ``http(s)`` URL is left alone;
- a sibling recap resolves to the neighbouring published page, or to GitHub when
  that week is not published;
- any other repository path resolves to the same GitHub blob URL Stage 1d emits,
  and must exist in the tree;
- **anything else fails the build.** A silent passthrough is how a
  ``docs/``-relative path becomes a broken public link, and this corpus holds
  133 of them.

Classification is on the resolved path, not on the ``plans/`` directory alone:
the corpus holds 128 ``../plans/``, 4 ``../planning/`` and 1 ``../reference/``
link, so a rule that recognises only the first refuses to build against files
that are already committed.

**Links are rewritten in the token stream, not in the text.** A regex over
Markdown cannot tell a link from the same characters inside a fenced code
block, and the corpus has 42 of those.

Usage::

    python scripts/build_public_recaps.py            # write the artifacts
    python scripts/build_public_recaps.py --check    # fail on drift, write nothing
"""
from __future__ import annotations

import argparse
import datetime as dt
import html
import re
import sys
from pathlib import Path

from markdown_it import MarkdownIt

# One repo-root-relative constant block, matching scripts/build_public_roadmap.py.
REPO_ROOT = Path(__file__).resolve().parents[1]
DOCS_DIR = "docs"
RECAPS_DIR = "docs/recaps"
OUTPUT_DIR = "ops/static_ops/generated/recaps"

# The same destination Stage 1d resolves plan links to. The repository is
# public, so a blob URL is the honest target for a document this plan's
# non-goals bar from being published as a page of its own.
BLOB_BASE = "https://github.com/whitewalls86/new_car_tracker/blob/master/"

# Plan 138 Stage 2 gave these pages a canonical route. The generated files are
# also reachable at their static path -- ``handle /static_ops/*`` is public and
# has been since the Stage 1e deploy -- so every link the projection emits, and
# the canonical link on every page, names the route rather than the file. That
# is what makes the static path a duplicate of one address instead of a second
# address for the same content.
PUBLIC_BASE_URL = "https://cartracker.info"
RECAPS_ROUTE = "/recaps"

PUBLISH_FIELD = "**Publish:**"

_TITLE_RE = re.compile(r"^# Week of (\d{4}-\d{2}-\d{2}) to (\d{4}-\d{2}-\d{2})\s*$")
_PUBLISH_RE = re.compile(r"^\*\*Publish:\*\*\s*(\S+)\s*$")
_RECAP_NAME_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_SLUG_STRIP_RE = re.compile(r"[^a-z0-9\s-]")


class RecapBuildError(Exception):
    """A recap did not look the way this script requires.

    Raised rather than worked around: every condition that reaches here is one
    where the alternative is publishing something wrong, or broken, in public.
    """


# ---------------------------------------------------------------------------
# Reading a recap
# ---------------------------------------------------------------------------

class Recap:
    """One recap file, parsed down to what the projection needs."""

    def __init__(self, path: Path, week_start: str, week_end: str,
                 publish: bool, body: str) -> None:
        self.path = path
        self.slug = path.stem
        self.week_start = week_start
        self.week_end = week_end
        self.publish = publish
        self.body = body

    @property
    def output_name(self) -> str:
        """The file the projection writes. Not the URL -- see ``route``."""
        return f"{self.slug}.html"

    @property
    def route(self) -> str:
        return f"{RECAPS_ROUTE}/{self.slug}"

    @property
    def title(self) -> str:
        return f"Week of {self.week_start} to {self.week_end}"


def _display(path: Path) -> str:
    """A path for an error message, which must never raise on its way out."""
    try:
        return path.relative_to(REPO_ROOT).as_posix()
    except ValueError:
        return str(path)


def read_recap(path: Path) -> Recap:
    """Parse one recap, holding it to the shape ``plan-week`` writes.

    The filename is checked against the week the document states. They are two
    independent assertions of the same fact, and a recap filed under the wrong
    Sunday would otherwise sort into the wrong place in the index and carry a
    heading that contradicts its own URL.
    """
    if not _RECAP_NAME_RE.match(path.stem):
        raise RecapBuildError(
            f"{_display(path)}: a recap filename must be YYYY-MM-DD.md"
        )

    lines = path.read_text(encoding="utf-8").split("\n")

    title_match = _TITLE_RE.match(lines[0]) if lines else None
    if not title_match:
        raise RecapBuildError(
            f"{_display(path)}: first line must be "
            f"'# Week of YYYY-MM-DD to YYYY-MM-DD', found {lines[0][:80]!r}"
        )
    week_start, week_end = title_match.group(1), title_match.group(2)

    for field, value in (("start", week_start), ("end", week_end)):
        try:
            dt.date.fromisoformat(value)
        except ValueError as exc:
            raise RecapBuildError(
                f"{_display(path)}: week {field} {value!r} is not a real date"
            ) from exc

    if week_end != path.stem:
        raise RecapBuildError(
            f"{_display(path)}: filename says the week ends {path.stem}, the "
            f"heading says {week_end}. One of them is wrong"
        )
    if week_start >= week_end:
        raise RecapBuildError(
            f"{_display(path)}: week starts {week_start} and ends {week_end}"
        )

    publish, body_lines = None, []
    for line in lines:
        match = _PUBLISH_RE.match(line)
        if not match:
            body_lines.append(line)
            continue
        if publish is not None:
            raise RecapBuildError(
                f"{_display(path)}: more than one '{PUBLISH_FIELD}' line"
            )
        raw = match.group(1)
        if raw not in ("true", "false"):
            raise RecapBuildError(
                f"{_display(path)}: '{PUBLISH_FIELD}' must be exactly 'true' or "
                f"'false', found {raw!r}"
            )
        publish = raw == "true"

    if publish is None:
        raise RecapBuildError(
            f"{_display(path)}: no '{PUBLISH_FIELD}' line. Publication is an "
            f"editorial decision and this file does not record one -- a default "
            f"here would either publish an unread week or drop one silently"
        )

    # The marker is machine plumbing and never reaches the page. Everything
    # else in the header block is a fact the reader should see.
    return Recap(path, week_start, week_end, publish, "\n".join(body_lines))


def read_all() -> list[Recap]:
    directory = REPO_ROOT / RECAPS_DIR
    if not directory.is_dir():
        raise RecapBuildError(f"{RECAPS_DIR}: not a directory")
    recaps = [read_recap(path) for path in sorted(directory.glob("*.md"))]
    if not recaps:
        raise RecapBuildError(f"{RECAPS_DIR}: no recaps found")
    return recaps


# ---------------------------------------------------------------------------
# Link classification
# ---------------------------------------------------------------------------

def _blob_url(path: Path) -> str:
    return BLOB_BASE + path.relative_to(REPO_ROOT).as_posix()


def rewrite_href(href: str, source: Recap, published: set[str]) -> str:
    """Rewrite one link, or fail the build saying why it could not be.

    ``published`` is the set of recap slugs that produced a page, which is what
    decides whether a sibling link points at a neighbouring page or at GitHub.
    A recap can only link to a week that exists, but not necessarily to one that
    is published, and sending a reader to a page that was deliberately not built
    would be the same 404 this function exists to prevent.
    """
    target = href.strip()
    if not target:
        raise RecapBuildError(f"{_display(source.path)}: empty link target")

    # Internal anchors stay internal; the heading ids below are what they hit.
    if target.startswith("#"):
        return target

    if target.startswith(("http://", "https://")):
        return target

    # Everything else is repository-relative, and is resolved as such. Any
    # fragment rides along to whatever the path resolves to.
    path_part, _, fragment = target.partition("#")
    suffix = f"#{fragment}" if fragment else ""

    if path_part.startswith(("/", "mailto:", "tel:")) or ":" in path_part.split("/")[0]:
        raise RecapBuildError(
            f"{_display(source.path)}: cannot classify link target {target!r}. "
            f"A recap may link to an internal anchor, an absolute http(s) URL, "
            f"or a path inside the repository"
        )

    resolved = (source.path.parent / path_part).resolve()
    try:
        relative = resolved.relative_to(REPO_ROOT)
    except ValueError:
        raise RecapBuildError(
            f"{_display(source.path)}: link target {target!r} resolves outside "
            f"the repository, to {resolved}"
        ) from None

    if not resolved.is_file():
        raise RecapBuildError(
            f"{_display(source.path)}: link target {target!r} resolves to "
            f"{relative.as_posix()}, which is not a file in this tree"
        )

    # A sibling recap: the neighbouring page when that week was published,
    # GitHub when it was not.
    if relative.parent.as_posix() == RECAPS_DIR and relative.suffix == ".md":
        if relative.stem in published:
            return f"{RECAPS_ROUTE}/{relative.stem}{suffix}"
        return _blob_url(resolved) + suffix

    return _blob_url(resolved) + suffix


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def _slug(text: str) -> str:
    return re.sub(r"[\s-]+", "-", _SLUG_STRIP_RE.sub("", text.lower()).strip())


def _markdown() -> MarkdownIt:
    """CommonMark plus tables, and no raw HTML.

    ``html=False`` is passed explicitly and is **not** the preset's default:
    ``MarkdownIt("commonmark")`` sets ``html=True``, because the CommonMark spec
    admits raw HTML. Left alone, a recap containing a ``<script>`` tag would
    have it copied verbatim onto a public page -- precisely what Stage 4 item 3
    and Stage 3c's CSP bar. With it off the tag is escaped and shown as text.

    The corpus holds no HTML tags today, so this costs nothing now and closes
    the door on the day someone pastes one into a recap.
    """
    return MarkdownIt("commonmark", {"html": False}).enable("table")


def render_body(recap: Recap, published: set[str]) -> str:
    """Render one recap's Markdown, rewriting links in the token stream.

    Walking tokens rather than the text is what makes the rewrite safe: a regex
    cannot tell a link from the same characters inside a fenced code block, and
    this corpus has 42 fenced blocks full of shell commands and paths.
    """
    md = _markdown()
    tokens = md.parse(recap.body)

    seen: dict[str, int] = {}
    for index, token in enumerate(tokens):
        if token.type == "heading_open":
            inline = tokens[index + 1]
            base = _slug(inline.content) or "section"
            count = seen.get(base, 0)
            seen[base] = count + 1
            token.attrSet("id", base if not count else f"{base}-{count}")
        elif token.type == "inline":
            for child in token.children or ():
                if child.type == "link_open":
                    href = child.attrGet("href") or ""
                    child.attrSet("href", rewrite_href(href, recap, published))

    return md.renderer.render(tokens, md.options, {})


_STYLE = """\
:root { color-scheme: light dark; }
body { margin: 0 auto; padding: 2rem 1.25rem 4rem; max-width: 46rem;
  font: 16px/1.65 system-ui, -apple-system, "Segoe UI", sans-serif; }
h1, h2, h3 { line-height: 1.25; margin-top: 2.25rem; }
h1 { margin-top: 0; }
code, pre { font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  font-size: 0.9em; }
pre { overflow-x: auto; padding: 0.85rem 1rem; border-radius: 6px;
  background: rgba(127, 127, 127, 0.12); }
:not(pre) > code { padding: 0.15em 0.35em; border-radius: 3px;
  background: rgba(127, 127, 127, 0.15); }
.table-scroll { overflow-x: auto; }
table { border-collapse: collapse; width: 100%; margin: 1.25rem 0; }
th, td { text-align: left; padding: 0.45rem 0.7rem;
  border-bottom: 1px solid rgba(127, 127, 127, 0.35); }
.note { margin: 1.5rem 0 2.5rem; padding: 0.85rem 1rem; border-radius: 6px;
  font-size: 0.925rem; background: rgba(127, 127, 127, 0.1);
  border-left: 3px solid rgba(127, 127, 127, 0.5); }
.index-list { list-style: none; padding: 0; }
.index-list li { padding: 0.6rem 0;
  border-bottom: 1px solid rgba(127, 127, 127, 0.25); }
.index-list .meta { display: block; font-size: 0.9rem; opacity: 0.75; }
"""

# Truth contract §5: a recap is correct as of its date and is never revised to
# match a later truth, so every page says so in its own words. Without this a
# six-month-old page reads as a current claim and contradicts §1's
# production/experimental split.
_POINT_IN_TIME = (
    "This is a point-in-time record of the week of {start} to {end}. It was "
    "written from the repository's history for that week and is not revised "
    "afterwards, so later pages may supersede what it says."
)


def _page(title: str, note: str, body: str, canonical: str) -> str:
    return (
        "<!doctype html>\n"
        '<html lang="en">\n'
        "<head>\n"
        '<meta charset="utf-8">\n'
        '<meta name="viewport" content="width=device-width, initial-scale=1">\n'
        f"<title>{html.escape(title)}</title>\n"
        f'<link rel="canonical" href="{PUBLIC_BASE_URL}{canonical}">\n'
        f"<style>\n{_STYLE}</style>\n"
        "</head>\n"
        "<body>\n"
        f"<main>\n{note}{body}</main>\n"
        "</body>\n"
        "</html>\n"
    )


def render_page(recap: Recap, published: set[str]) -> str:
    note = (
        f'<p class="note">'
        f"{html.escape(_POINT_IN_TIME.format(start=recap.week_start, end=recap.week_end))}"
        f"</p>\n"
    )
    body = render_body(recap, published)
    # Tables scroll inside their own box rather than widening the page. The
    # corpus holds 381 table rows and some are far wider than a phone.
    body = body.replace("<table>", '<div class="table-scroll"><table>')
    body = body.replace("</table>", "</table></div>")
    return _page(recap.title, note, body, recap.route)


def render_index(recaps: list[Recap]) -> str:
    """The newest-first index of the published weeks."""
    items = []
    for recap in recaps:
        items.append(
            f'<li><a href="{recap.route}">{html.escape(recap.title)}</a>'
            f'<span class="meta">Week ending {recap.week_end}</span></li>'
        )
    body = (
        "<h1>Weekly recaps</h1>\n"
        '<p>Each week of work on this project, written from the repository\'s '
        "own history. Newest first.</p>\n"
        '<ul class="index-list">\n' + "\n".join(items) + "\n</ul>\n"
    )
    note = (
        '<p class="note">Each recap is a point-in-time record of its week and is '
        "not revised afterwards. Weeks in which no commits landed are kept in the "
        "repository but are not published here.</p>\n"
    )
    return _page("Weekly recaps", note, body, RECAPS_ROUTE)


# ---------------------------------------------------------------------------
# The projection
# ---------------------------------------------------------------------------

def build() -> dict[str, str]:
    """Every file the projection produces, keyed by name under ``OUTPUT_DIR``.

    Returning the whole set rather than writing as it goes is what lets
    ``--check`` compare bytes without touching the tree, and what lets the
    caller notice a *stale* file that no longer has a recap behind it.
    """
    recaps = read_all()

    ordered = sorted(recaps, key=lambda recap: recap.week_end, reverse=True)
    selected = [recap for recap in ordered if recap.publish]
    if not selected:
        raise RecapBuildError(
            f"{RECAPS_DIR}: no recap is marked '{PUBLISH_FIELD} true', so the "
            f"projection would publish an empty index"
        )

    published = {recap.slug for recap in selected}
    files = {recap.output_name: render_page(recap, published) for recap in selected}
    files["index.html"] = render_index(selected)
    return files


def _existing(destination: Path) -> dict[str, str]:
    if not destination.is_dir():
        return {}
    return {
        path.name: path.read_text(encoding="utf-8")
        for path in sorted(destination.glob("*.html"))
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--check",
        action="store_true",
        help="regenerate in memory and exit non-zero on drift; write nothing",
    )
    args = parser.parse_args(argv)

    try:
        files = build()
    except RecapBuildError as exc:
        print(f"build_public_recaps: {exc}", file=sys.stderr)
        return 2

    destination = REPO_ROOT / OUTPUT_DIR
    current = _existing(destination)

    if args.check:
        missing = sorted(set(files) - set(current))
        # A page whose recap flipped to 'false', or was deleted. Left alone it
        # would keep serving a week that is no longer published, which is the
        # failure this check exists for.
        orphaned = sorted(set(current) - set(files))
        changed = sorted(
            name for name in set(files) & set(current) if files[name] != current[name]
        )
        if not (missing or orphaned or changed):
            print(f"build_public_recaps: {OUTPUT_DIR} is up to date "
                  f"({len(files) - 1} published recaps)")
            return 0

        for label, names in (
            ("not generated", missing),
            ("published without a recap behind it", orphaned),
            ("stale", changed),
        ):
            if names:
                print(
                    f"build_public_recaps: {len(names)} file(s) {label}: "
                    f"{', '.join(names)}",
                    file=sys.stderr,
                )
        print(
            "build_public_recaps: run `python scripts/build_public_recaps.py` "
            "and commit the result",
            file=sys.stderr,
        )
        return 1

    destination.mkdir(parents=True, exist_ok=True)
    for name in sorted(set(current) - set(files)):
        (destination / name).unlink()
    for name, text in sorted(files.items()):
        (destination / name).write_text(text, encoding="utf-8")
    print(
        f"build_public_recaps: wrote {len(files)} files to {OUTPUT_DIR} "
        f"({len(files) - 1} published recaps)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
