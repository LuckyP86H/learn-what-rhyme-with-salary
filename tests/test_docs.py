"""Drift guards for the documentation: stale docs fail here, in CI, instead of rotting quietly.

Generated pages (docs/reference/) must be fresh; hand-written pages may only mention things that exist.
Run with `make docs-check`. If one of these fails, the message says what to fix.
"""
import importlib.util
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("gen_docs", ROOT / "scripts" / "gen_docs.py")
gen_docs = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gen_docs)

ALL_DOCS = sorted([ROOT / "README.md", ROOT / "celery_viz" / "README.md", *(ROOT / "docs").rglob("*.md")])
HAND_WRITTEN = [p for p in ALL_DOCS if "reference" not in p.relative_to(ROOT).parts]
# Build output and local environments: referenced by the docs but intentionally not in the repository.
NOT_IN_REPO = ("celery_viz/ui_dist", ".venv")


def rel(p: Path) -> str:
    return p.relative_to(ROOT).as_posix()


def strip_fences(text: str) -> str:
    return re.sub(r"```.*?```", "", text, flags=re.S)


def slug(heading: str) -> str:
    """GitHub's heading anchor: lowercase, drop punctuation, spaces to hyphens."""
    s = re.sub(r"[^\w\- ]", "", heading.strip().lower().replace("`", ""))
    return s.replace(" ", "-")


def anchors(path: Path) -> set[str]:
    return {slug(m.group(1)) for m in re.finditer(r"^#{1,6}\s+(.+?)\s*$", strip_fences(path.read_text()), re.M)}


def code_snippets(text: str) -> list[str]:
    """Inline code spans and every line inside fenced blocks."""
    spans = re.findall(r"`([^`\n]+)`", strip_fences(text))
    fenced = [line for block in re.findall(r"```[^\n]*\n(.*?)```", text, flags=re.S) for line in block.splitlines()]
    return spans + fenced


# ---- generated pages -------------------------------------------------------------------------------------
@pytest.mark.parametrize("name", sorted(gen_docs.PAGES))
def test_generated_page_is_fresh(name):
    path = gen_docs.REF / name
    assert path.exists(), f"docs/reference/{name} is missing: run `make docs`"
    assert path.read_text() == gen_docs.PAGES[name](), f"docs/reference/{name} is stale: run `make docs`"


def test_every_source_file_has_a_description():
    blank = [line for line in gen_docs.modules_md().splitlines() if re.match(r"^\| \[`.+`\]\(.+\) \| *\|$", line)]
    assert not blank, "add a one-line header comment to: " + ", ".join(re.findall(r"`([^`]+)`", " ".join(blank)))


def test_every_compose_service_has_a_description_label():
    compose, _ = gen_docs._load_compose()
    missing = [n for n, svc in compose["services"].items() if not (svc.get("labels") or {}).get("description")]
    assert not missing, f"add labels.description to compose services: {missing}"


# ---- links ---------------------------------------------------------------------------------------------------
@pytest.mark.parametrize("doc", ALL_DOCS, ids=rel)
def test_relative_links_and_anchors_resolve(doc):
    problems = []
    for target in re.findall(r"\]\(([^)\s]+)\)", strip_fences(doc.read_text())):
        if re.match(r"^[a-z]+:", target):  # http:, https:, mailto:
            continue
        path_part, _, fragment = target.partition("#")
        dest = doc if not path_part else (doc.parent / path_part).resolve()
        if not dest.exists():
            if any(rel(dest).startswith(prefix) for prefix in NOT_IN_REPO):
                continue
            problems.append(f"{target}: no such file")
        elif fragment and dest.suffix == ".md" and fragment not in anchors(dest):
            problems.append(f"{target}: no heading '#{fragment}' in {rel(dest)}")
    assert not problems, f"{rel(doc)}: " + "; ".join(problems)


def test_every_page_is_reachable_from_the_docs_home():
    home = (ROOT / "docs" / "README.md").read_text() + (ROOT / "README.md").read_text()
    orphans = [rel(p) for p in HAND_WRITTEN if p.parent == ROOT / "docs" and p.name != "README.md" and p.name not in home]
    assert not orphans, f"link these from docs/README.md: {orphans}"


# ---- things the prose mentions must exist ---------------------------------------------------------------------
def makefile_targets() -> set[str]:
    return {m.group(1) for m in re.finditer(r"^([a-zA-Z0-9_-]+):", (ROOT / "Makefile").read_text(), re.M)}


@pytest.mark.parametrize("doc", HAND_WRITTEN, ids=rel)
def test_make_targets_mentioned_exist(doc):
    targets = makefile_targets()
    missing = set()
    for snippet in code_snippets(doc.read_text()):
        snippet = snippet.split("#", 1)[0]
        m = re.search(r"(?:^|\s)make\s+(.*)$", snippet)
        if not m:
            continue
        for word in m.group(1).split():
            if "=" in word or not re.fullmatch(r"[a-z][a-z0-9-]*", word):
                break
            if word not in targets:
                missing.add(word)
    assert not missing, f"{rel(doc)} mentions make targets that do not exist: {sorted(missing)}"


FILE_EXTENSIONS = (".py", ".md", ".json", ".yml", ".yaml", ".ts", ".tsx", ".sh", ".toml", ".html", ".css", ".txt")
_SKIP_DIRS = {".git", "node_modules", ".venv", "__pycache__", "ui_dist", ".ruff_cache", ".pytest_cache"}


def repo_file_names() -> set[str]:
    """Every file name in the repository, for prose that names a file without its folder (`states.json`)."""
    import os

    names: set[str] = set()
    for _, dirs, files in os.walk(ROOT):
        dirs[:] = [d for d in dirs if d not in _SKIP_DIRS]
        names.update(files)
    return names


@pytest.mark.parametrize("doc", HAND_WRITTEN, ids=rel)
def test_file_paths_mentioned_exist(doc):
    # Only things that look like repo files (a known extension) or directories (trailing slash) are checked, so
    # Docker image names such as `mher/flower` are not mistaken for paths.
    missing = []
    for snippet in re.findall(r"`([^`\n]+)`", strip_fences(doc.read_text())):
        s = snippet.strip().removeprefix("./")
        if not re.fullmatch(r"[\w.\-]+(/[\w.\-]+)*/?", s) or s.startswith(("/", ".")):
            continue
        if not (s.endswith("/") or s.endswith(FILE_EXTENSIONS)):
            continue
        if any(s.startswith(prefix) for prefix in NOT_IN_REPO):
            continue
        exists = (ROOT / s).exists() or (doc.parent / s).exists() if "/" in s else s in repo_file_names()
        if not exists:
            missing.append(s)
    assert not missing, f"{rel(doc)} mentions files that do not exist: {missing}"


@pytest.mark.parametrize("doc", HAND_WRITTEN, ids=rel)
def test_localhost_ports_mentioned_are_really_published(doc):
    published = gen_docs.published_ports()
    wrong = sorted({int(p) for p in re.findall(r"localhost:(\d+)", doc.read_text())} - published)
    assert not wrong, f"{rel(doc)} mentions localhost ports the stack does not publish: {wrong} (published: {sorted(published)})"


# ---- coverage: new faults / scenarios must be documented ------------------------------------------------
def test_every_pipeline_fault_is_documented():
    from app.tasks.pipeline import FAULTS

    text = (ROOT / "docs" / "pipeline.md").read_text()
    missing = [f for f in FAULTS if f"`{f}`" not in text]
    assert not missing, f"document these faults in docs/pipeline.md: {missing}"


def test_every_demo_scenario_is_documented():
    from app.viz_catalog import SCENARIOS

    text = (ROOT / "docs" / "visualizer.md").read_text()
    missing = [s["id"] for s in SCENARIOS if f"`{s['id']}`" not in text]
    assert not missing, f"document these scenarios in docs/visualizer.md: {missing}"


# ---- house rules ---------------------------------------------------------------------------------------------
@pytest.mark.parametrize("doc", HAND_WRITTEN, ids=rel)
def test_no_file_trees(doc):
    # Trees go stale at the next rename; the generated source map (docs/reference/modules.md) replaces them.
    assert not re.search(r"[├└]──", doc.read_text()), f"{rel(doc)} contains a file tree: link to docs/reference/modules.md instead"


def test_every_api_route_has_a_description():
    # The generated API page shows each route's docstring; without one it would only show an auto-generated title.
    from app.main import app as pipeline_app
    from celery_viz.server import create_app
    from celery_viz.store import RunStore

    missing = []
    for name, app in (("pipeline", pipeline_app), ("visualizer", create_app(RunStore(), mode="replay"))):
        for path, methods in app.openapi()["paths"].items():
            for method, op in methods.items():
                if not (op.get("description") or "").strip():
                    missing.append(f"{name} {method.upper()} {path}")
    assert not missing, f"add a docstring to these routes: {missing}"


def test_source_code_only_points_at_docs_that_exist():
    """Docstrings and comments that say 'see docs/x.md' go stale when pages move; this catches them."""
    import os

    bad = []
    for base, dirs, files in os.walk(ROOT):
        dirs[:] = [d for d in dirs if d not in _SKIP_DIRS and d != "reference"]
        for name in files:
            if not name.endswith((".py", ".ts", ".tsx", ".sh", ".yml", ".json")) or name == "test_docs.py":
                continue
            path = Path(base) / name
            for ref in re.findall(r"\b((?:docs/)[\w./-]+\.md|[A-Z][A-Z_]+\.md)\b", path.read_text(errors="ignore")):
                if not (ROOT / ref).exists():
                    bad.append(f"{rel(path)} -> {ref}")
    assert not bad, f"code points at docs that do not exist: {bad}"
