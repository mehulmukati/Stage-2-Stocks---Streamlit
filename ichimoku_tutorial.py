"""Markdown and visual helpers for the in-app Ichimoku tutorial."""

from __future__ import annotations

import html
import re
from pathlib import Path

TUTORIAL_PATH = Path(__file__).resolve().parent / "docs" / "ichimoku_comprehensive_tutorial.md"

_FENCED_TEXT = re.compile(r"```text\s*\n(.*?)\n```", re.DOTALL)
_CHEAT_SECTION = re.compile(
    r"^## 69\. The One-Page Cheat Sheet\s*$(.*?)(?=^## 70\.)",
    re.MULTILINE | re.DOTALL,
)
_SEPARATOR = re.compile(r"^[=-]{8,}$")

_THEMES = {
    "dark": {
        "background": "#0f1420",
        "border": "#30384a",
        "text": "#d7dce5",
        "muted": "#8b93a7",
        "grid": "#222a3a",
        "bull": "#4ade80",
        "bear": "#f87171",
        "cloud": "#a78bfa",
        "tenkan": "#38bdf8",
        "kijun": "#f59e0b",
        "chikou": "#e879f9",
    },
    "light": {
        "background": "#ffffff",
        "border": "#cbd5e1",
        "text": "#0f172a",
        "muted": "#64748b",
        "grid": "#e2e8f0",
        "bull": "#16a34a",
        "bear": "#dc2626",
        "cloud": "#7c3aed",
        "tenkan": "#0284c7",
        "kijun": "#d97706",
        "chikou": "#c026d3",
    },
}


def load_tutorial(path: Path = TUTORIAL_PATH) -> str:
    """Load the tutorial from its canonical Markdown source."""
    return path.read_text(encoding="utf-8")


def strip_title(markdown: str) -> str:
    """Remove the document H1 because the tab supplies its own page header."""
    lines = markdown.splitlines()
    if lines and lines[0].startswith("# "):
        return "\n".join(lines[1:]).lstrip()
    return markdown


def strip_cheat_sheet(markdown: str) -> str:
    """Remove the standalone cheat sheet from the long-form tutorial view."""
    return _CHEAT_SECTION.sub(
        "\n## 69. One-Page Cheat Sheet\n\nOpen the **⚡ Cheat Sheet** tab for the compact reference.\n\n---\n\n",
        markdown,
    )


def _is_ascii_diagram(block: str) -> bool:
    """Distinguish visual sketches from formulas and short rule snippets."""
    lines = [line for line in block.splitlines() if line.strip()]
    if len(lines) < 3:
        return False
    joined = "\n".join(lines)
    if "Kijun  ------" in joined:
        return True
    if any(character in joined for character in ("█", "│", "─", "┌", "┐", "└", "┘")):
        return True
    art_lines = sum(bool(re.search(r"^\s*(?:[/\\~]{2,}|[-_]{6,}|[|↑↓V]+)(?:\s*[^/\\]*)?$", line)) for line in lines)
    return art_lines >= 2


def tutorial_parts(markdown: str) -> list[tuple[str, str]]:
    """Split Markdown into normal prose and diagram blocks."""
    parts: list[tuple[str, str]] = []
    cursor = 0
    for match in _FENCED_TEXT.finditer(markdown):
        if match.start() > cursor:
            parts.append(("markdown", markdown[cursor : match.start()]))
        block = match.group(1)
        if _is_ascii_diagram(block):
            parts.append(("diagram", block))
        else:
            parts.append(("markdown", match.group(0)))
        cursor = match.end()
    if cursor < len(markdown):
        parts.append(("markdown", markdown[cursor:]))
    return [(kind, content) for kind, content in parts if content.strip()]


def _diagram_kind(block: str) -> str:
    lower = block.lower()
    if "below cloud" in lower and "inside cloud" in lower:
        return "breakout"
    if "above cloud" in lower and "below cloud" in lower:
        return "regime"
    if "future cloud" in lower and "today" in lower:
        return "future"
    if "thick cloud" in lower or lower.count("████") >= 3 and len(block.splitlines()) <= 4:
        return "thickness"
    if "chikou" in lower and "clear space" in lower:
        return "chikou"
    if "strong bullish structure" in lower or "bullish kumo" in lower and "chikou" in lower:
        return "bullish"
    if "bearish kumo" in lower and "price" in lower:
        return "bearish"
    if "back inside" in lower:
        return "false_breakout"
    if "pullback" in lower:
        return "pullback"
    if "price  ~" in lower:
        return "sideways"
    if "top of kumo" in lower:
        return "hierarchy"
    if "declines again" in lower:
        return "resistance"
    if "kijun  ---" in lower:
        return "flat_kijun"
    return "kijun"


def _label(x: int, y: int, text: str, color: str, size: int = 15, weight: int = 600) -> str:
    return (
        f'<text x="{x}" y="{y}" fill="{color}" font-family="Inter,Segoe UI,sans-serif" '
        f'font-size="{size}" font-weight="{weight}">{html.escape(text)}</text>'
    )


def _cloud(points: str, palette: dict[str, str], bearish: bool = False, opacity: float = 0.32) -> str:
    color = palette["bear"] if bearish else palette["bull"]
    return f'<polygon points="{points}" fill="{color}" fill-opacity="{opacity}" stroke="{color}" stroke-width="2"/>'


def _market_lines(kind: str, palette: dict[str, str]) -> str:
    bull_price = "M86 220 C170 205 220 172 292 184 S410 130 490 138 S620 78 792 62"
    bear_price = "M86 82 C180 98 240 130 318 120 S440 178 510 168 S640 220 792 244"
    if kind == "bullish":
        return (
            _cloud("80,255 230,240 380,248 540,216 700,224 800,200 800,275 80,275", palette)
            + f'<path d="{bull_price}" fill="none" stroke="{palette["text"]}" stroke-width="4"/>'
            + f'<path d="M90 230 C230 215 340 183 480 170 S650 122 790 105" fill="none" '
            f'stroke="{palette["tenkan"]}" stroke-width="3"/>'
            + f'<path d="M90 246 C260 235 390 214 520 198 S660 160 790 150" fill="none" '
            f'stroke="{palette["kijun"]}" stroke-width="3"/>'
            + f'<path d="M55 192 C150 162 220 140 310 128" fill="none" stroke="{palette["chikou"]}" '
            'stroke-width="2.5" stroke-dasharray="7 6"/>'
            + _label(704, 48, "PRICE", palette["text"])
            + _label(690, 100, "TENKAN", palette["tenkan"])
            + _label(694, 146, "KIJUN", palette["kijun"])
            + _label(110, 268, "BULLISH KUMO", palette["bull"])
            + _label(62, 183, "CHIKOU", palette["chikou"], 13)
        )
    if kind == "bearish":
        return (
            _cloud("80,58 230,75 380,62 540,96 700,88 800,112 800,32 80,32", palette, bearish=True)
            + f'<path d="{bear_price}" fill="none" stroke="{palette["text"]}" stroke-width="4"/>'
            + f'<path d="M90 105 C230 118 350 154 490 166 S650 210 790 222" fill="none" '
            f'stroke="{palette["tenkan"]}" stroke-width="3"/>'
            + f'<path d="M90 88 C250 96 390 130 520 142 S670 177 790 188" fill="none" '
            f'stroke="{palette["kijun"]}" stroke-width="3"/>'
            + _label(675, 264, "PRICE", palette["text"])
            + _label(682, 222, "TENKAN", palette["tenkan"])
            + _label(688, 184, "KIJUN", palette["kijun"])
            + _label(110, 58, "BEARISH KUMO", palette["bear"])
        )
    return ""


def _diagram_body(kind: str, palette: dict[str, str]) -> tuple[str, str]:
    if kind in ("bullish", "bearish"):
        return (
            "Textbook bullish alignment" if kind == "bullish" else "Textbook bearish alignment",
            _market_lines(kind, palette),
        )
    if kind == "regime":
        body = (
            _cloud(
                "70,150 210,138 340,157 490,141 630,158 800,143 800,210 650,196 500,216 340,199 200,214 70,202", palette
            )
            + '<path d="M88 104 C190 80 270 118 360 88 S520 92 610 60 S735 75 790 48" fill="none" '
            f'stroke="{palette["bull"]}" stroke-width="4"/>'
            + '<path d="M88 264 C190 236 275 278 370 246 S520 264 610 230 S730 252 790 224" fill="none" '
            f'stroke="{palette["bear"]}" stroke-width="4"/>'
            + _label(88, 72, "ABOVE CLOUD · bullish territory", palette["bull"])
            + _label(88, 188, "KUMO · transition zone", palette["text"])
            + _label(88, 286, "BELOW CLOUD · bearish territory", palette["bear"])
        )
        return "Price relative to the Kumo", body
    if kind == "future":
        body = (
            '<path d="M70 210 C160 178 240 202 330 150 S420 162 470 112" fill="none" '
            f'stroke="{palette["text"]}" stroke-width="4"/>'
            + f'<line x1="500" y1="38" x2="500" y2="274" stroke="{palette["muted"]}" stroke-dasharray="7 7"/>'
            + _cloud("540,158 610,137 680,149 745,111 810,126 810,214 745,199 680,225 610,210 540,224", palette)
            + _label(454, 30, "TODAY", palette["text"], 13)
            + _label(86, 252, "Observed price", palette["muted"], 14)
            + _label(610, 248, "Projected 26 periods", palette["cloud"], 14)
            + '<line x1="515" y1="80" x2="780" y2="80" stroke="'
            + palette["cloud"]
            + '" stroke-width="2" marker-end="url(#arrow)"/>'
        )
        return "Why the cloud is plotted forward", body
    if kind == "thickness":
        body = (
            _cloud(
                "92,84 240,66 380,92 520,70 690,88 790,68 790,236 650,216 510,240 370,218 220,242 92,224",
                palette,
                opacity=0.38,
            )
            + '<line x1="820" y1="82" x2="820" y2="224" stroke="'
            + palette["text"]
            + '" stroke-width="2" marker-start="url(#arrowBack)" marker-end="url(#arrow)"/>'
            + _label(632, 156, "THICK KUMO", palette["bull"], 16)
            + _label(696, 266, "broader support / resistance zone", palette["muted"], 13)
        )
        return "Cloud thickness", body
    if kind == "chikou":
        candles = "".join(
            f'<line x1="{x}" y1="{150 + (x % 35)}" x2="{x}" y2="{226 - (x % 28)}" stroke="{palette["muted"]}"/>'
            f'<rect x="{x - 6}" y="{168 + (x % 24)}" width="12" height="30" fill="{palette["muted"]}" fill-opacity=".5"/>'  # noqa: E501
            for x in range(100, 430, 42)
        )
        body = (
            candles
            + f'<path d="M90 105 C180 78 255 95 335 62 S420 74 486 42" fill="none" stroke="{palette["chikou"]}" stroke-width="4"/>'  # noqa: E501
            + '<line x1="474" y1="48" x2="474" y2="150" stroke="'
            + palette["chikou"]
            + '" stroke-width="2" marker-end="url(#arrow)"/>'
            + _label(92, 52, "CHIKOU", palette["chikou"])
            + _label(530, 92, "Clear of historical price", palette["bull"], 16)
            + _label(104, 248, "Candles 26 periods earlier", palette["muted"], 13)
        )
        return "Chikou clearance test", body
    if kind in ("breakout", "false_breakout"):
        path = "M90 246 C210 232 290 215 370 176 S505 118 620 78 S730 70 800 46"
        title = "Bullish Kumo breakout"
        if kind == "false_breakout":
            path = "M90 244 C220 220 330 205 420 154 S535 65 610 78 S700 142 800 176"
            title = "False breakout: rejection back into the cloud"
        body = (
            _cloud(
                "70,128 220,120 360,140 500,122 650,142 800,126 800,208 650,194 500,214 350,197 210,211 70,201", palette
            )
            + f'<path d="{path}" fill="none" stroke="{palette["text"]}" stroke-width="4"/>'
            + _label(92, 232, "BELOW", palette["bear"], 13)
            + _label(360, 188, "INSIDE", palette["cloud"], 13)
            + _label(
                676,
                68 if kind == "breakout" else 166,
                "ABOVE" if kind == "breakout" else "BACK INSIDE",
                palette["bull"] if kind == "breakout" else palette["bear"],
                13,
            )
        )
        return title, body
    if kind == "pullback":
        body = (
            f'<path d="M75 245 C170 222 230 177 310 158 S430 88 510 72 C568 64 594 132 655 145 S735 105 805 72" fill="none" stroke="{palette["text"]}" stroke-width="4"/>'  # noqa: E501
            + f'<path d="M80 232 C230 216 350 181 490 161 S660 148 800 124" fill="none" stroke="{palette["kijun"]}" stroke-width="3"/>'  # noqa: E501
            + '<circle cx="640" cy="148" r="8" fill="'
            + palette["bull"]
            + '"/>'
            + _label(654, 172, "pullback holds near Kijun", palette["bull"], 14)
            + _label(704, 118, "KIJUN", palette["kijun"], 13)
        )
        return "Healthy bullish pullback", body
    if kind == "sideways":
        body = (
            _cloud(
                "70,154 190,143 300,162 420,146 540,164 660,148 800,160 800,205 670,191 545,207 420,190 295,208 180,191 70,202",  # noqa: E501
                palette,
                opacity=0.18,
            )
            + f'<path d="M76 128 C140 82 205 172 270 116 S395 170 460 112 S590 168 655 114 S745 160 808 120" fill="none" stroke="{palette["text"]}" stroke-width="3.5"/>'  # noqa: E501
            + f'<path d="M76 145 C180 119 255 166 350 137 S520 158 610 139 S740 157 808 143" fill="none" stroke="{palette["tenkan"]}" stroke-width="2.5"/>'  # noqa: E501
            + f'<path d="M76 170 C210 154 350 180 475 160 S670 177 808 165" fill="none" stroke="{palette["kijun"]}" stroke-width="2.5"/>'  # noqa: E501
            + _label(620, 250, "Repeated crosses · no directional edge", palette["muted"], 14)
        )
        return "Sideways market and whipsaws", body
    if kind == "hierarchy":
        layers = [
            ("PRICE", 48, palette["text"]),
            ("TENKAN", 91, palette["tenkan"]),
            ("KIJUN", 134, palette["kijun"]),
            ("TOP OF KUMO", 190, palette["bull"]),
            ("BOTTOM OF KUMO", 244, palette["cloud"]),
        ]
        body = "".join(
            f'<rect x="125" y="{y - 25}" width="620" height="34" rx="8" fill="{color}" fill-opacity=".14" stroke="{color}"/>'  # noqa: E501
            + _label(150, y - 2, text, color, 14)
            for text, y, color in layers
        )
        body += (
            '<line x1="780" y1="35" x2="780" y2="255" stroke="'
            + palette["muted"]
            + '" stroke-width="2" marker-end="url(#arrow)"/>'
        )
        return "Support hierarchy in a bull trend", body
    if kind == "resistance":
        body = (
            _cloud(
                "70,58 230,72 380,54 530,75 670,59 805,76 805,137 670,121 530,140 380,119 220,136 70,122",
                palette,
                bearish=True,
            )
            + f'<path d="M90 250 C195 228 268 196 350 171 S470 105 545 128 S625 191 700 205 S765 222 808 246" fill="none" stroke="{palette["text"]}" stroke-width="4"/>'  # noqa: E501
            + '<circle cx="540" cy="128" r="8" fill="'
            + palette["bear"]
            + '"/>'
            + _label(560, 115, "rejection", palette["bear"], 14)
            + _label(110, 96, "KUMO RESISTANCE", palette["bear"], 14)
        )
        return "Cloud as resistance", body
    if kind == "flat_kijun":
        body = (
            f'<path d="M80 172 C135 86 205 225 270 128 S390 205 460 108 S585 216 650 120 S760 195 810 95" fill="none" stroke="{palette["text"]}" stroke-width="4"/>'  # noqa: E501
            + f'<line x1="70" y1="212" x2="815" y2="212" stroke="{palette["kijun"]}" stroke-width="4"/>'
            + _label(670, 204, "FLAT KIJUN", palette["kijun"], 14)
            + _label(92, 250, "persistent equilibrium level", palette["muted"], 13)
        )
        return "Flat Kijun", body
    body = (
        _cloud("70,230 220,218 370,231 520,210 670,224 805,205 805,264 70,264", palette)
        + f'<path d="M82 208 C180 185 250 142 340 132 S490 92 580 75 S710 62 810 45" fill="none" stroke="{palette["text"]}" stroke-width="4"/>'  # noqa: E501
        + f'<path d="M82 218 C220 198 340 163 470 145 S670 103 810 85" fill="none" stroke="{palette["tenkan"]}" stroke-width="3"/>'  # noqa: E501
        + f'<line x1="70" y1="195" x2="815" y2="195" stroke="{palette["kijun"]}" stroke-width="3"/>'
        + _label(698, 82, "TENKAN", palette["tenkan"], 13)
        + _label(698, 188, "KIJUN", palette["kijun"], 13)
    )
    return "Price, Tenkan, Kijun, and Kumo", body


def diagram_svg(block: str, theme: str = "dark") -> str:
    """Transform an ASCII sketch into a semantic, chart-like SVG illustration."""
    palette = _THEMES["light" if theme.lower() == "light" else "dark"]
    title, body = _diagram_body(_diagram_kind(block), palette)
    accessible_text = html.escape(" ".join(line.strip() for line in block.splitlines() if line.strip()))
    definitions = (
        '<defs><marker id="arrow" markerWidth="8" markerHeight="8" refX="5" refY="3" orient="auto">'
        f'<path d="M0,0 L0,6 L6,3 z" fill="{palette["muted"]}"/></marker>'
        '<marker id="arrowBack" markerWidth="8" markerHeight="8" refX="1" refY="3" orient="auto-start-reverse">'
        f'<path d="M6,0 L6,6 L0,3 z" fill="{palette["muted"]}"/></marker></defs>'
    )
    grid = "".join(
        f'<line x1="54" y1="{y}" x2="846" y2="{y}" stroke="{palette["grid"]}"/>' for y in (78, 132, 186, 240)
    )
    return (
        f'<svg class="ichimoku-diagram" role="img" aria-label="{accessible_text}" width="100%" '
        'style="display:block;height:auto;margin:0.75rem 0 1.25rem" '
        'viewBox="0 0 900 340" xmlns="http://www.w3.org/2000/svg">'
        + definitions
        + f'<rect x="1" y="1" width="898" height="338" rx="16" fill="{palette["background"]}" '
        f'stroke="{palette["border"]}"/>'
        + _label(34, 34, title, palette["text"], 17, 700)
        + f'<g transform="translate(0 32)">{grid}{body}</g></svg>'
    )


def extract_cheat_sheet(markdown: str) -> list[tuple[str, list[str]]]:
    """Extract the cheat-sheet groups from section 69 of the source document."""
    section_match = _CHEAT_SECTION.search(markdown)
    if not section_match:
        return []
    fence_match = _FENCED_TEXT.search(section_match.group(1))
    if not fence_match:
        return []

    groups: list[tuple[str, list[str]]] = []
    heading: str | None = None
    entries: list[str] = []
    for raw_line in fence_match.group(1).splitlines():
        line = raw_line.strip()
        if not line or _SEPARATOR.match(line) or line == "ICHIMOKU CHEAT SHEET":
            continue
        if line == line.upper() and not any(character in line for character in "=><"):
            if heading is not None:
                groups.append((heading, entries))
            heading, entries = line.title(), []
        elif heading is not None:
            entries.append(re.sub(r"\s{2,}", " — ", line))
    if heading is not None:
        groups.append((heading, entries))
    return groups
