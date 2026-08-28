from ichimoku_tutorial import (
    diagram_svg,
    extract_cheat_sheet,
    load_tutorial,
    strip_cheat_sheet,
    strip_title,
    tutorial_parts,
)


def test_tutorial_source_loads_and_title_can_be_removed():
    tutorial = load_tutorial()
    assert tutorial.startswith("# Ichimoku Cloud")
    assert not strip_title(tutorial).startswith("# Ichimoku Cloud")


def test_tutorial_parts_promote_ascii_art_but_leave_formula_as_markdown():
    source = """Intro

```text
Price
  ↑
████ Cloud ████
  ↓
Price
```

```text
Tenkan = (high + low) / 2
```"""
    parts = tutorial_parts(source)
    assert [kind for kind, _ in parts] == ["markdown", "diagram", "markdown"]
    assert "```text" in parts[-1][1]


def test_diagram_svg_uses_requested_theme_and_escapes_content():
    dark = diagram_svg("Price\n████ Kumo ████\nA < B", "dark")
    light = diagram_svg("Price\n████ Kumo ████\nA < B", "light")
    assert dark.startswith('<svg class="ichimoku-diagram"')
    assert "#0f1420" in dark
    assert "#ffffff" in light
    assert "A &lt; B" in light
    assert "<polygon" in light
    assert "<path" in light
    assert "ui-monospace" not in light


def test_cheat_sheet_is_extracted_from_the_canonical_document():
    tutorial = load_tutorial()
    groups = dict(extract_cheat_sheet(tutorial))
    assert "Price Vs Cloud" in groups
    assert "Above cloud — = Bullish" in groups["Price Vs Cloud"]
    assert "Ideal Bull" in groups
    assert "Market State" in groups
    assert "Open the **⚡ Cheat Sheet** tab" in strip_cheat_sheet(tutorial)
    assert "ICHIMOKU CHEAT SHEET" not in strip_cheat_sheet(tutorial)
