#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class BBox:
    min_x: float
    min_y: float
    max_x: float
    max_y: float

    def pad(self, amount: float) -> "BBox":
        return BBox(
            min_x=self.min_x - amount,
            min_y=self.min_y - amount,
            max_x=self.max_x + amount,
            max_y=self.max_y + amount,
        )

    @property
    def width(self) -> float:
        return max(1.0, self.max_x - self.min_x)

    @property
    def height(self) -> float:
        return max(1.0, self.max_y - self.min_y)


def _escape_xml(text: str) -> str:
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _iter_points(el: dict) -> list[tuple[float, float]]:
    x = float(el.get("x", 0))
    y = float(el.get("y", 0))
    pts = el.get("points") or []
    out: list[tuple[float, float]] = []
    for p in pts:
        if not isinstance(p, list) or len(p) != 2:
            continue
        out.append((x + float(p[0]), y + float(p[1])))
    return out


def _compute_bbox(elements: list[dict]) -> BBox:
    min_x = 1e18
    min_y = 1e18
    max_x = -1e18
    max_y = -1e18

    def add(x: float, y: float) -> None:
        nonlocal min_x, min_y, max_x, max_y
        min_x = min(min_x, x)
        min_y = min(min_y, y)
        max_x = max(max_x, x)
        max_y = max(max_y, y)

    for el in elements:
        if el.get("isDeleted"):
            continue
        t = el.get("type")
        x = float(el.get("x", 0))
        y = float(el.get("y", 0))
        if t in ("line", "arrow", "freedraw"):
            pts = _iter_points(el)
            if pts:
                for px, py in pts:
                    add(px, py)
            else:
                w = float(el.get("width", 0))
                h = float(el.get("height", 0))
                add(x, y)
                add(x + w, y + h)
        else:
            w = float(el.get("width", 0))
            h = float(el.get("height", 0))
            add(x, y)
            add(x + w, y + h)

    if min_x > max_x or min_y > max_y:
        return BBox(0, 0, 100, 100)

    return BBox(min_x, min_y, max_x, max_y)


def _svg_for_shape(el: dict, *, dx: float, dy: float) -> str | None:
    t = el.get("type")
    x = float(el.get("x", 0)) - dx
    y = float(el.get("y", 0)) - dy
    w = float(el.get("width", 0))
    h = float(el.get("height", 0))

    stroke = el.get("strokeColor", "#1e1e1e")
    fill = el.get("backgroundColor", "transparent")
    sw = float(el.get("strokeWidth", 2))
    stroke_style = el.get("strokeStyle", "solid")
    dash = ' stroke-dasharray="10 7"' if stroke_style == "dashed" else ""

    if t == "rectangle":
        rx = 0.0
        roundness = el.get("roundness")
        if isinstance(roundness, dict) and roundness.get("type"):
            rx = 22.0
        return (
            f'<rect x="{x:.2f}" y="{y:.2f}" width="{w:.2f}" height="{h:.2f}" '
            f'rx="{rx:.2f}" fill="{fill}" stroke="{stroke}" stroke-width="{sw:.2f}"{dash}/>'
        )

    if t == "ellipse":
        cx = x + w / 2
        cy = y + h / 2
        return (
            f'<ellipse cx="{cx:.2f}" cy="{cy:.2f}" rx="{w/2:.2f}" ry="{h/2:.2f}" '
            f'fill="{fill}" stroke="{stroke}" stroke-width="{sw:.2f}"{dash}/>'
        )

    return None


def _svg_for_linear(el: dict, *, dx: float, dy: float) -> str | None:
    t = el.get("type")
    if t not in ("line", "arrow"):
        return None

    pts = _iter_points(el)
    if len(pts) < 2:
        return None

    stroke = el.get("strokeColor", "#1e1e1e")
    sw = float(el.get("strokeWidth", 2))
    stroke_style = el.get("strokeStyle", "solid")
    dash = ' stroke-dasharray="10 7"' if stroke_style == "dashed" else ""
    marker = ' marker-end="url(#arrow)"' if t == "arrow" else ""

    d = f"M {pts[0][0]-dx:.2f} {pts[0][1]-dy:.2f} "
    for px, py in pts[1:]:
        d += f"L {px-dx:.2f} {py-dy:.2f} "

    return f'<path d="{d}" fill="none" stroke="{stroke}" stroke-width="{sw:.2f}"{dash}{marker}/>'


def _svg_for_text(el: dict, *, dx: float, dy: float) -> str | None:
    if el.get("type") != "text":
        return None

    x = float(el.get("x", 0)) - dx
    y = float(el.get("y", 0)) - dy
    w = float(el.get("width", 0))
    fs = float(el.get("fontSize", 16))
    color = el.get("strokeColor", "#1e1e1e")
    text_align = el.get("textAlign", "left")
    line_height = float(el.get("lineHeight", 1.15))
    raw = el.get("text", "") or ""
    lines = raw.split("\n")

    if text_align == "center":
        anchor = "middle"
        tx = x + w / 2
    elif text_align == "right":
        anchor = "end"
        tx = x + w
    else:
        anchor = "start"
        tx = x

    out: list[str] = []
    for idx, line in enumerate(lines):
        ty = y + fs * (idx + 1) * line_height
        out.append(
            f'<text x="{tx:.2f}" y="{ty:.2f}" font-size="{fs:.2f}" '
            f'fill="{color}" text-anchor="{anchor}" dominant-baseline="alphabetic">'
            f"{_escape_xml(line)}</text>"
        )

    return "\n".join(out)


def excalidraw_to_svg(excalidraw_path: Path, svg_path: Path, *, padding: float = 40) -> None:
    data = json.loads(excalidraw_path.read_text(encoding="utf-8"))
    elements: list[dict] = data.get("elements") or []

    bbox = _compute_bbox(elements).pad(padding)
    dx = bbox.min_x
    dy = bbox.min_y

    parts: list[str] = []
    parts.append('<?xml version="1.0" encoding="UTF-8" standalone="no"?>')
    parts.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{bbox.width:.0f}" height="{bbox.height:.0f}" '
        f'viewBox="0 0 {bbox.width:.0f} {bbox.height:.0f}">'
    )
    parts.append(
        '<defs>'
        '<marker id="arrow" markerWidth="10" markerHeight="10" refX="9" refY="5" orient="auto" '
        'markerUnits="strokeWidth">'
        '<path d="M0,0 L10,5 L0,10 Z" fill="context-stroke"/></marker>'
        "</defs>"
    )
    parts.append('<g font-family="Söhne, Helvetica, Arial, sans-serif" stroke-linecap="round" stroke-linejoin="round">')

    # Draw order: shapes -> lines/arrows -> text
    for el in elements:
        if el.get("isDeleted"):
            continue
        frag = _svg_for_shape(el, dx=dx, dy=dy)
        if frag:
            parts.append(frag)

    for el in elements:
        if el.get("isDeleted"):
            continue
        frag = _svg_for_linear(el, dx=dx, dy=dy)
        if frag:
            parts.append(frag)

    for el in elements:
        if el.get("isDeleted"):
            continue
        frag = _svg_for_text(el, dx=dx, dy=dy)
        if frag:
            parts.append(frag)

    parts.append("</g></svg>")
    svg_path.write_text("\n".join(parts) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Convert a subset of .excalidraw JSON into a clean SVG.")
    parser.add_argument("input", type=Path, help="Input .excalidraw path.")
    parser.add_argument("output", type=Path, help="Output .svg path.")
    parser.add_argument("--padding", type=float, default=40, help="Padding (px) around content.")
    args = parser.parse_args()

    excalidraw_to_svg(args.input, args.output, padding=args.padding)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
