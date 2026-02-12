"""
One-off script to dump PDF structure (words + bbox + font) for analysis.
Output: text and JSON per PDF to understand actual header vs data layout.
"""

import json
import sys
from pathlib import Path

import fitz

def analyze_pdf(pdf_path: Path, out_dir: Path) -> None:
    doc = fitz.open(pdf_path)
    name = pdf_path.stem.replace("&", "_")
    lines_out = []
    all_words = []
    try:
        for page_no in range(min(3, len(doc))):  # first 3 pages
            page = doc[page_no]
            rect = page.rect
            lines_out.append(f"\n=== Page {page_no + 1} (h={rect.height:.0f}) ===\n")
            words = page.get_text("words", sort=True)
            # Dict for font sizes
            blocks = page.get_text("dict")
            y_to_font = {}
            for block in blocks.get("blocks", []):
                for line in block.get("lines", []):
                    y0 = line.get("bbox", (0,0,0,0))[1]
                    for span in line.get("spans", []):
                        y_to_font[int(y0)] = span.get("size", 0)
            # Group by y (lines)
            from collections import defaultdict
            by_y = defaultdict(list)
            for w in words:
                if len(w) < 5:
                    continue
                x0, y0, x1, y1, text = w[0], w[1], w[2], w[3], w[4]
                fs = y_to_font.get(int(y0), 0)
                by_y[int(y0)].append((x0, text, fs))
                all_words.append({"page": page_no, "y": y0, "x0": x0, "text": text, "font_size": fs})
            for y in sorted(by_y.keys())[:35]:  # first 35 lines
                toks = by_y[y]
                toks.sort(key=lambda t: t[0])
                line_text = " | ".join(f"{t[1]!r}(fs={t[2]})" for t in toks[:20])
                if len(toks) > 20:
                    line_text += " ..."
                lines_out.append(f"y={y}: {line_text}\n")
    finally:
        doc.close()
    out_dir.mkdir(parents=True, exist_ok=True)
    txt_path = out_dir / f"{name}_structure.txt"
    txt_path.write_text("".join(lines_out), encoding="utf-8")
    json_path = out_dir / f"{name}_words.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(all_words[:500], f, indent=0)
    print(f"Wrote {txt_path} and {json_path}")


if __name__ == "__main__":
    folder = Path(sys.argv[1] if len(sys.argv) > 1 else "pdfsamples")
    out = Path("analysis_out")
    for f in sorted(folder.glob("*.pdf")) + sorted(folder.glob("*.PDF")) + sorted(folder.glob("*.Pdf")):
        analyze_pdf(f, out)
