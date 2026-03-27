#!/usr/bin/env python3
"""Convert Fabric .py notebooks (with # %% cell markers) to .ipynb format.

Usage:
    python3 convert_notebooks.py                    # Convert all .py in fabric-notebooks/
    python3 convert_notebooks.py nb_bronze_cdc.py   # Convert a specific file

The .py files use VS Code / Jupyter cell markers:
    # %%              → code cell boundary
    # %% [markdown]   → markdown cell boundary
    # lines starting with '# ' inside markdown cells → stripped to plain markdown

Output .ipynb files are written alongside the .py source files.
"""
import json
import re
import sys
from pathlib import Path

NOTEBOOKS_DIR = Path(__file__).parent.parent / "fabric-notebooks"


def py_to_ipynb(py_path: Path) -> Path:
    """Convert a single .py file to .ipynb, return output path."""
    content = py_path.read_text()

    # Split on cell markers: # %% or # %% [markdown]
    parts = re.split(r"^# %%(.*)$", content, flags=re.MULTILINE)

    cells = []

    # First part = everything before the first # %% marker (preamble code cell)
    preamble = parts[0].strip()
    if preamble:
        cells.append({
            "cell_type": "code",
            "source": _format_source(preamble),
            "metadata": {},
            "outputs": [],
            "execution_count": None,
        })

    # Remaining parts come in pairs: (marker_text, cell_content)
    i = 1
    while i < len(parts):
        marker = parts[i].strip()
        cell_content = parts[i + 1].strip() if i + 1 < len(parts) else ""
        i += 2

        if not cell_content:
            continue

        if marker.startswith("[markdown]"):
            lines = cell_content.split("\n")
            md_lines = []
            for line in lines:
                if line.startswith("# "):
                    md_lines.append(line[2:])
                elif line == "#":
                    md_lines.append("")
                else:
                    md_lines.append(line)
            cells.append({
                "cell_type": "markdown",
                "source": _format_source("\n".join(md_lines)),
                "metadata": {},
            })
        else:
            cells.append({
                "cell_type": "code",
                "source": _format_source(cell_content),
                "metadata": {},
                "outputs": [],
                "execution_count": None,
            })

    notebook = {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            },
            "language_info": {
                "name": "python",
                "version": "3.10.0",
            },
        },
        "cells": cells,
    }

    out_path = py_path.with_suffix(".ipynb")
    out_path.write_text(json.dumps(notebook, indent=1) + "\n")
    return out_path


def _format_source(text: str) -> list:
    """Split text into lines with trailing newlines (Jupyter convention)."""
    lines = text.split("\n")
    if not lines:
        return []
    return [line + "\n" for line in lines[:-1]] + [lines[-1]]


def main():
    if len(sys.argv) > 1:
        # Convert specific files
        for name in sys.argv[1:]:
            py_path = NOTEBOOKS_DIR / name
            if not py_path.exists():
                print(f"  SKIP {name} — file not found")
                continue
            out = py_to_ipynb(py_path)
            cells = json.loads(out.read_text())["cells"]
            print(f"  {py_path.name} → {out.name} ({len(cells)} cells)")
    else:
        # Convert all .py files in the notebooks directory
        py_files = sorted(NOTEBOOKS_DIR.glob("nb_*.py"))
        if not py_files:
            print(f"  No nb_*.py files found in {NOTEBOOKS_DIR}")
            return

        for py_path in py_files:
            out = py_to_ipynb(py_path)
            cells = json.loads(out.read_text())["cells"]
            print(f"  {py_path.name} → {out.name} ({len(cells)} cells)")

    print("  Done!")


if __name__ == "__main__":
    main()
