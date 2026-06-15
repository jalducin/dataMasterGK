"""Configuración de pruebas: expone `src` y la raíz del repo en el path."""
import os
import re
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(REPO_ROOT, "src")

for p in (SRC, REPO_ROOT):
    if p not in sys.path:
        sys.path.insert(0, p)

GOLDEN_DIR = os.path.join(REPO_ROOT, "tests", "golden")


def normalizar_xml(xml: str) -> str:
    """Neutraliza campos variables (timestamps) para comparar contra el golden."""
    return re.sub(r'ChangeTimestamp="[^"]*"', 'ChangeTimestamp="NORM"', xml)


def leer_golden(nombre: str) -> str:
    with open(os.path.join(GOLDEN_DIR, nombre), encoding="utf-8") as fh:
        return fh.read()
