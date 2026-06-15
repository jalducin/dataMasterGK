"""Despacho de ejecución manual de una interfaz desde el panel.

Ejecuta cada interfaz UNA sola vez (antes se invocaba dos veces, duplicando trabajo y envíos).
"""
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "src")))

import utils
from src.classes.operator import Operator
from src.classes.promotion import Promotion
from src.classes.promotion_category import PromotionCategory
from src.classes.store import Store

# Nombre de interfaz (GK) → (clase, etiqueta para Ejecuciones)
INTERFACES = {
    "Operator": (Operator, "operator"),
    "Store": (Store, "store"),
    "Promotion": (Promotion, "promotion"),
    "Promotion Category": (PromotionCategory, "promotion_category"),
}


def run_single_interface(config, name):
    entry = INTERFACES.get(name)
    if not entry:
        utils.log_interfaces("ERROR", f"Interfaz desconocida: {name}")
        raise ValueError(f"Interfaz desconocida: {name}")

    clase, etiqueta = entry
    try:
        clase(config).read_file_items()
        utils.registrar_ejecucion("manual", etiqueta, "éxito", "Procesado correctamente")
    except Exception as e:
        utils.log_interfaces("ERROR", f"{name} falló: {e}")
        utils.registrar_ejecucion("manual", etiqueta, "error", str(e))
        raise
