"""Pruebas golden: el XML generado para GK no debe cambiar tras el rediseño.

Comparan la salida de los generadores puros de `utils` contra muestras conocidas
en `tests/golden/`. Si una prueba falla, el refactor alteró el XML que GK espera.
"""
import utils
from conftest import leer_golden, normalizar_xml


def test_store_xml_coincide_con_golden():
    root = utils.generar_store_xml(
        "100", "Tienda Centro", "Centro", "CDMX", "CDMX", "Cuauhtemoc",
        "Av. Juarez 1", "5550001111", "MX", "http://x", "MXN", "es-MX",
        "CST", "GMT-6", "ABC123", "0000000100",
    )
    salida = utils.serializar_xml(root, standalone=False)
    assert normalizar_xml(salida) == leer_golden("store.xml")


def test_operator_xml_coincide_con_golden():
    root = utils.generar_operator_xml(
        "200", "Ana", "Lopez", "es-MX", "MX",
        "1990", "5", "10", "100", "CASHIER",
        {"Web": "w1", "Mobile": "p1", "POS": "p1"}, "0000000200",
    )
    salida = utils.serializar_xml(root, standalone=False)
    assert normalizar_xml(salida) == leer_golden("operator.xml")


def test_promotion_xml_coincide_con_golden():
    pr = ["P1", "Promo 1", "2026-01-01", "2026-12-31", "PR1", "1", "99", "10", "1", "U"]
    items = [["P1", "ITEM1"]]
    stores = [["x", "100"]]
    root = utils.generar_promotion_xml(pr, items, stores)
    salida = utils.serializar_xml(root, standalone=True)
    assert normalizar_xml(salida) == leer_golden("promotion.xml")


def test_promotion_category_xml_coincide_con_golden():
    pr = ["P1", "Promo 1", "2026-01-01", "2026-12-31", "PR1", "1", "99", "10", "1", "U"]
    cat_map = {"P1": "CAT1"}
    stores = [["x", "100"]]
    root = utils.generar_promotion_category_xml(pr, cat_map, stores)
    salida = utils.serializar_xml(root, standalone=True)
    assert normalizar_xml(salida) == leer_golden("promotion_category.xml")
