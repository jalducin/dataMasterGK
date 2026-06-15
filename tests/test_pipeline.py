"""Pruebas del pipeline ETL: una sola pasada, auditoría sin duplicados, esquema correcto.

No requiere servidor SFTP: el transmisor se reemplaza por uno falso.
"""
import json
import os
import sqlite3

import pandas as pd
import pytest

import utils
import src.classes.base as base
from log_database import LogDatabase


class _TransmisorFake:
    """Sustituye a transport.Transmisor: simula envío exitoso sin red."""
    enviados = []

    def __init__(self, *a, **k):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def enviar(self, xml_path, name):
        _TransmisorFake.enviados.append(name)
        return True


@pytest.fixture
def entorno(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "db").mkdir()
    dirs = {}
    for k in ["operadores", "tiendas", "promociones", "promociones_categoria"]:
        d = tmp_path / k
        d.mkdir()
        dirs[k] = str(d)
    config = {
        "operadores": [{"directory": dirs["operadores"]}],
        "tiendas": [{"directory": dirs["tiendas"]}],
        "promociones": [{"directory": dirs["promociones"]}],
        "promociones_categoria": [{"directory": dirs["promociones_categoria"]}],
        "server": [{"server": "", "user": "", "pwd": "", "pathUcon": "/x/", "protocol": "sftp", "port": 22}],
    }
    (tmp_path / "config.json").write_text(json.dumps(config), encoding="utf-8")
    # Crear el esquema en la ruta que usa el pipeline (relativa al cwd del test)
    ldb = LogDatabase()
    ldb.db_path = os.path.join(str(tmp_path), "db", "LogDatabaseDataGK.db")
    ldb.create_tables()
    monkeypatch.setattr(base, "Transmisor", _TransmisorFake)
    _TransmisorFake.enviados = []
    return config, dirs, tmp_path


def _contar(tmp_path, sql, args=()):
    with sqlite3.connect(os.path.join(str(tmp_path), "db", "LogDatabaseDataGK.db")) as c:
        return c.execute(sql, args).fetchone()[0]


def test_operator_pipeline_sin_duplicados_y_master_ok(entorno):
    config, dirs, tmp_path = entorno
    pd.DataFrame([{
        "Operator": "200", "Nombre": "Ana", "Apellido": "Lopez", "Año": "1990",
        "Mes ": "5", "Dia": "10", "Tienda": "100", "Role": "CASHIER",
        "PWD Web": "w1", "PWD POS": "p1", "Lenguaje": "es-MX", "Código Pais": "MX",
    }]).to_excel(os.path.join(dirs["operadores"], "ops.xlsx"), index=False)

    from src.classes.operator import Operator
    Operator(config).read_file_items()

    # Un único registro en XML_Generados (antes se registraba 2+ veces)
    assert _contar(tmp_path, "SELECT COUNT(*) FROM XML_Generados") == 1
    # La fila maestra de Operadores se inserta (antes fallaba por columna 'login')
    assert _contar(tmp_path, "SELECT COUNT(*) FROM Operadores WHERE codigo='200'") == 1
    # Se envió exactamente una vez (antes: 2x por la doble ejecución)
    assert len(_TransmisorFake.enviados) == 1
    assert _TransmisorFake.enviados[0].startswith("Operator_200_")
    # El Excel se movió a procesados (no se reprocesa)
    assert not os.path.exists(os.path.join(dirs["operadores"], "ops.xlsx"))


def test_store_pipeline_genera_un_xml(entorno):
    config, dirs, tmp_path = entorno
    pd.DataFrame([{
        "Tienda": "100", "Nombre Tienda": "Centro", "Nombre Sucursal": "C",
        "Ciudad": "CDMX", "Departamento": "CDMX", "Municipio": "Cuauhtemoc",
        "Direccion": "Av 1", "Telefono": "555", "CountryCode": "MX", "URL": "http://x",
        "Moneda": "MXN", "Lenguaje": "es-MX", "TimeZone": "CST", "TimeZoneGTM": "GMT-6",
        "VatRegistrationNumber": "ABC",
    }]).to_excel(os.path.join(dirs["tiendas"], "s.xlsx"), index=False)

    from src.classes.store import Store
    Store(config).read_file_items()

    assert _contar(tmp_path, "SELECT COUNT(*) FROM XML_Generados") == 1
    assert _contar(tmp_path, "SELECT COUNT(*) FROM Tiendas WHERE codigo='100'") == 1
    assert len(_TransmisorFake.enviados) == 1
