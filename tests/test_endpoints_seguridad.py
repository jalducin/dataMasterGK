"""Pruebas de seguridad: los endpoints de consulta solo aceptan tipos de la lista blanca."""
import app as app_module


def _client():
    app_module.app.config.update(TESTING=True)
    return app_module.app.test_client()


def test_filtrar_fecha_rechaza_tabla_no_permitida():
    r = _client().get("/filtrar_fecha?tipo=Operadores&fecha=2026-01-01")
    assert r.status_code == 400


def test_filtrar_fecha_rechaza_intento_inyeccion():
    r = _client().get("/filtrar_fecha?tipo=x;DROP+TABLE+Logs_del_Sistema&fecha=2026-01-01")
    assert r.status_code == 400


def test_descargar_csv_rechaza_tabla_no_permitida():
    r = _client().get("/descargar_csv?tipo=sqlite_master&fecha=2026-01-01")
    assert r.status_code == 400


def test_filtrar_fecha_acepta_tipo_de_lista_blanca():
    # Tipo válido: no debe ser 400 (puede devolver lista vacía si no hay datos).
    r = _client().get("/filtrar_fecha?tipo=Logs_del_Sistema&fecha=2026-01-01")
    assert r.status_code == 200
