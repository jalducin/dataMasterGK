import sqlite3
import os

class LogDatabase:
    """Manejador de la BD local LogDatabaseDataGK.db."""

    def __init__(self) -> None:
        base = os.path.dirname(__file__)
        self.db_path = os.path.abspath(os.path.join(base, "../db/LogDatabaseDataGK.db"))
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)

    def create_tables(self) -> None:
        """Crea todas las tablas necesarias con timestamp local por defecto."""
        ddl = """
        CREATE TABLE IF NOT EXISTS Operadores (
            codigo      TEXT,
            nombre      TEXT,
            ubicacion   TEXT,
            estado      TEXT,
            fecha       TIMESTAMP DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS Tiendas (
            codigo      TEXT,
            nombre      TEXT,
            ubicacion   TEXT,
            estado      TEXT,
            fecha       TIMESTAMP DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS Promociones (
            codigo         TEXT PRIMARY KEY,
            descripcion    TEXT,
            fecha_inicio   TEXT,
            fecha_fin      TEXT,
            impresora      TEXT,
            fecha          TIMESTAMP DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS Categorias (
            codigo         TEXT PRIMARY KEY,
            descripcion    TEXT,
            fecha_inicio   TEXT,
            fecha_fin      TEXT,
            impresora      TEXT,
            fecha          TIMESTAMP DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS XML_Generados (
            tipo           TEXT,
            nombre_archivo TEXT,
            ruta           TEXT,
            estado         TEXT,
            descripcion    TEXT,
            fecha          TIMESTAMP DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS Logs_del_Sistema (
            tipo    TEXT,
            mensaje TEXT,
            fecha   TIMESTAMP DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS ProgramacionInterfaces (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            interface       TEXT,
            hora            TEXT,
            activo          INTEGER DEFAULT 1,
            fecha_guardado  TIMESTAMP DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS Ejecuciones (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tipo TEXT,
            interfaz TEXT,
            estado TEXT,
            mensaje TEXT,
            fecha TIMESTAMP DEFAULT (datetime('now','localtime'))
        );
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript(ddl)

def crear_tablas_si_no_existen() -> None:
    LogDatabase().create_tables()
