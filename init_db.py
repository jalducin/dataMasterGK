#!/usr/bin/env python
# init_db.py — Script de inicialización de la base de datos

from src.log_database import crear_tablas_si_no_existen

if __name__ == "__main__":
    crear_tablas_si_no_existen()
    print("✅ Tablas de la base de datos creadas o verificadas correctamente.")