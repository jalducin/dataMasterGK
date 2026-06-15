"""Pipeline ETL único y compartido por las cuatro interfaces.

Cada interfaz solo implementa `generar()` (leer Excel → construir XML + fila maestra).
La base centraliza, una sola vez por archivo: escritura del XML, envío del lote con
una única conexión SFTP/FTP, auditoría (sin duplicados) y archivado idempotente.
"""
import os
import shutil
import sqlite3
from datetime import datetime

import pandas as pd

import utils
from transport import Transmisor


class ItemGenerado:
    """Un XML generado a partir de una fila/promoción, con su fila maestra."""

    def __init__(self, ident, root, name_file, standalone, master):
        self.ident = ident          # id para logs (store_id, operator_id, promo_id)
        self.root = root            # Element del XML
        self.name_file = name_file  # nombre de archivo XML
        self.standalone = standalone  # encabezado XML (Promociones=True)
        self.master = master        # (tabla, campos, valores) de la tabla maestra, o None
        self.xml_path = None        # ruta asignada por la base al escribir
        self.enviado = False        # resultado del envío


class InterfaceProcessor:
    """Plantilla del pipeline. Las subclases definen la configuración y `generar()`."""

    clave_config = None       # clave en config.json (ej. "tiendas")
    prefijo = ""              # prefijo del nombre de archivo (ej. "BU_")
    tipo_xml = ""             # etiqueta para XML_Generados.tipo (ej. "Tienda")
    usa_microsegundos = False  # formato del timestamp del nombre de archivo
    master_upsert = False     # tablas que actualizan en conflicto (Promociones/Categorias)

    def __init__(self, config: dict):
        self.config = config

    # -- API que implementa cada subclase ------------------------------------
    def generar(self, directory: str, file_path: str, element: str) -> list:
        """Lee el Excel y devuelve una lista de ItemGenerado. NO escribe archivos."""
        raise NotImplementedError

    # -- helpers de lectura/nombrado -----------------------------------------
    @staticmethod
    def leer_hoja(file_path: str, idx: int, cols: int):
        df = pd.read_excel(file_path, sheet_name=idx, dtype=str).iloc[:, :cols].fillna("")
        return df.apply(lambda c: c.map(lambda v: str(v).strip())).values.tolist()

    def nombre_archivo(self, ident: str) -> str:
        if self.usa_microsegundos:
            ts = datetime.now().strftime("%Y%m%d%H%M%S%f")[:-3]
        else:
            ts = datetime.now().strftime("%Y%m%d%H%M%S")
        return f"{self.prefijo}{ident}_{ts}.xml"

    # -- pipeline ------------------------------------------------------------
    def read_file_items(self):
        for path in self.config.get(self.clave_config, []):
            directory = path["directory"]
            os.makedirs(directory, exist_ok=True)
            self._procesar_directorio(directory)

    def _procesar_directorio(self, directory: str):
        conn = utils.conectar_db()
        try:
            for element in sorted(os.listdir(directory)):
                file_path = os.path.join(directory, element)
                if os.path.isdir(file_path):
                    continue
                if not element.lower().endswith(".xlsx") or element.startswith("~$"):
                    continue
                self._procesar_archivo(directory, element, file_path, conn)
            conn.commit()
        finally:
            conn.close()

    def _procesar_archivo(self, directory, element, file_path, conn):
        # 1) Generar (una sola lectura/transformación por archivo)
        try:
            items = self.generar(directory, file_path, element)
        except Exception as e:
            utils.log_interfaces(f"ERROR {self.tipo_xml.upper()}", f"{element}: {e}")
            self._insertar(conn, "Logs_del_Sistema", ["tipo", "mensaje"], ["ERROR", f"{element}: {e}"])
            conn.commit()
            utils.move_files_error(directory, element, file_path)
            return

        # 2) Escribir XML al disco
        xml_folder = os.path.join(directory, "xml")
        os.makedirs(xml_folder, exist_ok=True)
        for it in items:
            it.xml_path = os.path.join(xml_folder, it.name_file)
            with open(it.xml_path, "w", encoding="utf-8") as fh:
                fh.write(utils.serializar_xml(it.root, it.standalone))

        # 3) Enviar el lote reutilizando UNA sola conexión
        if items:
            try:
                with Transmisor() as tx:
                    for it in items:
                        if utils.wait_for_file_ready(it.xml_path):
                            it.enviado = tx.enviar(it.xml_path, it.name_file)
                        else:
                            utils.log_interfaces("ERROR", f"Archivo no accesible para envío: {it.xml_path}")
                            it.enviado = False
            except Exception as e:
                utils.log_interfaces("ERROR FTP", f"No se pudo abrir conexión de envío: {e}")
                for it in items:
                    it.enviado = False

        # 4) Auditar (una sola vez por XML)
        for it in items:
            estado = "Enviado" if it.enviado else "Pendiente"
            if it.master:
                self._insertar_master(conn, it.master)
            self._insertar(
                conn, "XML_Generados",
                ["tipo", "nombre_archivo", "ruta", "estado", "descripcion"],
                [self.tipo_xml, it.name_file, it.xml_path, estado,
                 "Envío completado" if it.enviado else "Generado; envío pendiente"],
            )
            self._insertar(
                conn, "Logs_del_Sistema", ["tipo", "mensaje"],
                ["INFO", f"{self.tipo_xml} {it.ident} generada correctamente."],
            )
        conn.commit()

        # 5) Archivar XML (enviadas/no_enviadas)
        for it in items:
            subdir = "enviadas" if it.enviado else "no_enviadas"
            dest_dir = os.path.join(xml_folder, subdir, datetime.now().strftime("%Y%m%d"))
            os.makedirs(dest_dir, exist_ok=True)
            if os.path.exists(it.xml_path):
                try:
                    shutil.move(it.xml_path, os.path.join(dest_dir, it.name_file))
                except Exception as e:
                    utils.log_interfaces("ERROR", f"No se pudo mover XML {it.name_file}: {e}")

        # 6) Mover el Excel a procesados (idempotente: no se reprocesa)
        utils.move_files(directory, element, file_path)

    # -- persistencia (nombres de tabla son constantes del código, no entrada) --
    def _insertar(self, conn, table, fields, values):
        try:
            ph = ",".join(["?"] * len(values))
            conn.execute(f"INSERT INTO {table} ({','.join(fields)}) VALUES ({ph})", values)
        except Exception as e:
            utils.log_interfaces("ERROR DB", f"No se pudo registrar en {table}: {e}")

    def _insertar_master(self, conn, master):
        table, fields, values = master
        try:
            ph = ",".join(["?"] * len(values))
            conn.execute(f"INSERT INTO {table} ({','.join(fields)}) VALUES ({ph})", values)
        except sqlite3.IntegrityError:
            if self.master_upsert and table in ("Promociones", "Categorias"):
                conn.execute(
                    f"UPDATE {table} SET descripcion=?, fecha_inicio=?, fecha_fin=?, impresora=? WHERE codigo=?",
                    (values[1], values[2], values[3], values[4], values[0]),
                )
        except Exception as e:
            utils.log_interfaces("ERROR DB", f"{table}: {e}")
