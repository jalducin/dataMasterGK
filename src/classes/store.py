
import os, shutil, sqlite3
from datetime import datetime
import pandas as pd
import xml.etree.ElementTree as ET
from xml.dom import minidom

import utils

class Store:
    def __init__(self, config):
        self.config = config
        self.db_path = os.path.join("db", "LogDatabaseDataGK.db")

    def log_to_db(self, table, fields, values):
        try:
            with sqlite3.connect(self.db_path) as c:
                c.execute(
                    f"INSERT INTO {table} ({','.join(fields)}) VALUES ({','.join('?'*len(values))})",
                    values
                )
        except Exception as e:
            utils.log_interfaces("ERROR DB", f"No se pudo registrar en {table}: {e}")

    def read_file_items(self):
        for path in self.config["tiendas"]:
            directory = path["directory"]
            os.makedirs(directory, exist_ok=True)

            for element in os.listdir(directory):
                file_path = os.path.join(directory, element)
                if os.path.isdir(file_path):
                    continue

                if element.lower().endswith(".xlsx") and not element.startswith("~$"):
                    archivos_generados = []
                    try:
                        archivos_generados = self._create_files(directory, element, file_path)
                    except Exception as e:
                        utils.log_interfaces("ERROR STORE", f"{element}: {e}")
                        self.log_to_db("Logs_del_Sistema", ["tipo", "mensaje"], ["ERROR", f"{element}: {e}"])
                        utils.move_files_error(directory, element, file_path)
                        continue

                    for xml_path, name_file, store_id in archivos_generados:
                        try:
                            if utils.wait_for_file_ready(xml_path):
                                sent = utils.send_item_files(xml_path, name_file, store_id, "store")
                                utils.register_xml_log("tiendas", name_file, xml_path,
                                                       "Enviado" if sent else "Pendiente",
                                                       "Envío completado" if sent else "Envío fallido")
                            else:
                                utils.log_interfaces("ERROR", f"Archivo no accesible para envío: {xml_path}")
                                sent = False

                            subdir = "enviadas" if sent else "no_enviadas"
                            dest_dir = os.path.join(os.path.dirname(xml_path), subdir, datetime.now().strftime("%Y%m%d"))
                            os.makedirs(dest_dir, exist_ok=True)
                            if os.path.exists(xml_path):
                                shutil.move(xml_path, os.path.join(dest_dir, name_file))
                        except Exception as envio_err:
                            utils.log_interfaces("ERROR", f"Error moviendo o enviando {name_file}: {envio_err}")

                    if archivos_generados:
                        utils.move_files(directory, element, file_path)


            for element in os.listdir(directory):
                file_path = os.path.join(directory, element)
                if os.path.isdir(file_path):
                    continue

                if element.lower().endswith(".xlsx") and not element.startswith("~$"):
                    archivos_generados = []
                    try:
                        archivos_generados = self._create_files(directory, element, file_path)
                    except Exception as e:
                        utils.log_interfaces("ERROR STORE", f"{element}: {e}")
                        self.log_to_db("Logs_del_Sistema", ["tipo", "mensaje"], ["ERROR", f"{element}: {e}"])
                        utils.move_files_error(directory, element, file_path)
                        continue

                    for xml_path, name_file, store_id in archivos_generados:
                        try:
                            if utils.wait_for_file_ready(xml_path):
                                sent = utils.send_item_files(xml_path, name_file, store_id, "store")
                                utils.register_xml_log("tiendas", name_file, xml_path,
                                                       "Enviado" if sent else "Pendiente",
                                                       "Envío completado" if sent else "Envío fallido")
                            else:
                                utils.log_interfaces("ERROR", f"Archivo no accesible para envío: {xml_path}")
                                sent = False

                            subdir = "enviadas" if sent else "no_enviadas"
                            dest_dir = os.path.join(os.path.dirname(xml_path), subdir, datetime.now().strftime("%Y%m%d"))
                            os.makedirs(dest_dir, exist_ok=True)
                            if os.path.exists(xml_path):
                                shutil.move(xml_path, os.path.join(dest_dir, name_file))
                        except Exception as envio_err:
                            utils.log_interfaces("ERROR", f"Error moviendo o enviando {name_file}: {envio_err}")

                    if archivos_generados:
                        utils.move_files(directory, element, file_path)
            for element in os.listdir(directory):
                file_path = os.path.join(directory, element)

                if os.path.isdir(file_path):
                    continue

                if element.lower().endswith(".xlsx") and not element.startswith("~$"):
                    try:
                        archivos_generados = self._create_files(directory, element, file_path)

                        for xml_path, name_file, store_id in archivos_generados:
                            if utils.wait_for_file_ready(xml_path):
                                sent = utils.send_item_files(xml_path, name_file, store_id, "store")
                                utils.register_xml_log("tiendas", name_file, xml_path,
                                                       "Enviado" if sent else "Pendiente",
                                                       "Envío completado" if sent else "Envío fallido")
                            else:
                                utils.log_interfaces("ERROR", f"Archivo no accesible para envío: {xml_path}")
                                sent = False

                            subdir = "enviadas" if sent else "no_enviadas"
                            dest_dir = os.path.join(os.path.dirname(xml_path), subdir, datetime.now().strftime("%Y%m%d"))
                            os.makedirs(dest_dir, exist_ok=True)
                            if os.path.exists(xml_path):
                                shutil.move(xml_path, os.path.join(dest_dir, name_file))

                        utils.move_files(directory, element, file_path)

                    except Exception as e:
                        utils.log_interfaces("ERROR STORE", f"{element}: {e}")
                        self.log_to_db("Logs_del_Sistema",
                                       ["tipo", "mensaje"],
                                       ["ERROR", f"{element}: {e}"])
                        utils.move_files_error(directory, element, file_path)

    def _create_files(self, directory, element, file_path):
        df = pd.read_excel(file_path, sheet_name=0, dtype=str).fillna("")
        archivos_generados = []

        for _, row in df.iterrows():
            try:
                store_id = row["Tienda"]
                nombre = row["Nombre Tienda"]
                sucursal = row["Nombre Sucursal"]
                ciudad = row["Ciudad"]
                depto = row["Departamento"]
                municipio = row["Municipio"]
                direccion = row["Direccion"]
                telefono = row["Telefono"]
                pais = row["CountryCode"]
                url = row["URL"]
                moneda = row["Moneda"]
                lenguaje = row["Lenguaje"]
                timezone = row["TimeZone"]
                timezone_g = row["TimeZoneGTM"]
                rfc = row["VatRegistrationNumber"]

                now_str = datetime.now().strftime("%Y%m%d%H%M%S")
                external_id = store_id.zfill(10)

                root = utils.generar_store_xml(
                    store_id, nombre, sucursal, ciudad, depto, municipio, direccion,
                    telefono, pais, url, moneda, lenguaje, timezone, timezone_g,
                    rfc, external_id
                )

                name_file = f"BU_{store_id}_{now_str}.xml"
                xml_folder = os.path.join(directory, "xml")
                os.makedirs(xml_folder, exist_ok=True)
                xml_path = os.path.join(xml_folder, name_file)

                with open(xml_path, "w", encoding="utf-8") as fh:
                    fh.write(minidom.parseString(ET.tostring(root)).toprettyxml(indent="  "))

                self.log_to_db("Tiendas",
                               ["codigo", "nombre", "ubicacion", "estado"],
                               [store_id, nombre, direccion, "Activo"])
                self.log_to_db("XML_Generados",
                               ["tipo", "nombre_archivo", "ruta", "estado", "descripcion"],
                               ["Tienda", name_file, xml_path, "Pendiente", "Generado correctamente"])
                self.log_to_db("Logs_del_Sistema",
                               ["tipo", "mensaje"],
                               ["INFO", f"Tienda {store_id} generada correctamente."])

                utils.register_xml_log("tiendas", name_file, xml_path,
                                       "Pendiente", "Generado correctamente")

                archivos_generados.append((xml_path, name_file, store_id))

            except KeyError as ke:
                utils.log_interfaces("WARN", f"{element}: columna faltante {ke}")
            except Exception as e:
                raise

        return archivos_generados
