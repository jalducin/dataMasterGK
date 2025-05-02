
import os
import shutil
import pandas as pd
import utils
from datetime import datetime
import xml.etree.ElementTree as ET
from xml.dom import minidom
import sqlite3

class Operator:
    def __init__(self, config):
        self.config = config
        self.db_path = os.path.join("db", "LogDatabaseDataGK.db")

    def log_to_db(self, table, fields, values):
        try:
            with sqlite3.connect(self.db_path) as conn:
                placeholders = ','.join(['?'] * len(values))
                sql = f"INSERT INTO {table} ({','.join(fields)}) VALUES ({placeholders})"
                conn.execute(sql, values)
                conn.commit()
        except Exception as e:
            utils.log_interfaces("ERROR DB", f"No se pudo registrar en {table}: {e}")

    def read_file_items(self):
        paths = self.config['operadores']
        for path in paths:
            directory = path['directory']
            os.makedirs(directory, exist_ok=True)
            archivos_generados = []

            for element in os.listdir(directory):
                file_path = os.path.join(directory, element)

                if os.path.isdir(file_path):
                    continue

                if element.lower().endswith('.xlsx') and not element.startswith('~$'):
                    try:
                        archivos_generados += self.create_files(directory, element, file_path)
                        utils.move_files(directory, element, file_path)
                    except Exception as e:
                        utils.log_interfaces("ERROR OPERATOR", f"{element}: {e}")
                        self.log_to_db(
                            "Logs_del_Sistema", ["tipo", "mensaje"],
                            ["ERROR", f"{element}: {e}"]
                        )
                        utils.move_files_error(directory, element, file_path)
                else:
                    if os.path.isfile(file_path):
                        utils.move_files_error(directory, element, file_path)

            # Fase 2: envío agrupado de XMLs
            for xml_path, name_file, operator_id in archivos_generados:
                if utils.wait_for_file_ready(xml_path):
                    sent = utils.send_item_files(xml_path, name_file, operator_id, "operator")
                    utils.register_xml_log("operator", name_file, xml_path,
                                           "Enviado" if sent else "Pendiente",
                                           "OK" if sent else "Fallo al enviar")
                    subdir = 'enviadas' if sent else 'no_enviadas'
                else:
                    utils.log_interfaces("ERROR", f"No se pudo acceder al archivo para envío: {xml_path}")
                    subdir = 'no_enviadas'

                dest_dir = os.path.join(os.path.dirname(xml_path), subdir, datetime.now().strftime('%Y%m%d'))
                os.makedirs(dest_dir, exist_ok=True)
                try:
                    if os.path.exists(xml_path):
                        shutil.move(xml_path, os.path.join(dest_dir, name_file))
                    else:
                        utils.log_interfaces("WARN", f"No se encontró XML para mover: {xml_path}")
                except Exception as move_err:
                    utils.log_interfaces("ERROR", f"No se pudo mover XML {name_file}: {move_err}")

    def create_files(self, directory, element, file_path):
        df = pd.read_excel(file_path, sheet_name=0, dtype=str).fillna('')
        archivos_generados = []
        for _, row in df.iterrows():
            operator_id      = row['Operator']
            first_name       = row['Nombre']
            last_name        = row['Apellido']
            birth_year       = str(int(row['Año']))
            birth_month      = str(int(row['Mes ']))
            birth_day        = str(int(row['Dia']))
            business_unit_id = str(int(row['Tienda']))
            role             = row['Role']
            pwd_web          = row['PWD Web']
            pwd_pos          = row['PWD POS']
            language         = row['Lenguaje']
            country          = row['Código Pais']

            now_str     = datetime.now().strftime('%Y%m%d%H%M%S')
            external_id = operator_id.zfill(10)
            root = utils.generar_operator_xml(
                operator_id, first_name, last_name,
                language, country,
                birth_year, birth_month, birth_day,
                business_unit_id, role,
                {'Web': pwd_web, 'Mobile': pwd_pos, 'POS': pwd_pos},
                external_id
            )

            name_file = f"Operator_{operator_id}_{now_str}.xml"
            xml_folder = os.path.join(directory, "xml")
            os.makedirs(xml_folder, exist_ok=True)
            xml_path = os.path.join(xml_folder, name_file)
            with open(xml_path, "w", encoding="utf-8") as f:
                f.write(minidom.parseString(ET.tostring(root)).toprettyxml(indent="  "))

            self.log_to_db("Operadores", ["codigo", "login"], [operator_id, operator_id])
            self.log_to_db(
                "XML_Generados",
                ["tipo", "nombre_archivo", "ruta", "estado", "descripcion"],
                ["Operator", name_file, xml_path, "Pendiente", "Generado correctamente"]
            )
            self.log_to_db(
                "Logs_del_Sistema",
                ["tipo", "mensaje"],
                ["INFO", f"Operator {operator_id} generado correctamente."]
            )

            utils.register_xml_log("operator", name_file, xml_path, "Pendiente", "Generado correctamente")
            archivos_generados.append((xml_path, name_file, operator_id))

        return archivos_generados
