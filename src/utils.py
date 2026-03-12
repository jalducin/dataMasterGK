# utils.py — versión estable completa (sin cortes ni errores)
# ============================================================
#  ✦ Logs por archivo y consola
#  ✦ Movimiento de excels procesados / con error
#  ✦ Envío de XML por FTP o SFTP (configurable)
#  ✦ Registro de errores FTP en BD con hora local
#  ✦ Generadores XML (Store y Operator) de referencia
# ============================================================

import os
import shutil
import ftplib
import logging
import sqlite3
import json
from datetime import datetime
import xml.etree.ElementTree as ET

# ===========================================================
#  LOGGING
# ===========================================================
LOG_BASE = "logs"
os.makedirs(LOG_BASE, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(LOG_BASE, "dataMasterGK.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8",
)

def log_interfaces(tipo: str, mensaje: str):
    day_dir = os.path.join(LOG_BASE, datetime.now().strftime("%Y%m%d"))
    os.makedirs(day_dir, exist_ok=True)
    with open(os.path.join(day_dir, "interfaz.log"), "a", encoding="utf-8") as fh:
        fh.write(f"{datetime.now()} - {tipo} - {mensaje}\n")
    logging.info(f"{tipo} - {mensaje}")

# ===========================================================
#  CONFIG
# ===========================================================

def load_config():
    with open("config.json", encoding="utf-8") as fh:
        return json.load(fh)

# ===========================================================
#  MOVIMIENTO DE EXCELS
# ===========================================================

def move_files(directory: str, element: str, file_path: str):
    ts = datetime.now().strftime("%Y%m%d%H%M%S")
    dest = os.path.join(directory, "excel_procesados", datetime.now().strftime("%Y%m%d"))
    os.makedirs(dest, exist_ok=True)
    if os.path.exists(file_path):
        shutil.move(file_path, os.path.join(dest, f"{ts}_{element}"))
    else:
        log_interfaces("ERROR", f"No se pudo mover archivo, no existe: {file_path}")

def move_files_error(directory: str, element: str, file_path: str):
    ts = datetime.now().strftime("%Y%m%d%H%M%S")
    dest = os.path.join(directory, "excel_no_procesados", datetime.now().strftime("%Y%m%d"))
    os.makedirs(dest, exist_ok=True)
    if os.path.exists(file_path):
        shutil.move(file_path, os.path.join(dest, f"{ts}_{element}"))
    else:
        log_interfaces("ERROR", f"No se pudo mover archivo, no existe: {file_path}")

# ===========================================================
#  ENVÍO DE XML (FTP / SFTP)
# ===========================================================

def send_item_files(xml_path: str, xml_name: str, store_code: str, tipo: str) -> bool:
    """Devuelve *True* si se envió correctamente tu XML.

    Config esperado en config.json → "server".
    """
    cfg = load_config()["server"][0]
    host, user, pwd = cfg["server"], cfg["user"], cfg["pwd"]
    remote_dir      = cfg.get("pathUcon", "/")
    protocol        = cfg.get("protocol", "ftp").lower()
    port            = int(cfg.get("port", 22 if protocol == "sftp" else 21))

    ok = False
    if not os.path.exists(xml_path):
        log_interfaces("ERROR", f"No se encontró el XML a enviar: {xml_path}")
        return False
    if protocol == "sftp":
        try:
            import paramiko
            log_interfaces("INFO FTP", f"[SFTP] Conectando a {host}:{port}")
            t = paramiko.Transport((host, port))
            t.connect(username=user, password=pwd)
            sftp = paramiko.SFTPClient.from_transport(t)
            try:
                sftp.chdir(remote_dir)
            except IOError:
                sftp.mkdir(remote_dir); sftp.chdir(remote_dir)
            sftp.put(xml_path, xml_name)
            sftp.close(); t.close()
            log_interfaces("INFO FTP", f"{xml_name} enviado correctamente a {host}:{remote_dir} (SFTP)")
            ok = True
        except Exception as e:
            log_interfaces("ERROR FTP", f"SFTP falló {xml_name} → {host}:{remote_dir} – {e}")
            _registrar_error_ftp(xml_name, e)
    else:
        try:
            log_interfaces("INFO FTP", f"[FTP] Conectando a {host}:{port}")
            ftp = ftplib.FTP(); ftp.connect(host, port, timeout=30); ftp.login(user, pwd)
            ftp.set_pasv(True); ftp.cwd(remote_dir)
            with open(xml_path, "rb") as fh:
                ftp.storbinary(f"STOR {xml_name}", fh)
            ftp.quit()
            log_interfaces("INFO FTP", f"{xml_name} enviado correctamente a {host}:{remote_dir} (FTP)")
            
            ok = True
        except Exception as e:
            log_interfaces("ERROR FTP", f"FTP falló {xml_name} → {host}:{remote_dir} – {e}")
            _registrar_error_ftp(xml_name, e)
    return ok

def _registrar_error_ftp(xml_name: str, error: Exception):
    try:
        db = sqlite3.connect(os.path.join("db", "LogDatabaseDataGK.db"))
        db.execute(
            "INSERT INTO Logs_del_Sistema(tipo, mensaje, fecha) VALUES (?,?,?)",
            ("ERROR", f"FTP {xml_name}: {error}", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        )
        db.commit(); db.close()
    except Exception as db_e:
        log_interfaces("ERROR DB", f"No se pudo registrar error FTP en BD: {db_e}")
        
# ===========================================================
#  REGISTRO XML_Generados
# ===========================================================

def register_xml_log(tipo, name_file, ruta, estado, descripcion):
    db = sqlite3.connect(os.path.join("db", "LogDatabaseDataGK.db"))
    db.execute(
        "INSERT INTO XML_Generados(tipo,nombre_archivo,ruta,estado,descripcion) VALUES (?,?,?,?,?)",
        (tipo, name_file, ruta, estado, descripcion),
    )
    db.commit(); db.close()

# ===========================================================
#  GENERAR STORE XML
# ===========================================================

def generar_store_xml(
    store_id, nombre, sucursal, ciudad, depto, municipio, direccion,
    telefono, pais, url, moneda, lenguaje, timezone, timezone_gmt,
    rfc, external_id
):
    """Construye el elemento BusinessUnitPackageDO completo."""
    ns = {
        "": "http://www.gk-software.com/gkr/md/business_unit_pkg/1.0.0",
        "business_unit": "http://www.gk-software.com/gkr/md/business_unit/1.0.0",
        "posDepartment": "http://www.gk-software.com/gkr/md/pos_department/1.0.0",
        "contact": "http://www.gk-software.com/gkr/md/contact/1.0.0",
        "importHeader": "http://www.gk-software.com/gkr/common/import_header/1.0.0",
        "xsi": "http://www.w3.org/2001/XMLSchema-instance",
    }
    for p, u in ns.items():
        ET.register_namespace(p, u)

    root = ET.Element(ET.QName(ns[""], "BusinessUnitPackageDO"), {
        ET.QName(ns["xsi"], "schemaLocation"): f"{ns['']} BusinessUnitPackageDO.xsd"
    })

    # Header
    hdr = ET.SubElement(root, "Header")
    hdr_vals = {
        "ClientID": "GLOBAL",
        "Type": "BusinessUnitImport",
        "ChangeType": "MODIFY",
        "ElementsCount": "1",
    }
    for k, v in hdr_vals.items():
        ET.SubElement(hdr, ET.QName(ns["importHeader"], k)).text = v

    bu_elem = ET.SubElement(ET.SubElement(root, "BusinessUnitListDO"), "BusinessUnitElementDO")
    bu_imp  = ET.SubElement(bu_elem, "BusinessUnitImportDO")
    bu_do   = ET.SubElement(bu_imp, ET.QName(ns["business_unit"], "BusinessUnitDO"))

    def _b(tag, text):
        ET.SubElement(bu_do, ET.QName(ns["business_unit"], tag)).text = text

    _b("BusinessUnitID", store_id)
    _b("ExternalBusinessUnitID", external_id)
    _b("VatRegistrationNumber", rfc)
    _b("TimeZoneCode", timezone)
    _b("TimeZoneLongID", timezone_gmt)
    _b("LanguageID", lenguaje)
    _b("Name", nombre)
    _b("SurrogateName", sucursal)
    _b("MainCurrencyID", moneda)
    _b("IncludesSalesTaxFlag", "true")
    ET.SubElement(bu_do, "MainWeighingUnit").text = "UN"
    ET.SubElement(bu_do, ET.QName(ns["business_unit"], "ReceiverLocationList"))
    ET.SubElement(bu_do, ET.QName(ns["business_unit"], "CompanyCodeList"))
    ET.SubElement(bu_do, ET.QName(ns["business_unit"], "BankAccountList"))

    # MHG CA1001‑CA1009
    mhg_list = ET.SubElement(bu_imp, ET.QName(ns["business_unit"], "MerchandiseHierarchyGroupDetailList"))
    for i in range(1, 10):
        detail = ET.SubElement(mhg_list, ET.QName(ns["business_unit"], "MerchandiseHierarchyGroupDetail"))
        ET.SubElement(detail, ET.QName(ns["business_unit"], "MerchandiseHierarchyGroupID")).text = f"CA100{i}"
        ET.SubElement(detail, ET.QName(ns["business_unit"], "PosDepartmentID")).text = "0001"

    # ContactDO
    cdo = ET.SubElement(bu_elem, "ContactDO")
    ET.SubElement(cdo, ET.QName(ns["contact"], "InternetURL")).text = url

    addr_list = ET.SubElement(cdo, ET.QName(ns["contact"], "AddressList"))
    def _addr(purpose, method, extra_null=False):
        a = ET.SubElement(addr_list, ET.QName(ns["contact"], "Address"))
        vals = {
            "ContactPurposeTypeCode": purpose,
            "ContactMethodTypeCode": method,
            "City": ciudad,
            "IsoCountryCode": pais,
            "SubTerritoryName": municipio,
            "TerritoryName": depto,
            "AddressLine1": direccion,
        }
        for k, v in vals.items():
            ET.SubElement(a, ET.QName(ns["contact"], k)).text = v
        if extra_null:
            for n in (3, 4, 5):
                ET.SubElement(a, ET.QName(ns["contact"], f"AddressLine{n}"), {"isNull": "true"})
    _addr("DEFAULT", "WORK")
    _addr("DEFAULT", "CONTACT", extra_null=True)

    tel_list = ET.SubElement(cdo, ET.QName(ns["contact"], "TelephoneList"))
    tel = ET.SubElement(tel_list, ET.QName(ns["contact"], "Telephone"))
    ET.SubElement(tel, ET.QName(ns["contact"], "ContactPurposeTypeCode")).text = "DEFAULT"
    ET.SubElement(tel, ET.QName(ns["contact"], "ContactMethodTypeCode")).text = "WORKTELEPHONE"
    ET.SubElement(tel, ET.QName(ns["contact"], "CompleteTelephoneNumber")).text = telefono

    # PosDepartment
    pd_list = ET.SubElement(bu_elem, "PosDepartmentListDO")
    pd = ET.SubElement(pd_list, "PosDepartmentDO")
    ET.SubElement(pd, ET.QName(ns["posDepartment"], "PosDepartmentID")).text = "0001"
    ET.SubElement(pd, ET.QName(ns["posDepartment"], "Name")).text = "0001"

    return root

# ===========================================================
#  GENERAR OPERATOR XML (compacto, sin cambios)
# ===========================================================

def generar_operator_xml(
    operator_id, first_name, last_name,
    language, country,
    birth_year, birth_month, birth_day,
    business_unit_id, role,
    pwd_dict,
    external_id,
):
    """Genera OperatorList con
    xmlns="http://…/operator/2.1.0"
    xmlns:importDomain="http://…/import_domain/2.4.0"
    y sin atributos duplicados.
    """
    from collections import OrderedDict

    ns = {
        "": "http://www.gk-software.com/storeweaver/master_data/operator/2.1.0",
        "importDomain": "http://www.gk-software.com/storeweaver/master_data/import_domain/2.4.0",
        "xsi": "http://www.w3.org/2001/XMLSchema-instance",
    }

    # registrar en el orden deseado
    ET.register_namespace("importDomain", ns["importDomain"])
    ET.register_namespace("", ns[""])
    ET.register_namespace("xsi", ns["xsi"])

    # atributos ordenados (sin xmlns, ElementTree los añade solo)
    attrs = OrderedDict()
    attrs[ET.QName(ns["xsi"], "schemaLocation")] = (
        f"{ns['']} file:///D:/mappings/NRF2016/05_Operator_Worker/mapping/target/v2_1_0/Operator.xsd"
    )
    attrs["NumberOfOperators"] = "1"
    attrs["ChangeTimestamp"]  = datetime.now().isoformat()

    # raíz con namespace por defecto (QName)
    root = ET.Element(ET.QName(ns[""], "OperatorList"), attrs)

    # ---------- nodo Operator ----------
    op = ET.SubElement(root, ET.QName(ns[""], "Operator"), {"changeType": "MOD"})
    def _o(tag, text):
        ET.SubElement(op, ET.QName(ns[""], tag)).text = text

    _o("OperatorID",      operator_id)
    _o("WorkerID",        operator_id)
    _o("FirstName",       first_name)
    _o("LastName",        last_name)
    _o("LanguageID",      language)
    _o("ISOCountryCode",  country)
    _o("LeftHandedFlag",  "false")
    _o("EMailAddress",    "medipiel@medipiel.com")
    _o("BirthYearNumber",  birth_year)
    _o("BirthMonthNumber", birth_month)
    _o("BirthDayNumber",   birth_day)

    bua  = ET.SubElement(op, ET.QName(ns[""], "BusinessUnitAssignment"))
    buid = ET.SubElement(bua, ET.QName(ns[""], "BusinessUnitIdentification"))
    ET.SubElement(buid, ET.QName(ns[""], "BusinessUnitID")).text = business_unit_id

    buac = ET.SubElement(bua, ET.QName(ns[""], "BusinessUnitAssignmentContent"))
    ET.SubElement(buac, ET.QName(ns[""], "RoleID")).text = role

    for ps, pwd in pwd_dict.items():
        psl = ET.SubElement(buac, ET.QName(ns[""], "PeripheralSystemTypeLogin"))
        ET.SubElement(psl, ET.QName(ns[""], "PeripheralSystemType")).text = ps
        ET.SubElement(psl, ET.QName(ns[""], "LoginName")).text            = operator_id
        ET.SubElement(psl, ET.QName(ns[""], "Password")).text              = pwd

    return root


def obtener_programacion_activa():
    try:
        db_path = os.path.join("db", "LogDatabaseDataGK.db")
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT interface, hora FROM ProgramacionInterfaces WHERE activo = 1")
            return cursor.fetchall()
    except Exception as e:
        log_interfaces("ERROR DB", f"No se pudo obtener programación activa: {e}")
        return []


def registrar_ejecucion(tipo, interfaz, estado, mensaje):
    try:
        db_path = os.path.join("db", "LogDatabaseDataGK.db")
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO Ejecuciones (tipo, interfaz, estado, mensaje) VALUES (?, ?, ?, ?)",
                (tipo, interfaz, estado, mensaje)
            )
            conn.commit()
    except Exception as e:
        log_interfaces("ERROR DB", f"No se pudo registrar ejecución ({interfaz}): {e}")

def wait_for_file_ready(filepath, retries=10, delay=1):
    """
    Espera a que un archivo esté disponible para lectura.
    Retorna True si lo logra dentro de los reintentos, False si no.
    """
    import time
    for _ in range(retries):
        if os.path.exists(filepath):
            try:
                with open(filepath, 'rb'):
                    return True
            except PermissionError:
                time.sleep(delay)
        else:
            time.sleep(delay)
    return False
