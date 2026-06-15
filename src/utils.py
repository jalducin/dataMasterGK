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
import time
from datetime import datetime
import xml.etree.ElementTree as ET
from xml.dom import minidom

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
#  CONEXIÓN BD (reutilizable)
# ===========================================================
DB_PATH = os.path.join("db", "LogDatabaseDataGK.db")


def conectar_db():
    """Abre una conexión SQLite a la BD canónica para reutilizarla en una corrida."""
    return sqlite3.connect(DB_PATH, timeout=10)

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
    """Envía un único XML abriendo y cerrando una conexión.

    Wrapper de compatibilidad sobre `transport.Transmisor`. Para enviar varios
    XML en una corrida, usar `Transmisor` directamente y reutilizar la conexión.
    Config esperado en config.json → "server".
    """
    from transport import Transmisor  # import perezoso: evita ciclo utils↔transport
    try:
        with Transmisor() as tx:
            return tx.enviar(xml_path, xml_name)
    except Exception as e:
        log_interfaces("ERROR FTP", f"No se pudo abrir conexión para {xml_name} → {e}")
        _registrar_error_ftp(xml_name, e)
        return False

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

# ===========================================================
#  GENERAR PROMOTION / PROMOTION CATEGORY XML
# ===========================================================

def generar_promotion_xml(pr, items, stores):
    """Construye el elemento PromotionImport de una promoción por ítems."""
    promo_id, desc, fi, ff, printer = pr[0], pr[1], pr[2], pr[3], pr[4]
    pct = pr[7] if len(pr) > 7 else "0"

    root = ET.Element("PromotionImport", {
        "ElementsCount": "1",
        "xmlns": "http://www.gk-software.com/masterdata/promotion_v2/1.9.0",
        "xmlns:data-extension-map": "http://www.gk-software.com/schema/core/server/extension-map/map/map-1.0",
        "xmlns:importDomain": "http://www.gk-software.com/masterdata/import_domain_promotion/1.9.0",
    })
    pe = ET.SubElement(root, "PromotionElement", {"ChangeType": "MODIFY"})

    bul = ET.SubElement(pe, "BusinessUnitAssignmentList")
    for st in sorted({s[1] for s in stores if len(s) > 1}):
        bu = ET.SubElement(bul, "BusinessUnitAssignment")
        ET.SubElement(bu, "BusinessUnitID").text = st

    pn = ET.SubElement(pe, "Promotion")
    ET.SubElement(pn, "PromotionID").text = promo_id
    ET.SubElement(pn, "EffectiveDateTime").text = f"{fi}T00:00:00"
    ET.SubElement(pn, "ExpirationDateTime").text = f"{ff}T23:59:59"
    ET.SubElement(pn, "ReceiptPrinterName").text = printer
    ET.SubElement(pn, "Origin").text = "01"
    ET.SubElement(pn, "Description").text = desc

    pc = ET.SubElement(ET.SubElement(pn, "ConditionList"), "PromotionCondition")
    ET.SubElement(pc, "InternalEligibilityID").text = "1"
    ET.SubElement(pc, "TypeCode").text = "ZRKR"
    ET.SubElement(pc, "Sequence").text = pr[8]
    ET.SubElement(pc, "Resolution").text = pr[9]
    ET.SubElement(pc, "NotShowingFlag").text = "false"
    ET.SubElement(pc, "SaleReturnTypeCode").text = "00"
    ET.SubElement(pc, "ExclusiveFlag").text = "false"
    ET.SubElement(pc, "notConsideredInLineItemModeFlag").text = "false"
    ET.SubElement(pc, "RecommendationFlag").text = "false"
    ET.SubElement(pc, "RecommendationContextList")

    el = ET.SubElement(ET.SubElement(pc, "EligibilityList"), "PromotionConditionEligibility")
    for tag in ("InternalEligibilityID", "RootEligibilityID", "ParentEligibilityID"):
        ET.SubElement(el, tag).text = "1"
    ET.SubElement(el, "TypeCode").text = "ITEM"
    item_eligibility = ET.SubElement(el, "ItemPromotionConditionEligibility")
    ilist = ET.SubElement(item_eligibility, "ItemList")
    for it in items:
        if it[0] == promo_id:
            itm = ET.SubElement(ilist, "Item")
            ET.SubElement(itm, "importDomain:ItemID").text = it[1]
            ET.SubElement(itm, "importDomain:UnitOfMeasureCode").text = "_ALL"
    ET.SubElement(item_eligibility, "ThresholdTypeCode").text = "QUT"
    ET.SubElement(item_eligibility, "ThresholdQuantity").text = pr[5] if len(pr) > 5 else "0"
    ET.SubElement(item_eligibility, "LimitQuantity").text = pr[6] if len(pr) > 6 else "0"

    rule = ET.SubElement(pc, "PromotionConditionRule")
    ET.SubElement(rule, "TransactionControlBreakCode").text = "PO"
    ET.SubElement(rule, "StatusCode").text = "AC"
    ET.SubElement(rule, "TypeCode").text = "RB"
    ET.SubElement(rule, "BonusPointsFlag").text = "false"
    ET.SubElement(rule, "RoundingMethodCode").text = "00"
    ET.SubElement(rule, "DecimalPlacesCount").text = "2"
    ET.SubElement(rule, "RoundDestinationValue").text = "1"
    ET.SubElement(rule, "DiscountMethodCode").text = "00"
    ET.SubElement(rule, "ProhibitTransactionRelatedPromotionConditionFlag").text = "false"
    ET.SubElement(rule, "ChooseItemMethod").text = "00"
    ET.SubElement(rule, "NoEffectOnSubsequentPromotionConditionFlag").text = "false"
    ET.SubElement(rule, "CalculationBase").text = "00"
    ET.SubElement(rule, "CouponPrintoutRule").text = "00"
    ET.SubElement(rule, "CouponPrintoutText").text = "<CouponPrintoutText></CouponPrintoutText>"
    ET.SubElement(rule, "ConsiderPreviousPromotionConditionFlag").text = "false"
    ET.SubElement(rule, "CalculationBaseSequence").text = "-2"
    ET.SubElement(rule, "noPreviousMonetaryDiscountAllowedFlag").text = "false"

    rebate = ET.SubElement(rule, "RebatePromotionConditionRule")
    ET.SubElement(rebate, "PriceModificationMethodCode").text = "RP"
    ET.SubElement(rebate, "PriceModificationAmount").text = pct
    ET.SubElement(rebate, "PriceModificationPercent").text = pct
    ET.SubElement(rebate, "NewPriceAmount").text = pct

    return root


def generar_promotion_category_xml(pr, cat_map, stores):
    """Construye el elemento PromotionImport de una promoción por categoría."""
    promo_id, desc, fi, ff, printer = pr[0], pr[1], pr[2], pr[3], pr[4]
    pct, seq, res = pr[7], pr[8], pr[9]

    root = ET.Element("PromotionImport", {
        "ElementsCount": "1",
        "xmlns": "http://www.gk-software.com/masterdata/promotion_v2/1.9.0",
        "xmlns:data-extension-map": "http://www.gk-software.com/schema/core/server/extension-map/map/map-1.0",
        "xmlns:importDomain": "http://www.gk-software.com/masterdata/import_domain_promotion/1.9.0",
    })
    pe = ET.SubElement(root, "PromotionElement", {"ChangeType": "MODIFY"})

    # Tiendas
    bul = ET.SubElement(pe, "BusinessUnitAssignmentList")
    for st in sorted({s[1] for s in stores if len(s) > 1}):
        bu = ET.SubElement(bul, "BusinessUnitAssignment")
        ET.SubElement(bu, "BusinessUnitID").text = st

    # Datos de la promo
    pn = ET.SubElement(pe, "Promotion")
    ET.SubElement(pn, "PromotionID").text = promo_id
    ET.SubElement(pn, "EffectiveDateTime").text  = f"{fi}T00:00:00"
    ET.SubElement(pn, "ExpirationDateTime").text = f"{ff}T23:59:59"
    ET.SubElement(pn, "ReceiptPrinterName").text = printer
    ET.SubElement(pn, "Origin").text = "01"
    ET.SubElement(pn, "Description").text = desc

    # Condición
    pc = ET.SubElement(ET.SubElement(pn, "ConditionList"), "PromotionCondition")
    ET.SubElement(pc, "InternalEligibilityID").text = "1"
    ET.SubElement(pc, "TypeCode").text = "ZRKR"
    ET.SubElement(pc, "Sequence").text = seq
    ET.SubElement(pc, "Resolution").text = res
    ET.SubElement(pc, "NotShowingFlag").text = "false"
    ET.SubElement(pc, "SaleReturnTypeCode").text = "00"
    ET.SubElement(pc, "ExclusiveFlag").text = "false"
    ET.SubElement(pc, "notConsideredInLineItemModeFlag").text = "false"
    ET.SubElement(pc, "RecommendationFlag").text = "false"
    ET.SubElement(pc, "RecommendationContextList")

    # Elegibilidad por categoría
    elig = ET.SubElement(ET.SubElement(pc, "EligibilityList"), "PromotionConditionEligibility")
    for tag in ("InternalEligibilityID", "RootEligibilityID", "ParentEligibilityID"):
        ET.SubElement(elig, tag).text = "1"
    ET.SubElement(elig, "TypeCode").text = "MSTR"
    mhg = ET.SubElement(elig, "MHGPromotionConditionEligibility")
    mlist = ET.SubElement(mhg, "MerchandiseHierarchyGroupList")

    if promo_id in cat_map:
        grp = ET.SubElement(mlist, "MerchandiseHierarchyGroup")
        ET.SubElement(grp, "importDomain:MerchandiseHierarchyGroupID").text = cat_map[promo_id]
        ET.SubElement(grp, "importDomain:MerchandiseHierarchyGroupIDQualifier").text = "MAIN"

    ET.SubElement(mhg, "ThresholdTypeCode").text  = "QUT"
    ET.SubElement(mhg, "ThresholdQuantity").text  = pr[5] if len(pr) > 5 else "0"
    ET.SubElement(mhg, "LimitQuantity").text      = pr[6] if len(pr) > 6 else "0"

    # Regla de descuento
    rule = ET.SubElement(pc, "PromotionConditionRule")
    for tag, val in (
        ("TransactionControlBreakCode", "PO"),
        ("StatusCode", "AC"),
        ("TypeCode", "RB"),
        ("BonusPointsFlag", "false"),
        ("RoundingMethodCode", "00"),
        ("DecimalPlacesCount", "2"),
        ("RoundDestinationValue", "1"),
        ("DiscountMethodCode", "00"),
        ("ProhibitTransactionRelatedPromotionConditionFlag", "false"),
        ("ChooseItemMethod", "00"),
        ("NoEffectOnSubsequentPromotionConditionFlag", "false"),
        ("CalculationBase", "00"),
        ("CouponPrintoutRule", "00"),
        ("CouponPrintoutText", "<CouponPrintoutText></CouponPrintoutText>"),
        ("ConsiderPreviousPromotionConditionFlag", "false"),
        ("CalculationBaseSequence", "-2"),
        ("noPreviousMonetaryDiscountAllowedFlag", "false"),
    ):
        ET.SubElement(rule, tag).text = val

    rebate = ET.SubElement(rule, "RebatePromotionConditionRule")
    for tag in ("PriceModificationAmount", "PriceModificationPercent", "NewPriceAmount"):
        ET.SubElement(rebate, tag).text = pct
    ET.SubElement(rebate, "PriceModificationMethodCode").text = "RP"

    return root

# ===========================================================
#  SERIALIZACIÓN XML
# ===========================================================

def serializar_xml(root, standalone: bool = False) -> str:
    """Serializa un Element a XML indentado.

    - standalone=False: encabezado simple (Tiendas/Operadores).
    - standalone=True: encabezado con standalone="yes" (Promociones/Categorías).
    """
    if standalone:
        xml_bytes = minidom.parseString(
            ET.tostring(root, encoding="utf-8")
        ).toprettyxml(indent="  ", encoding="utf-8")
        return xml_bytes.decode("utf-8").replace(
            '<?xml version="1.0" encoding="utf-8"?>',
            '<?xml version="1.0" encoding="utf-8" standalone="yes"?>',
        )
    return minidom.parseString(ET.tostring(root)).toprettyxml(indent="  ")


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
