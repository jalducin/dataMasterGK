import os
import shutil
import xml.etree.ElementTree as ET
from xml.dom import minidom
from datetime import datetime
import pandas as pd
import utils
import sqlite3

class Promotion:
    def __init__(self, config: dict) -> None:
        self.config = config
        self.db_path = os.path.join("db", "LogDatabaseDataGK.db")

    def log_to_db(self, table: str, fields: list[str], values: list[str]):
        try:
            with sqlite3.connect(self.db_path, timeout=10) as conn:
                ph = ",".join(["?"] * len(values))
                conn.execute(f"INSERT INTO {table} ({','.join(fields)}) VALUES ({ph})", values)
        except sqlite3.IntegrityError:
            if table == "Promociones":
                with sqlite3.connect(self.db_path, timeout=10) as conn:
                    conn.execute(
                        "UPDATE Promociones SET descripcion=?, fecha_inicio=?, fecha_fin=?, impresora=? WHERE codigo=?",
                        (values[1], values[2], values[3], values[4], values[0]),
                    )
        except Exception as exc:
            utils.log_interfaces("ERROR DB", f"{table}: {exc}")

    def _clean(self, val: str) -> str:
        return str(val).strip()

    def _sheet(self, file_path: str, idx: int, cols: int):
        df = pd.read_excel(file_path, sheet_name=idx, dtype=str).iloc[:, :cols].fillna("")
        return df.apply(lambda c: c.map(self._clean)).values.tolist()

    def read_file_items(self):
        for path in self.config.get("promociones", []):
            directory = path["directory"]
            os.makedirs(directory, exist_ok=True)

            for element in os.listdir(directory):
                file_path = os.path.join(directory, element)
                if os.path.isdir(file_path):
                    continue

                if element.lower().endswith(".xlsx") and not element.startswith("~$"):
                    archivos_generados = []
                    try:
                        promos = self._sheet(file_path, 0, 10)
                        items  = self._sheet(file_path, 1, 2)
                        stores = self._sheet(file_path, 2, 2)
                        archivos_generados = self._create_xmls(directory, promos, items, stores)
                    except Exception as exc:
                        utils.log_interfaces("ERROR PROMOTION", f"{element}: {exc}")
                        self.log_to_db("Logs_del_Sistema", ["tipo", "mensaje"], ["ERROR", f"{element}: {exc}"])
                        utils.move_files_error(directory, element, file_path)
                        continue

                    for xml_path, name_file, promo_id in archivos_generados:
                        try:
                            if utils.wait_for_file_ready(xml_path):
                                sent = utils.send_item_files(xml_path, name_file, promo_id, "promotion")
                                utils.register_xml_log("promotion", name_file, xml_path,
                                                       "Enviado" if sent else "Pendiente",
                                                       "Envío completado" if sent else "Envío fallido")
                            else:
                                utils.log_interfaces("ERROR", f"Archivo no accesible para envío: {xml_path}")
                                sent = False

                            subdir = "enviadas" if sent else "no_enviadas"
                            dest_dir = os.path.join(os.path.dirname(xml_path), subdir, datetime.now().strftime('%Y%m%d'))
                            os.makedirs(dest_dir, exist_ok=True)
                            if os.path.exists(xml_path):
                                shutil.move(xml_path, os.path.join(dest_dir, name_file))
                        except Exception as envio_err:
                            utils.log_interfaces("ERROR", f"Error moviendo o enviando {name_file}: {envio_err}")

                    if archivos_generados:
                        utils.move_files(directory, element, file_path)

    def _create_xmls(self, directory: str, promos, items, stores):
        archivos_generados = []

        for pr in promos:
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

            now_str = datetime.now().strftime("%Y%m%d%H%M%S%f")[:-3]
            name_file = f"Promo_{promo_id}_{now_str}.xml"
            xml_dir = os.path.join(directory, "xml")
            os.makedirs(xml_dir, exist_ok=True)
            xml_path = os.path.join(xml_dir, name_file)

            with open(xml_path, "w", encoding="utf-8") as fh:
                xml_str = ET.tostring(root, encoding="utf-8")
                pretty_xml = minidom.parseString(xml_str).toprettyxml(indent="  ", encoding="utf-8")
                fh.write(pretty_xml.decode("utf-8").replace(
                    '<?xml version="1.0" encoding="utf-8"?>',
                    '<?xml version="1.0" encoding="utf-8" standalone="yes"?>'))

            self.log_to_db("Promociones", ["codigo", "descripcion", "fecha_inicio", "fecha_fin", "impresora"],
                           [promo_id, desc, fi, ff, printer])
            self.log_to_db("XML_Generados", ["tipo", "nombre_archivo", "ruta", "estado", "descripcion"],
                           ["Promoción", name_file, xml_path, "Pendiente", "Generado correctamente"])
            self.log_to_db("Logs_del_Sistema", ["tipo", "mensaje"], ["INFO", f"Promoción {promo_id} generada."])
            utils.register_xml_log("promotion", name_file, xml_path, "Pendiente", "Generado correctamente")

            archivos_generados.append((xml_path, name_file, promo_id))

        return archivos_generados
