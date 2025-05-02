import os
import shutil
import sqlite3
import pandas as pd
import xml.etree.ElementTree as ET
from xml.dom import minidom
from datetime import datetime
import utils

class PromotionCategory:
    """Procesa archivos Excel de categorías de promoción y genera XML legibles"""

    def __init__(self, config: dict):
        self.config = config
        self.db_path = os.path.join("db", "LogDatabaseDataGK.db")
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)

    # ------------------------------------------------------------------ BD util
    def log_to_db(self, table: str, fields: list[str], values: list[str]):
        try:
            with sqlite3.connect(self.db_path, timeout=10) as c:
                c.execute(f"INSERT INTO {table} ({','.join(fields)}) VALUES ({','.join('?'*len(values))})", values)
        except sqlite3.IntegrityError:
            if table == "Categorias":
                with sqlite3.connect(self.db_path, timeout=10) as c:
                    c.execute("""UPDATE Categorias
                                  SET descripcion=?, fecha_inicio=?, fecha_fin=?, impresora=?
                                  WHERE codigo=?""", (values[1], values[2], values[3], values[4], values[0]))
        except Exception as e:
            utils.log_interfaces("ERROR DB", f"{table}: {e}")

    # ------------------------------------------------------------------ helpers
    _clean = staticmethod(lambda v: str(v).strip())

    def _sheet(self, path: str, sheet_idx: int, cols: int):
        try:
            df = pd.read_excel(path, sheet_name=sheet_idx, dtype=str).iloc[:, :cols].fillna("")
            return df.apply(lambda c: c.map(self._clean)).values.tolist()
        except Exception as e:
            utils.log_interfaces("ERROR", f"Error leyendo hoja {sheet_idx} de {path}: {e}")
            return []

    # ------------------------------------------------------------------ público
    def read_file_items(self):
        for cfg in self.config.get("promociones_categoria", []):
            directory = cfg["directory"]
            os.makedirs(directory, exist_ok=True)

            excels = [f for f in os.listdir(directory)
                      if f.lower().endswith(".xlsx") and not f.startswith("~$")]

            for excel in excels:
                excel_path = os.path.join(directory, excel)
                try:
                    promos  = self._sheet(excel_path, 0, 10)
                    cats    = self._sheet(excel_path, 1,  2)
                    stores  = self._sheet(excel_path, 2,  2)

                    if not promos or not cats or not stores:
                        utils.log_interfaces("ERROR", f"{excel}: hoja vacía; se omite")
                        self.log_to_db("Logs_del_Sistema", ["tipo", "mensaje"],
                                       ["ERROR", f"Hoja vacía en {excel}"])
                        utils.move_files_error(directory, excel, excel_path)
                        continue

                    xmls = self._create_xmls(directory, promos, cats, stores)

                    for xml_path, name_file, promo_id in xmls:
                        try:
                            if utils.wait_for_file_ready(xml_path):
                                sent = utils.send_item_files(xml_path, name_file, promo_id, "promotion_category")
                                utils.register_xml_log("promotion_category", name_file, xml_path,
                                                       "Enviado" if sent else "Pendiente",
                                                       "Envío completado" if sent else "Envío fallido")
                            else:
                                sent = False
                                utils.log_interfaces("ERROR", f"Archivo ocupado: {xml_path}")

                            subdir = "enviadas" if sent else "no_enviadas"
                            dest = os.path.join(os.path.dirname(xml_path),
                                                subdir,
                                                datetime.now().strftime('%Y%m%d'))
                            os.makedirs(dest, exist_ok=True)
                            if os.path.exists(xml_path):
                                shutil.move(xml_path, os.path.join(dest, name_file))
                        except Exception as mv_err:
                            utils.log_interfaces("ERROR", f"{name_file}: {mv_err}")

                    if xmls:
                        utils.move_files(directory, excel, excel_path)

                except Exception as proc_err:
                    utils.log_interfaces("ERROR", f"{excel}: {proc_err}")
                    self.log_to_db("Logs_del_Sistema", ["tipo", "mensaje"],
                                   ["ERROR", f"{excel}: {proc_err}"])
                    utils.move_files_error(directory, excel, excel_path)

    # ------------------------------------------------------------------ core
    def _create_xmls(self, directory, promos, cats, stores):
        archivos = []
        cat_map = {c[0]: c[1] for c in cats}

        for pr in promos:
            if len(pr) < 9:
                utils.log_interfaces("ERROR", f"Promo incompleta: {pr}")
                continue

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

            # ---- guardar archivo
            now_str = datetime.now().strftime("%Y%m%d%H%M%S%f")[:-3]
            name_file = f"PromoCat_{promo_id}_{now_str}.xml"
            xml_dir   = os.path.join(directory, "xml")
            os.makedirs(xml_dir, exist_ok=True)
            xml_path  = os.path.join(xml_dir, name_file)

            try:
                xml_bytes = minidom.parseString(
                    ET.tostring(root, encoding='utf-8')
                ).toprettyxml(indent="  ", encoding="utf-8")
                pretty = xml_bytes.decode("utf-8").replace(
                    '<?xml version="1.0" encoding="utf-8"?>',
                    '<?xml version="1.0" encoding="utf-8" standalone="yes"?>'
                )
                with open(xml_path, "w", encoding="utf-8") as fh:
                    fh.write(pretty)
            except Exception as e:
                utils.log_interfaces("ERROR", f"Error XML {xml_path}: {e}")
                continue

            # Bd & lista
            self.log_to_db("Categorias",
                           ["codigo","descripcion","fecha_inicio","fecha_fin","impresora"],
                           [promo_id, desc, fi, ff, printer])
            self.log_to_db("XML_Generados",
                           ["tipo","nombre_archivo","ruta","estado","descripcion"],
                           ["Promotion Category", name_file, xml_path, "Pendiente", "Generado correctamente"])
            self.log_to_db("Logs_del_Sistema",
                           ["tipo","mensaje"],
                           ["INFO", f"Promoción categoría {promo_id} generada."])
            utils.register_xml_log("promotion_category", name_file, xml_path,
                                   "Pendiente", "Generado correctamente")
            archivos.append((xml_path, name_file, promo_id))

        return archivos
