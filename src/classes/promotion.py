"""Interfaz Promociones: Excel (3 hojas) → PromotionImport por ítems (XML GK)."""
import utils
from src.classes.base import InterfaceProcessor, ItemGenerado


class Promotion(InterfaceProcessor):
    clave_config = "promociones"
    prefijo = "Promo_"
    tipo_xml = "Promoción"
    usa_microsegundos = True
    master_upsert = True

    def generar(self, directory, file_path, element):
        promos = self.leer_hoja(file_path, 0, 10)
        items_sheet = self.leer_hoja(file_path, 1, 2)
        stores = self.leer_hoja(file_path, 2, 2)

        items = []
        for pr in promos:
            promo_id, desc, fi, ff, printer = pr[0], pr[1], pr[2], pr[3], pr[4]
            root = utils.generar_promotion_xml(pr, items_sheet, stores)
            items.append(ItemGenerado(
                ident=promo_id, root=root, name_file=self.nombre_archivo(promo_id),
                standalone=True,
                master=("Promociones", ["codigo", "descripcion", "fecha_inicio", "fecha_fin", "impresora"],
                        [promo_id, desc, fi, ff, printer]),
            ))
        return items
