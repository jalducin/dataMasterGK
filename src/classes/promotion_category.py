"""Interfaz Promociones por Categoría: Excel (3 hojas) → PromotionImport por categoría."""
import utils
from src.classes.base import InterfaceProcessor, ItemGenerado


class PromotionCategory(InterfaceProcessor):
    clave_config = "promociones_categoria"
    prefijo = "PromoCat_"
    tipo_xml = "Promotion Category"
    usa_microsegundos = True
    master_upsert = True

    def generar(self, directory, file_path, element):
        promos = self.leer_hoja(file_path, 0, 10)
        cats = self.leer_hoja(file_path, 1, 2)
        stores = self.leer_hoja(file_path, 2, 2)

        if not promos or not cats or not stores:
            raise ValueError("hoja vacía; se omite el archivo")

        cat_map = {c[0]: c[1] for c in cats}
        items = []
        for pr in promos:
            if len(pr) < 9:
                utils.log_interfaces("ERROR", f"Promo incompleta: {pr}")
                continue
            promo_id, desc, fi, ff, printer = pr[0], pr[1], pr[2], pr[3], pr[4]
            root = utils.generar_promotion_category_xml(pr, cat_map, stores)
            items.append(ItemGenerado(
                ident=promo_id, root=root, name_file=self.nombre_archivo(promo_id),
                standalone=True,
                master=("Categorias", ["codigo", "descripcion", "fecha_inicio", "fecha_fin", "impresora"],
                        [promo_id, desc, fi, ff, printer]),
            ))
        return items
