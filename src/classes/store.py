"""Interfaz Tiendas: Excel → BusinessUnitPackageDO (XML GK)."""
import pandas as pd

import utils
from src.classes.base import InterfaceProcessor, ItemGenerado


class Store(InterfaceProcessor):
    clave_config = "tiendas"
    prefijo = "BU_"
    tipo_xml = "Tienda"

    def generar(self, directory, file_path, element):
        df = pd.read_excel(file_path, sheet_name=0, dtype=str).fillna("")
        items = []
        for _, row in df.iterrows():
            try:
                store_id = row["Tienda"]
                nombre = row["Nombre Tienda"]
                direccion = row["Direccion"]
                external_id = store_id.zfill(10)
                root = utils.generar_store_xml(
                    store_id, nombre, row["Nombre Sucursal"], row["Ciudad"],
                    row["Departamento"], row["Municipio"], direccion, row["Telefono"],
                    row["CountryCode"], row["URL"], row["Moneda"], row["Lenguaje"],
                    row["TimeZone"], row["TimeZoneGTM"], row["VatRegistrationNumber"],
                    external_id,
                )
                items.append(ItemGenerado(
                    ident=store_id, root=root, name_file=self.nombre_archivo(store_id),
                    standalone=False,
                    master=("Tiendas", ["codigo", "nombre", "ubicacion", "estado"],
                            [store_id, nombre, direccion, "Activo"]),
                ))
            except KeyError as ke:
                utils.log_interfaces("WARN", f"{element}: columna faltante {ke}")
        return items
