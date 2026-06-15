"""Interfaz Operadores: Excel → OperatorList (XML GK)."""
import pandas as pd

import utils
from src.classes.base import InterfaceProcessor, ItemGenerado


class Operator(InterfaceProcessor):
    clave_config = "operadores"
    prefijo = "Operator_"
    tipo_xml = "Operator"

    def generar(self, directory, file_path, element):
        df = pd.read_excel(file_path, sheet_name=0, dtype=str).fillna("")
        items = []
        for _, row in df.iterrows():
            operator_id = row["Operator"]
            first_name = row["Nombre"]
            last_name = row["Apellido"]
            root = utils.generar_operator_xml(
                operator_id, first_name, last_name,
                row["Lenguaje"], row["Código Pais"],
                str(int(row["Año"])), str(int(row["Mes "])), str(int(row["Dia"])),
                str(int(row["Tienda"])), row["Role"],
                {"Web": row["PWD Web"], "Mobile": row["PWD POS"], "POS": row["PWD POS"]},
                operator_id.zfill(10),
            )
            items.append(ItemGenerado(
                ident=operator_id, root=root, name_file=self.nombre_archivo(operator_id),
                standalone=False,
                master=("Operadores", ["codigo", "nombre", "ubicacion", "estado"],
                        [operator_id, f"{first_name} {last_name}".strip(), "", "Activo"]),
            ))
        return items
