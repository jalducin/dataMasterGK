import sys, os
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "src")))

# interface_runner.py
from src.classes.operator import Operator
from src.classes.promotion import Promotion
from src.classes.promotion_category import PromotionCategory
from src.classes.store import Store

def run_single_interface(config, name):
    if name == "Operator":
        Operator(config).read_file_items()
        import utils
        try:
            Operator(config).read_file_items()
            utils.registrar_ejecucion('manual', 'operator', 'éxito', 'Procesado correctamente')
        except Exception as e:
            utils.log_interfaces('ERROR', f'Operator falló: {e}')
            utils.registrar_ejecucion('manual', 'operator', 'error', str(e))
    elif name == "Store":
        Store(config).read_file_items()
        import utils
        try:
            Store(config).read_file_items()
            utils.registrar_ejecucion('manual', 'store', 'éxito', 'Procesado correctamente')
        except Exception as e:
            utils.log_interfaces('ERROR', f'Store falló: {e}')
            utils.registrar_ejecucion('manual', 'store', 'error', str(e))
    elif name == "Promotion":
        Promotion(config).read_file_items()
        import utils
        try:
            Promotion(config).read_file_items()
            utils.registrar_ejecucion('manual', 'promotion', 'éxito', 'Procesado correctamente')
        except Exception as e:
            utils.log_interfaces('ERROR', f'Promotion falló: {e}')
            utils.registrar_ejecucion('manual', 'promotion', 'error', str(e))
    elif name == "Promotion Category":
        PromotionCategory(config).read_file_items()
        import utils
        try:
            PromotionCategory(config).read_file_items()
            utils.registrar_ejecucion('manual', 'promotion_category', 'éxito', 'Procesado correctamente')
        except Exception as e:
            utils.log_interfaces('ERROR', f'PromotionCategory falló: {e}')
            utils.registrar_ejecucion('manual', 'promotion_category', 'error', str(e))