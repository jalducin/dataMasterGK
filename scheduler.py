_started = False
import schedule
import time
import threading
import os
from datetime import datetime
from src import utils
from src.classes.store import Store
from src.classes.operator import Operator
from src.classes.promotion import Promotion
from src.classes.promotion_category import PromotionCategory

def ejecutar_si_programado(nombre_interfaz, clase):
    ahora = int(datetime.now().strftime("%H"))
    programadas = utils.obtener_programacion_activa()

    if not programadas:
        msg = "No hay programación activa definida."
        utils.log_interfaces("INFO", msg)
        _log_en_archivo(nombre_interfaz, msg)
        return

    # Mapeo de nombres para comparar con la base de datos
    nombres_sql = {
        "store": "Store",
        "operator": "Operator",
        "promotion": "Promotion",
        "promotion_category": "Promotion Category"
    }
    nombre_en_sql = nombres_sql.get(nombre_interfaz, nombre_interfaz)

    horas = []
    for interfaz, h in programadas:
        if interfaz == nombre_en_sql:
            try:
                horas.append(int(h))
            except:
                continue

    if ahora not in horas:
        msg = f"No es hora programada para {nombre_interfaz}. Ahora: {ahora}, Programadas: {horas}"
        utils.log_interfaces("INFO", msg)
        _log_en_archivo(nombre_interfaz, msg)
        return

    try:
        carpeta = _obtener_directorio(nombre_interfaz)
        if carpeta and any(f.endswith('.xlsx') for f in os.listdir(carpeta)):
            msg = f"Iniciando ejecución programada de {nombre_interfaz} a las {ahora}:00"
            utils.log_interfaces("INFO", msg)
            _log_en_archivo(nombre_interfaz, msg)

            clase(utils.load_config()).read_file_items()
            utils.registrar_ejecucion("programada", nombre_interfaz, "éxito", "Procesado correctamente")
        else:
            msg = f"{nombre_interfaz}: No hay archivos .xlsx para procesar en {carpeta}"
            utils.log_interfaces("INFO", msg)
            _log_en_archivo(nombre_interfaz, msg)
    except Exception as e:
        msg = f"{nombre_interfaz} falló: {e}"
        utils.log_interfaces("ERROR", msg)
        _log_en_archivo(nombre_interfaz, msg)
        utils.registrar_ejecucion("programada", nombre_interfaz, "error", str(e))

def _log_en_archivo(nombre_interfaz, mensaje):
    try:
        config = utils.load_config()
        rutas = {
            "store": config["tiendas"][0]["directory"],
            "operator": config["operadores"][0]["directory"],
            "promotion": config["promociones"][0]["directory"],
            "promotion_category": config["promociones_categoria"][0]["directory"]
        }
        ruta_base = rutas.get(nombre_interfaz, "")
        if not os.path.exists(ruta_base): return

        fecha = datetime.now().strftime("%Y%m%d")
        log_dir = os.path.join(ruta_base, "logs")
        os.makedirs(log_dir, exist_ok=True)

        log_path = os.path.join(log_dir, f"{nombre_interfaz}_{fecha}.log")
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {mensaje}\n")
    except Exception as e:
        utils.log_interfaces("ERROR", f"No se pudo escribir log local para {nombre_interfaz}: {e}")

def _obtener_directorio(nombre_interfaz):
    config = utils.load_config()
    ruta = ""
    if nombre_interfaz == "store":
        ruta = config["tiendas"][0]["directory"]
    elif nombre_interfaz == "operator":
        ruta = config["operadores"][0]["directory"]
    elif nombre_interfaz == "promotion":
        ruta = config["promociones"][0]["directory"]
    elif nombre_interfaz == "promotion_category":
        ruta = config["promociones_categoria"][0]["directory"]
    return ruta if ruta and os.path.exists(ruta) else None

def start():
    global _started
    if _started:
        return
    _started = True
    schedule.clear()
    schedule.every().hour.at(":00").do(lambda: ejecutar_si_programado("store", Store))
    schedule.every().hour.at(":00").do(lambda: ejecutar_si_programado("operator", Operator))
    schedule.every().hour.at(":00").do(lambda: ejecutar_si_programado("promotion", Promotion))
    schedule.every().hour.at(":00").do(lambda: ejecutar_si_programado("promotion_category", PromotionCategory))

    t = threading.Thread(target=_run_schedule, daemon=True)
    t.start()

def _run_schedule():
    while True:
        schedule.run_pending()
        time.sleep(60)