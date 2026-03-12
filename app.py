from flask import Flask, render_template, request, jsonify, send_file, Response, make_response
import sqlite3
import os
import json
import io
from io import BytesIO
import pandas as pd
import sys

# Ajuste importante para que src sea visible
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from interface_runner import run_single_interface
from log_database import crear_tablas_si_no_existen
from utils import load_config

app = Flask(__name__)
DB_PATH = os.path.join("db", "LogDatabaseDataGK.db")

@app.route('/')
def index():
    config = load_config()
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("""
            SELECT interface, hora FROM ProgramacionInterfaces
            WHERE fecha_guardado = (SELECT MAX(fecha_guardado) FROM ProgramacionInterfaces)
        """)
        data = c.fetchall()
    interfaces = list({x[0] for x in data})
    horas = list({x[1] for x in data})
    return render_template("index.html", config=config, interfaces=interfaces, horas=horas)

@app.route('/guardar_programacion', methods=['POST'])
def guardar_programacion():
    interfaces = request.form.getlist("interfaces[]")
    horas = request.form.getlist("horas[]")
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("DELETE FROM ProgramacionInterfaces")
        for iface in interfaces:
            for hora in horas:
                c.execute("INSERT INTO ProgramacionInterfaces(interface, hora) VALUES (?, ?)", (iface, hora))
        conn.commit()
    return ('', 204)

@app.route('/guardar_configuracion', methods=['POST'])
def guardar_configuracion():
    try:
        config = {
            "operadores": [{"directory": request.form.get("Operadores")}],
            "tiendas": [{"directory": request.form.get("Tiendas")}],
            "promociones": [{"directory": request.form.get("Promociones")}],
            "promociones_categoria": [{"directory": request.form.get("PromocionesCat")}],
            "server": [{
                "server": request.form.get("server"),
                "user": request.form.get("user"),
                "pwd": request.form.get("pwd"),
                "pathUcon": request.form.get("pathUcon"),
                "protocol": "sftp",
                "port": 22,
            }]
        }
        with open("config.json", "w", encoding="utf-8") as f:
            json.dump(config, f, indent=4)
        return "✅ Configuración guardada correctamente."
    except Exception as e:
        return f"❌ Error al guardar configuración: {e}"

@app.route('/ultima_programacion')
def ultima_programacion():
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("SELECT interface, hora FROM ProgramacionInterfaces ORDER BY fecha_guardado DESC")
        rows = c.fetchall()
        interfaces = list({r[0] for r in rows})
        horas = list({r[1] for r in rows})
        return jsonify({'interfaces': interfaces, 'horas': horas})

@app.route('/filtrar_fecha')
def filtrar_fecha():
    tipo = request.args.get('tipo')
    fecha = request.args.get('fecha')
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            if tipo == 'Logs_del_Sistema':
                cursor.execute("SELECT rowid, tipo, fecha, mensaje FROM Logs_del_Sistema WHERE DATE(fecha) = ? ORDER BY fecha DESC", (fecha,))
                headers = ['ID', 'Tipo', 'Fecha', 'Mensaje']
            elif tipo == 'XML_Generados':
                cursor.execute("SELECT rowid, tipo, nombre_archivo, ruta, estado, descripcion AS accion, fecha FROM XML_Generados WHERE DATE(fecha) = ? ORDER BY fecha DESC", (fecha,))
                headers = ['ID', 'Tipo', 'Nombre Archivo', 'Ruta', 'Estado', 'Accion', 'Fecha']
            else:
                cursor.execute(f"SELECT * FROM {tipo} WHERE DATE(fecha) = ? ORDER BY fecha DESC", (fecha,))
                headers = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
        return jsonify(rows)
    except Exception as e:
        print(f"Error: {e}")
        return jsonify([])

@app.route('/ejecutar_interface', methods=['POST'])
def ejecutar_interface():
    key = request.form.get("interface")
    mapping = {
        "operator": "Operator",
        "promotion": "Promotion",
        "promotion_category": "Promotion Category",
        "store": "Store"
    }
    interfaz = mapping.get(key, key)
    config = load_config()
    try:
        run_single_interface(config, interfaz)
        return f"✅ Se ejecutó {key}"
    except Exception as e:
        return f"❌ Error al ejecutar {key}: {e}"

@app.route('/ejecutar_stream')
def ejecutar_stream():
    key = request.args.get("interface")
    mapping = {
        "operator": "Operator",
        "promotion": "Promotion",
        "promotion_category": "Promotion Category",
        "store": "Store"
    }
    interfaz = mapping.get(key, key)

    def generate():
        buffer = io.StringIO()
        sys.stdout = buffer
        try:
            config = load_config()
            run_single_interface(config, interfaz)
        except Exception as e:
            yield f"data: ❌ Error: {e}\n\n"
        finally:
            sys.stdout = sys.__stdout__

        buffer.seek(0)
        for line in buffer:
            if line.strip():
                yield f"data: {line.strip()}\n\n"
        yield f"data: ✅ Finalizado {interfaz}\n\n"

    return Response(generate(), mimetype='text/event-stream')

@app.route('/cargar_excel', methods=['POST'])
def cargar_excel():
    if 'file' not in request.files:
        return 'No se envió archivo', 400

    files = request.files.getlist('file')
    tipo = request.form.get('tipo')

    if not files or len(files) == 0:
        return 'Archivos no seleccionados', 400

    config = load_config()
    rutas = {
        "operadores": config['operadores'][0]['directory'],
        "tiendas": config['tiendas'][0]['directory'],
        "promociones": config['promociones'][0]['directory'],
        "promociones_categoria": config['promociones_categoria'][0]['directory'],
    }

    ruta_destino = rutas.get(tipo)
    if not ruta_destino:
        return f"Tipo inválido: {tipo}", 400

    os.makedirs(ruta_destino, exist_ok=True)

    for file in files:
        file.save(os.path.join(ruta_destino, file.filename))

    return f"✅ {len(files)} archivos cargados correctamente en {tipo}."

@app.route('/descargar_csv')
def descargar_csv():
    tipo  = request.args.get('tipo')
    fecha = request.args.get('fecha')
    if not fecha:
        return "Fecha requerida", 400

    # Conectar y obtener datos según el tipo
    conn   = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    if tipo == 'Logs_del_Sistema':
        cursor.execute(
            "SELECT rowid, tipo, fecha, mensaje "
            "FROM Logs_del_Sistema "
            "WHERE DATE(fecha) = ? ORDER BY fecha DESC",
            (fecha,)
        )
        headers = ['ID', 'Tipo', 'Fecha', 'Mensaje']
    elif tipo == 'XML_Generados':
        cursor.execute(
            "SELECT rowid, tipo, nombre_archivo, ruta, estado, descripcion AS accion, fecha "
            "FROM XML_Generados "
            "WHERE DATE(fecha) = ? ORDER BY fecha DESC",
            (fecha,)
        )
        headers = ['ID', 'Tipo', 'Nombre Archivo', 'Ruta', 'Estado', 'Accion', 'Fecha']
    else:
        cursor.execute(
            f"SELECT * FROM {tipo} WHERE DATE(fecha) = ? ORDER BY fecha DESC",
            (fecha,)
        )
        headers = [col[0] for col in cursor.description]

    rows = cursor.fetchall()
    conn.close()

    # DataFrame y Excel en memoria
    df = pd.DataFrame(rows, columns=headers)
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Datos')
    output.seek(0)

    # Response con attachment .xlsx
    resp = make_response(output.read())
    resp.headers["Content-Disposition"] = f"attachment; filename={tipo}_{fecha}.xlsx"
    resp.headers["Content-Type"]        = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    return resp

# Iniciar programador
import scheduler

def iniciar_scheduler():
    try:
        scheduler.start()
    except Exception as e:
        print(f"❌ Error al iniciar scheduler: {e}")


@app.route("/logs_recientes")
def logs_recientes():
    try:
        log_path = "logs/dataMasterGK.log"
        if not os.path.exists(log_path):
            log_path = "dataMasterGK.log"  # si está directo en la raíz

        if not os.path.exists(log_path):
            return jsonify(["No existe archivo de log."]), 404

        with open(log_path, encoding="utf-8") as f:
            lineas = f.readlines()[-1000:]  # Últimas 1000 líneas
        return jsonify([linea.strip() for linea in lineas])
    except Exception as e:
        return jsonify(["Error al leer el archivo log: " + str(e)]), 500

@app.route("/archivos_excel")
def archivos_excel():
    try:
        with open("config.json", encoding="utf-8") as f:
            config = json.load(f)

        paths = []
        for key in ["operadores", "tiendas", "promociones", "promociones_categoria"]:
            ruta = config.get(key, [{}])[0].get("directory")
            if ruta and os.path.exists(ruta):
                archivos = [f for f in os.listdir(ruta) if f.endswith(".xlsx")]
                paths.extend(archivos)

        return jsonify(sorted(paths, reverse=True))
    except Exception as e:
        return jsonify(["Error al listar archivos Excel: " + str(e)]), 500

if __name__ == '__main__':
    crear_tablas_si_no_existen()
    iniciar_scheduler()
    app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)