// funciones.js actualizado

document.addEventListener("DOMContentLoaded", () => {
  const now = new Date();
  const pad = n => String(n).padStart(2, '0');
  const hoy = `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())}`;
  document.getElementById("fecha").value = hoy;
  document.getElementById("tipo").value = "Logs_del_Sistema";
  filtrar();
  cargarUltimaProgramacion();

  $('#interfaces').multipleSelect({ placeholder: "Selecciona interfaces", filter: true });
  $('#horas').multipleSelect({ placeholder: "Selecciona horas", filter: true });
});

function mostrarSeccion(seccionId) {
  document.querySelectorAll(".seccion").forEach(seccion => seccion.classList.add("hidden"));
  document.getElementById(seccionId).classList.remove("hidden");
}

$("#form-cargar").submit(function (e) {
  e.preventDefault();
  var formData = new FormData(this);
  $.ajax({
    url: "/cargar_excel",
    type: "POST",
    data: formData,
    processData: false,
    contentType: false,
    success: function (response) {
      mostrarAlerta("✅ " + response, "green");
      $("#form-cargar")[0].reset();
    },
    error: function (xhr) {
      mostrarAlerta("❌ Error al cargar: " + xhr.responseText, "red");
    }
  });
});

function mostrarAlerta(mensaje, color) {
  const alerta = document.getElementById("alerta") || crearAlerta();
  alerta.className = `p-4 rounded text-center font-bold ${color === "green" ? "bg-green-600" : "bg-red-600"}`;
  alerta.textContent = mensaje;
  alerta.style.display = "block";
  setTimeout(() => alerta.style.display = "none", 4000);
}

function crearAlerta() {
  const div = document.createElement("div");
  div.id = "alerta";
  document.body.appendChild(div);
  return div;
}

async function filtrar() {
  const tipo = document.getElementById('tipo')?.value;
  const fecha = document.getElementById('fecha')?.value;
  if (!tipo || !fecha) return;

  try {
    const response = await fetch(`/filtrar_fecha?tipo=${tipo}&fecha=${fecha}`);
    const datos = await response.json();

    document.getElementById('titulo_seccion').textContent = tipo.replace(/_/g, ' ');

    const thead = document.getElementById('thead_dinamico');
    thead.innerHTML = '';
    let headers = [];

    if (tipo === 'Logs_del_Sistema') {
      headers = ['Id', 'Tipo', 'Fecha', 'Mensaje'];
    } else if (tipo === 'XML_Generados') {
      headers = ['Id', 'Tipo', 'Nombre Archivo', 'Ruta', 'Estado', 'Accion', 'Fecha'];
    }

    const trHead = document.createElement('tr');
    headers.forEach(h => {
      const th = document.createElement('th');
      th.classList.add('px-3', 'py-2', 'border', 'whitespace-nowrap');
      th.textContent = h;
      trHead.appendChild(th);
    });
    thead.appendChild(trHead);

    const tbody = document.getElementById('tbody_dinamico');
    tbody.innerHTML = '';
    datos.forEach((row, idx) => {
      const tr = document.createElement('tr');
      tr.classList.add(idx % 2 === 0 ? 'bg-white' : 'bg-gray-50');
      row.forEach(cell => {
        const td = document.createElement('td');
        td.classList.add('px-3', 'py-1', 'border', 'whitespace-nowrap');
        td.textContent = cell;
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
    });
  } catch (error) {
    console.error("❌ Error al filtrar datos:", error);
  }
}

async function cargarUltimaProgramacion() {
  try {
    const response = await fetch('/ultima_programacion');
    const data = await response.json();
    document.getElementById('interfaces_programados').textContent = data.interfaces.join(', ') || 'No configurado';
    document.getElementById('horas_programadas').textContent = data.horas.join(', ') || 'No configurado';
  } catch (error) {
    console.warn("⚠️ Error al cargar la última programación:", error);
  }
}

document.getElementById("formConfiguracion")?.addEventListener("submit", async function (e) {
  e.preventDefault();
  const form = e.target;
  const formData = new FormData(form);

  formData.append("protocol", document.getElementById("protocol")?.value || "sftp");
  formData.append("port", document.getElementById("port")?.value || "22");

  try {
    const res = await fetch("/guardar_configuracion", { method: "POST", body: formData });
    const text = await res.text();
    alert(text.includes("✅") ? text : "❌ Error al guardar: " + text);
  } catch (err) {
    console.error(err);
    alert("❌ Error en la red.");
  }
});

document.getElementById("formProgramacion")?.addEventListener("submit", async function (e) {
  e.preventDefault();
  const interfaces = $('#interfaces').multipleSelect('getSelects');
  const horas = $('#horas').multipleSelect('getSelects');

  if (interfaces.length === 0 || horas.length === 0) {
    alert("Selecciona al menos una interfaz y una hora.");
    return;
  }

  const formData = new URLSearchParams();
  interfaces.forEach(i => formData.append("interfaces[]", i));
  horas.forEach(h => formData.append("horas[]", h));

  try {
    const res = await fetch("/guardar_programacion", {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: formData.toString()
    });
    if (res.ok || res.status === 204) {
      alert("✅ Programación guardada correctamente.");
      cargarUltimaProgramacion();
    } else {
      alert("❌ Hubo un error al guardar.");
    }
  } catch (error) {
    console.error("❌ Error de red:", error);
    alert("❌ Error de red.");
  }
});

function ejecutar(nombre) {
  const consola = document.getElementById("consolaManual");
  const progressBar = document.getElementById("progressBar");
  consola.innerHTML = "";
  progressBar.style.width = "0%";
  let progreso = 0;
  let finalizadoMostrado = false;

  const source = new EventSource(`/ejecutar_stream?interface=${encodeURIComponent(nombre)}`);

  source.onmessage = function (event) {
    const linea = event.data.trim();

    if (!finalizadoMostrado && linea.toLowerCase().includes("finalizado")) {
      finalizadoMostrado = true;
      const badge = document.createElement("div");
      badge.textContent = `✅ ${linea.replace(/\\n/g, "").replace("Finalizado", "Finalizado")}`;
      badge.className = "bg-green-100 text-green-800 px-3 py-1 rounded mt-2 inline-block font-semibold";
      consola.appendChild(badge);
      consola.scrollTop = consola.scrollHeight;
      progressBar.style.width = "100%";
      source.close();
      return;
    }

    const div = document.createElement("div");
    div.textContent = linea;
    consola.appendChild(div);
    consola.scrollTop = consola.scrollHeight;

    progreso = Math.min(progreso + 5, 100);
    progressBar.style.width = progreso + "%";
  };

  source.onerror = function () {
    source.close();
    progressBar.style.width = "100%";
  };
}

document.getElementById("btnDescargarCSV")?.addEventListener("click", () => {
  const tipo = document.getElementById("tipo").value;
  const fecha = document.getElementById("fecha").value;
  if (!fecha) {
    alert("⚠️ Por favor selecciona una fecha.");
    return;
  }
  const url = `/descargar_csv?tipo=${encodeURIComponent(tipo)}&fecha=${encodeURIComponent(fecha)}`;
  window.open(url, "_blank");
});


let intervaloConsolaAuto = null;

function actualizarConsolaAuto() {
  fetch("/logs_recientes")
    .then(res => res.json())
    .then(logs => {
      const consola = document.getElementById("consola-auto");
      consola.innerHTML = "";
      logs.forEach(linea => {
        const div = document.createElement("div");
        div.textContent = linea;
        consola.appendChild(div);
      });
      consola.scrollTop = consola.scrollHeight;
    })
    .catch(error => console.warn("Error al cargar logs del sistema:", error));
}

setInterval(actualizarConsolaAuto, 10000);
actualizarConsolaAuto();

let intervaloConsolaLogs= null;

function actualizarConsolaLogs() {
  fetch("/logs_recientes")
    .then(res => res.json())
    .then(logs => {
      const consola = document.getElementById("consola-logs");
      consola.innerHTML = "";
      logs.forEach(linea => {
        const div = document.createElement("div");
        div.textContent = linea;
        consola.appendChild(div);
      });
      consola.scrollTop = consola.scrollHeight;
    })
    .catch(error => console.warn("Error al cargar logs del sistema:", error));
}

setInterval(actualizarConsolaLogs, 10000);
actualizarConsolaLogs();

// Listar archivos Excel cargados en tiempo real
function cargarListaArchivosExcel() {
  fetch("/archivos_excel")
    .then(res => res.json())
    .then(archivos => {
      const lista = document.getElementById("lista-archivos");
      lista.innerHTML = "";
      if (archivos.length === 0) {
        lista.innerHTML = "<p class='text-sm italic text-gray-500'>No hay archivos cargados.</p>";
      } else {
        archivos.forEach(archivo => {
          const item = document.createElement("div");
          item.className = "text-sm text-blue-900 border-b py-1";
          item.textContent = archivo;
          lista.appendChild(item);
        });
      }
    })
    .catch(error => console.warn("Error al listar archivos Excel:", error));
}

setInterval(cargarListaArchivosExcel, 8000);
cargarListaArchivosExcel();

