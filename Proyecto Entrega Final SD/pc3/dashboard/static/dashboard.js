// dashboard.js — cliente WebSocket del dashboard de tráfico urbano

(function () {
  "use strict";

  const socket = io();
  const $stream    = document.getElementById("stream");
  const $accLog    = document.getElementById("acciones-log");
  const $hbLed     = document.getElementById("hb-led");
  const $hbText    = document.getElementById("hb-text");
  const MAX_STREAM = 30;

  // ─── Métricas ─────────────────────────────────────────────
  socket.on("metricas", (m) => {
    document.getElementById("m-principal").textContent    = (m.n_principal   || 0).toLocaleString();
    document.getElementById("m-replica").textContent      = (m.n_replica     || 0).toLocaleString();
    document.getElementById("m-congestiones").textContent = (m.congestiones  || 0).toLocaleString();
    document.getElementById("m-prioridades").textContent  = (m.prioridades   || 0).toLocaleString();
  });

  // ─── Heartbeat ────────────────────────────────────────────
  socket.on("heartbeat", (h) => {
    if (h.pc3_activo) {
      $hbLed.classList.add("on");  $hbLed.classList.remove("off");
      $hbText.textContent = "PC3 activo";
    } else {
      $hbLed.classList.add("off"); $hbLed.classList.remove("on");
      $hbText.textContent = "PC3 NO responde";
    }
  });

  // ─── Actualiza la luz del semáforo según el estado ────────
  function aplicarSemaforo(el, estado) {
    const sem = el.querySelector(".semaforo");
    if (!sem) return;
    // Verde en NORMAL/MODERADO/PRIORIDAD; rojo en CONGESTION y ROJO_MANUAL.
    const verde = (estado !== "CONGESTION" && estado !== "ROJO_MANUAL");
    sem.classList.toggle("verde", verde);
    sem.classList.toggle("rojo", !verde);
  }

  // ─── Estado consolidado del sistema ───────────────────────
  socket.on("estado_sistema", (intersecciones) => {
    Object.entries(intersecciones).forEach(([id, info]) => {
      const el = document.getElementById(id);
      if (!el) return;
      const estado = info.estado || "DESCONOCIDO";
      el.dataset.estado = estado;
      el.querySelector(".q-val").textContent  = formatNum(info.Q);
      el.querySelector(".vp-val").textContent = formatNum(info.Vp);
      el.querySelector(".cv-val").textContent = formatNum(info.Cv);
      aplicarSemaforo(el, estado);
    });
  });

  // ─── Eventos de sensor (en bruto desde el broker) ─────────
  socket.on("evento_sensor", (e) => {
    const el = document.getElementById(e.interseccion);
    if (el) {
      el.classList.add("flash");
      setTimeout(() => el.classList.remove("flash"), 250);
    }
    addStreamLine(e);
  });

  // ─── Acciones de control disparadas desde el dashboard ───
  socket.on("evento_semaforo", (a) => {
    const li = document.createElement("li");
    li.className = a.ok ? "ok" : "fail";
    const ts = new Date(a.ts).toLocaleTimeString();
    if (a.accion === "AMBULANCIA") {
      li.innerHTML = `<span>${ts}</span><span>🚑 PRIORIDAD activada en ${a.vias.join(", ")}</span>`;
      // Marcar visualmente las intersecciones priorizadas
      a.vias.forEach((via) => {
        const el = document.getElementById(via);
        if (el) {
          el.dataset.estado = "PRIORIDAD";
          aplicarSemaforo(el, "PRIORIDAD");
          // El estado lo sostiene la analítica por 60s; el polling de
          // estado_sistema lo refrescará. No forzamos reset aquí.
        }
      });
    } else if (a.accion === "CAMBIO_FORZADO") {
      li.innerHTML = `<span>${ts}</span><span>🚦 ${a.interseccion} → ${a.estado}</span>`;
      const el = document.getElementById(a.interseccion);
      if (el) {
        const st = (String(a.estado).toUpperCase() === "ROJO") ? "ROJO_MANUAL" : "PRIORIDAD";
        el.dataset.estado = st;
        aplicarSemaforo(el, st);
      }
    }
    $accLog.prepend(li);
    while ($accLog.children.length > 12) $accLog.lastChild.remove();
  });

  // ─── Selección de ruta de ambulancia en el mapa (línea recta) ─
  let modoRuta = false;
  let rutaSel = [];
  const $ciudad = document.querySelector(".ciudad");
  const idxId = (id) => {
    const m = id.match(/INT-([A-E])(\d)/);
    return { f: m[1].charCodeAt(0) - 65, c: +m[2] - 1 };
  };
  const mkId = (f, c) => `INT-${String.fromCharCode(65 + f)}${c + 1}`;

  function limpiarRutaClases() {
    document.querySelectorAll(".interseccion").forEach((cel) => {
      cel.classList.remove("ruta", "ruta-origen");
      const o = cel.querySelector(".ruta-orden");
      if (o) o.remove();
    });
  }
  function pintarRuta() {
    limpiarRutaClases();
    rutaSel.forEach((id, i) => {
      const cel = document.getElementById(id);
      if (!cel) return;
      cel.classList.add(i === 0 ? "ruta-origen" : "ruta");
      let o = cel.querySelector(".ruta-orden");
      if (!o) { o = document.createElement("span"); o.className = "ruta-orden"; cel.appendChild(o); }
      o.textContent = i + 1;
    });
    document.getElementById("amb-vias").value = rutaSel.join(", ");
  }
  function construirLinea(a, b) {
    const A = idxId(a), B = idxId(b);
    if (A.f === B.f) {
      const paso = (A.c <= B.c) ? 1 : -1;
      const arr = [];
      for (let c = A.c; c !== B.c + paso; c += paso) arr.push(mkId(A.f, c));
      return arr;
    }
    if (A.c === B.c) {
      const paso = (A.f <= B.f) ? 1 : -1;
      const arr = [];
      for (let f = A.f; f !== B.f + paso; f += paso) arr.push(mkId(f, A.c));
      return arr;
    }
    return null;
  }
  function clickRuta(id) {
    if (!modoRuta) return;
    if (rutaSel.length === 0) { rutaSel = [id]; pintarRuta(); return; }
    const origen = rutaSel[0];
    if (id === origen) { rutaSel = []; limpiarRutaClases(); return; }
    const linea = construirLinea(origen, id);
    if (!linea) {
      pingOut("Ruta inválida: debe ser en línea recta (misma fila o columna)");
      return;
    }
    rutaSel = linea;
    pintarRuta();
  }
  document.querySelectorAll(".interseccion").forEach((cel) => {
    cel.addEventListener("click", () => clickRuta(cel.dataset.id || cel.id));
  });
  document.getElementById("btn-mapa").addEventListener("click", (e) => {
    modoRuta = !modoRuta;
    if ($ciudad) $ciudad.classList.toggle("modo-ruta", modoRuta);
    e.target.classList.toggle("modo-ruta-activo", modoRuta);
    e.target.textContent = modoRuta
      ? "✓ Modo mapa activo (clic inicio → fin)"
      : "📍 Marcar ruta en el mapa";
    document.getElementById("ruta-hint").style.display = modoRuta ? "block" : "none";
    document.getElementById("btn-limpiar-ruta").style.display = modoRuta ? "block" : "none";
    if (!modoRuta) { rutaSel = []; limpiarRutaClases(); }
  });
  document.getElementById("btn-limpiar-ruta").addEventListener("click", () => {
    rutaSel = [];
    limpiarRutaClases();
    document.getElementById("amb-vias").value = "";
  });

  // ─── Botones ──────────────────────────────────────────────
  document.getElementById("btn-amb").addEventListener("click", async () => {
    const txt = document.getElementById("amb-vias").value || "";
    const vias = txt.split(",").map((s) => s.trim()).filter(Boolean);
    if (!vias.length) { alert("Indica al menos una intersección o márcala en el mapa"); return; }
    const r = await postJSON("/api/ambulancia", { vias });
    pingOut(`AMBULANCIA → ${JSON.stringify(r)}`);
  });

  document.getElementById("btn-fz").addEventListener("click", async () => {
    const interseccion = document.getElementById("fz-int").value;
    const nuevo_estado = document.getElementById("fz-estado").value;
    const r = await postJSON("/api/forzar_semaforo",
                             { interseccion, nuevo_estado });
    pingOut(`CAMBIAR ${interseccion}=${nuevo_estado} → ${JSON.stringify(r)}`);
  });

  document.getElementById("btn-ping").addEventListener("click", async () => {
    const r = await fetch("/api/ping_analitica").then((x) => x.json());
    pingOut(`PING → ${JSON.stringify(r)}`);
  });

  // ─── Helpers ──────────────────────────────────────────────
  function formatNum(v) {
    if (v === null || v === undefined || v === "") return "-";
    const n = Number(v);
    if (Number.isNaN(n)) return v;
    if (Number.isInteger(n)) return n.toString();
    return n.toFixed(1);
  }

  function addStreamLine(e) {
    const li = document.createElement("li");
    li.className = "t-" + (e.topic || "x");
    const ts = e.ts ? new Date(e.ts).toLocaleTimeString() : "";
    let detalle = "";
    if (e.topic === "camara")           detalle = `vol=${e.Q} vel=${formatNum(e.Vp)}`;
    else if (e.topic === "espira_inductiva") detalle = `cnt=${e.Cv}`;
    else if (e.topic === "gps")         detalle = `${e.nivel_gps || ""} v=${formatNum(e.Vp)}`;
    li.innerHTML =
      `<span class="topic">${e.topic || "?"}</span>` +
      `<span class="ts">${ts}</span>` +
      `<span><b>${e.interseccion || ""}</b> ${detalle}</span>`;
    $stream.prepend(li);
    while ($stream.children.length > MAX_STREAM) $stream.lastChild.remove();
  }

  function pingOut(msg) {
    const out = document.getElementById("ping-out");
    out.textContent = `${new Date().toLocaleTimeString()}  ${msg}\n` + out.textContent;
    if (out.textContent.length > 800) out.textContent = out.textContent.slice(0, 800);
  }

  async function postJSON(url, body) {
    try {
      const resp = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      return await resp.json();
    } catch (err) {
      return { ok: false, error: err.message };
    }
  }

  // Carga inicial vía REST por si el WebSocket arranca lento
  fetch("/api/estado").then((x) => x.json()).then((d) => {
    if (d.intersecciones) {
      Object.entries(d.intersecciones).forEach(([id, info]) => {
        const el = document.getElementById(id);
        if (!el) return;
        const estado = info.estado || "DESCONOCIDO";
        el.dataset.estado = estado;
        el.querySelector(".q-val").textContent  = formatNum(info.Q);
        el.querySelector(".vp-val").textContent = formatNum(info.Vp);
        el.querySelector(".cv-val").textContent = formatNum(info.Cv);
        aplicarSemaforo(el, estado);
      });
    }
  });
})();
