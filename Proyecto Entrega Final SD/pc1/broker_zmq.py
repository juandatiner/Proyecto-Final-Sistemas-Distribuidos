"""
pc1/broker_zmq.py
=====================================================================
Broker ZeroMQ - PC1
Actúa como intermediario entre los sensores (PUB) y el servicio
de analítica en PC2 (SUB).

Diseño original   : proxy XSUB/XPUB de un solo hilo.
Diseño modificado : múltiples hilos (uno por topic) para mayor throughput.

Uso:
    python broker_zmq.py                   # modo simple (default)
    python broker_zmq.py --modo multihilo  # modo multihilo
=====================================================================
"""
import zmq
import threading
import signal
import sys
import argparse
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.config_loader import cargar_config
from common.logger import configurar_logger

logger = configurar_logger("BrokerZMQ")

# ─────────────────────────────────────────────────────────────────
# DISEÑO ORIGINAL: proxy XSUB → XPUB de un solo hilo
# ─────────────────────────────────────────────────────────────────

def iniciar_proxy_simple(config: dict) -> None:
    """
    Inicia el proxy ZeroMQ en modo single-thread.
    Todos los mensajes de todos los sensores pasan por el mismo hilo.
    """
    ctx = zmq.Context()

    # XSUB: los sensores (PUB) se conectan aquí
    xsub = ctx.socket(zmq.XSUB)
    xsub.bind(f"tcp://*:{config['red']['puertos']['broker_sub']}")
    logger.info(f"[Simple] XSUB escuchando en *:{config['red']['puertos']['broker_sub']}")

    # XPUB: el servicio de analítica (SUB) se conecta aquí
    xpub = ctx.socket(zmq.XPUB)
    xpub.bind(f"tcp://*:{config['red']['puertos']['broker_pub']}")
    logger.info(f"[Simple] XPUB publicando en *:{config['red']['puertos']['broker_pub']}")

    logger.info("[Simple] Broker iniciado — esperando mensajes...")

    def _shutdown(sig, frame):
        logger.info("[Simple] Señal recibida, cerrando broker...")
        xsub.close()
        xpub.close()
        ctx.term()
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    try:
        # zmq.proxy bloquea indefinidamente; es el corazón del broker
        zmq.proxy(xsub, xpub)
    except zmq.ZMQError as e:
        if e.errno != zmq.ETERM:
            logger.error(f"Error en proxy: {e}")
        ctx.term()


# ─────────────────────────────────────────────────────────────────
# DISEÑO MODIFICADO: un hilo por topic
# ─────────────────────────────────────────────────────────────────

def _hilo_topic(ctx: zmq.Context, topic: str, port_in: int, port_out: int,
                stop_event: threading.Event) -> None:
    """
    Hilo dedicado a un topic específico.
    Recibe mensajes del socket interno de distribución y los reenvía
    al canal de salida.

    Args:
        ctx        : contexto ZMQ compartido
        topic      : nombre del topic ('camara', 'espira_inductiva', 'gps')
        port_in    : puerto del canal interno de entrada (inproc)
        port_out   : puerto del canal interno de salida (inproc)
        stop_event : evento para detener el hilo limpiamente
    """
    sub = ctx.socket(zmq.SUB)
    sub.connect(f"inproc://distribuidor")
    sub.setsockopt_string(zmq.SUBSCRIBE, topic)
    sub.setsockopt(zmq.RCVTIMEO, 500)   # timeout para poder chequear stop_event

    pub = ctx.socket(zmq.PUB)
    pub.connect(f"inproc://colector")

    logger.info(f"[Hilo-{topic}] Iniciado")

    conteo = 0
    while not stop_event.is_set():
        try:
            partes = sub.recv_multipart()
            pub.send_multipart(partes)
            conteo += 1
            if conteo % 50 == 0:
                logger.debug(f"[Hilo-{topic}] {conteo} mensajes procesados")
        except zmq.Again:
            continue   # timeout, volver a revisar stop_event
        except zmq.ZMQError as e:
            if e.errno == zmq.ETERM:
                break
            logger.error(f"[Hilo-{topic}] Error ZMQ: {e}")
            break

    sub.close()
    pub.close()
    logger.info(f"[Hilo-{topic}] Detenido (total: {conteo} mensajes)")


def iniciar_proxy_multihilo(config: dict) -> None:
    """
    Inicia el broker en modo multihilo.
    Cada topic tiene su propio hilo de procesamiento, lo que permite
    paralelismo en la recepción y reenvío de eventos.

    Arquitectura interna:
        Sensores  →  XSUB (externo)
                       ↓
                   PUB inproc://distribuidor
                       ↓  (un hilo por topic)
                   SUB inproc://distribuidor  →  PUB inproc://colector
                                                        ↓
                                              SUB inproc://colector
                                                        ↓
                                              XPUB (externo)  →  Analítica
    """
    ctx = zmq.Context()
    stop_event = threading.Event()

    # ── Sockets externos ──────────────────────────────────────────
    xsub_ext = ctx.socket(zmq.XSUB)
    xsub_ext.bind(f"tcp://*:{config['red']['puertos']['broker_sub']}")
    # ── FIX multihilo ────────────────────────────────────────────
    # El XSUB debe enviar una suscripción "a todo" hacia los sensores
    # PUB; de lo contrario, los PUB filtran y NO envían nada (throughput
    # cero). En el proxy simple esto lo hace zmq.proxy automáticamente.
    # El byte 0x01 con prefijo vacío = "suscribirse a todos los tópicos".
    # ZMQ reenvía esta suscripción también a los PUB que se conecten
    # más tarde, así que basta hacerlo una vez al inicio.
    xsub_ext.send(b"\x01")

    xpub_ext = ctx.socket(zmq.XPUB)
    xpub_ext.bind(f"tcp://*:{config['red']['puertos']['broker_pub']}")

    # ── Sockets internos (inproc) ─────────────────────────────────
    distribuidor = ctx.socket(zmq.PUB)
    distribuidor.bind("inproc://distribuidor")

    colector = ctx.socket(zmq.SUB)
    colector.bind("inproc://colector")
    colector.setsockopt_string(zmq.SUBSCRIBE, "")

    topics = ["camara", "espira_inductiva", "gps"]
    hilos = []
    for t in topics:
        hilo = threading.Thread(
            target=_hilo_topic,
            args=(ctx, t, 0, 0, stop_event),
            name=f"Broker-{t}",
            daemon=True
        )
        hilo.start()
        hilos.append(hilo)

    logger.info(f"[Multihilo] Broker iniciado con {len(topics)} hilos")

    # ── Hilo: XSUB externo → distribuidor interno ─────────────────
    def _receptor():
        poller = zmq.Poller()
        poller.register(xsub_ext, zmq.POLLIN)
        while not stop_event.is_set():
            eventos = dict(poller.poll(500))
            if xsub_ext in eventos:
                msg = xsub_ext.recv_multipart()
                distribuidor.send_multipart(msg)

    # ── Hilo: colector interno → XPUB externo ────────────────────
    def _emisor():
        poller = zmq.Poller()
        poller.register(colector, zmq.POLLIN)
        while not stop_event.is_set():
            eventos = dict(poller.poll(500))
            if colector in eventos:
                msg = colector.recv_multipart()
                xpub_ext.send_multipart(msg)

    t_rx = threading.Thread(target=_receptor, name="Broker-RX", daemon=True)
    t_tx = threading.Thread(target=_emisor,   name="Broker-TX", daemon=True)
    t_rx.start()
    t_tx.start()

    def _shutdown(sig, frame):
        logger.info("[Multihilo] Señal recibida, cerrando broker...")
        stop_event.set()
        for h in hilos + [t_rx, t_tx]:
            h.join(timeout=2.0)
        xsub_ext.close()
        xpub_ext.close()
        distribuidor.close()
        colector.close()
        ctx.term()
        logger.info("[Multihilo] Broker cerrado correctamente")
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    # Bloquear hilo principal
    stop_event.wait()


# ─────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Broker ZeroMQ para el sistema de tráfico urbano."
    )
    parser.add_argument(
        "--modo",
        choices=["simple", "multihilo"],
        default="simple",
        help="Modo de operación del broker (default: simple)"
    )
    args = parser.parse_args()

    cfg = cargar_config()

    logger.info(f"Iniciando broker en modo: {args.modo.upper()}")
    if args.modo == "multihilo":
        iniciar_proxy_multihilo(cfg)
    else:
        iniciar_proxy_simple(cfg)
