import os
import json
import logging
import signal
from confluent_kafka import Consumer, Producer, KafkaException, KafkaError

# ─── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ─── Configuration (variables d'environnement injectées par Condense) ──────────
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_USERNAME           = os.environ.get("KAFKA_USERNAME", "")
KAFKA_PASSWORD           = os.environ.get("KAFKA_PASSWORD", "")

INPUT_TOPIC              = os.environ.get("INPUT_TOPIC",       "obd-noil")
OUTPUT_TOPIC_V29         = os.environ.get("OUTPUT_TOPIC_V29",  "V29")   # sans warnbat
OUTPUT_TOPIC_V30         = os.environ.get("OUTPUT_TOPIC_V30",  "V30")   # avec warnbat
CONSUMER_GROUP_ID        = os.environ.get("CONSUMER_GROUP_ID", "obd-noil-transform-group")

# ─── Kafka config ──────────────────────────────────────────────────────────────
def build_kafka_config(extra: dict = {}) -> dict:
    config = {"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS}
    if KAFKA_USERNAME and KAFKA_PASSWORD:
        config.update({
            "security.protocol": "SASL_SSL",
            "sasl.mechanism":    "PLAIN",
            "sasl.username":     KAFKA_USERNAME,
            "sasl.password":     KAFKA_PASSWORD,
        })
    config.update(extra)
    return config


def create_consumer() -> Consumer:
    consumer = Consumer(build_kafka_config({
        "group.id":           CONSUMER_GROUP_ID,
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": True,
    }))
    consumer.subscribe([INPUT_TOPIC])
    logger.info(f"Consumer abonné au topic : {INPUT_TOPIC}")
    return consumer


def create_producer() -> Producer:
    producer = Producer(build_kafka_config({
        "acks":             "all",
        "retries":          3,
        "compression.type": "gzip",
    }))
    logger.info(f"Producer prêt → topics de sortie : {OUTPUT_TOPIC_V29} (sans warnbat) | {OUTPUT_TOPIC_V30} (avec warnbat)")
    return producer


# ─── Logique de transformation ─────────────────────────────────────────────────
def transform(msg: dict) -> dict | None:
    """
    Transforme un message NOIL OBD brut en message enrichi et normalisé.

    Champs source :
      noilSocBat, com_soc, noilReadyO, noilLowBatO, noilVoyMilO,
      noilStartI, noilRechargeI, noilKickI, noilCutI, noilBeltsI,
      noilVready, noilErrBatt, noilErrMot, noilThrottleP,
      noilPowInCont1, noilPowInCont2, noilRefSw,
      noilFlagRecharge, noilFlagPrvsRchrg,
      noilStartHBelts, noilPrvsStartH, noilPrvsKickH, noilLkickH,
      noilCutS, noilPrvsCutS, noilStartL, noilPrvsStartL,
      noilMaxCellU, noilMinCellU, noilSOH, com_soh,
      uniqueId, timestamp, latitude, longitude, serialNo,
      altitude, messageType, extBatVol, intBatVol,
      plusCode, event_flag
    """
    try:
        unique_id = msg.get("uniqueId") or msg.get("serialNo")
        timestamp = msg.get("timestamp")

        if not unique_id or not timestamp:
            logger.warning("Message ignoré : uniqueId ou timestamp manquant")
            return None

        # ── Batterie ────────────────────────────────────────────────────────
        soc         = msg.get("noilSocBat", msg.get("com_soc"))   # State of Charge (%)
        soh         = msg.get("noilSOH",    msg.get("com_soh"))    # State of Health (%)
        max_cell_v  = msg.get("noilMaxCellU")                       # Tension max cellule (V)
        min_cell_v  = msg.get("noilMinCellU")                       # Tension min cellule (V)
        ext_bat_vol = msg.get("extBatVol")                          # Tension batterie externe
        int_bat_vol = msg.get("intBatVol")                          # Tension batterie interne

        # Calcul delta tension entre cellules (indicateur de déséquilibre)
        cell_voltage_delta = None
        if max_cell_v is not None and min_cell_v is not None:
            cell_voltage_delta = round(max_cell_v - min_cell_v, 4)

        # ── États / signaux logiques ─────────────────────────────────────────
        is_ready      = bool(msg.get("noilReadyO"))       # Véhicule prêt
        is_low_bat    = bool(msg.get("noilLowBatO"))      # Batterie faible
        is_voyage_mil = bool(msg.get("noilVoyMilO"))      # Voyant MIL
        is_vready     = bool(msg.get("noilVready"))       # V-Ready
        is_recharging = bool(msg.get("noilFlagRecharge")) # En charge

        # ── Entrées de commande ──────────────────────────────────────────────
        start_input    = bool(msg.get("noilStartI"))      # Signal démarrage
        recharge_input = bool(msg.get("noilRechargeI"))   # Signal recharge
        kick_input     = bool(msg.get("noilKickI"))       # Signal kick
        cut_input      = bool(msg.get("noilCutI"))        # Signal coupure
        belts_input    = bool(msg.get("noilBeltsI"))      # Signal ceintures

        # ── Erreurs ──────────────────────────────────────────────────────────
        err_battery = bool(msg.get("noilErrBatt"))
        err_motor   = bool(msg.get("noilErrMot"))
        has_error   = err_battery or err_motor

        # ── Puissance / Contrôleurs ──────────────────────────────────────────
        throttle_pct  = msg.get("noilThrottleP")          # Position accélérateur (%)
        pow_cont1     = msg.get("noilPowInCont1")          # Puissance contrôleur 1
        pow_cont2     = msg.get("noilPowInCont2")          # Puissance contrôleur 2
        ref_sw        = msg.get("noilRefSw")               # Référence switch

        # ── Historiques / compteurs ──────────────────────────────────────────
        start_history = msg.get("noilPrvsStartH")
        kick_history  = msg.get("noilPrvsKickH")
        lkick_history = msg.get("noilLkickH")
        cut_seconds   = msg.get("noilCutS")
        start_level   = msg.get("noilStartL")

        # ── Localisation ─────────────────────────────────────────────────────
        latitude  = msg.get("latitude",  0)
        longitude = msg.get("longitude", 0)
        altitude  = msg.get("altitude",  0)
        plus_code = msg.get("plusCode")
        has_gps   = latitude != 0 and longitude != 0

        # ── Alerte faible SOC (seuil configurable via ENV) ───────────────────
        soc_alert_threshold = float(os.environ.get("SOC_ALERT_THRESHOLD", "20.0"))
        soc_alert = soc is not None and soc < soc_alert_threshold

        # ── Routage selon présence de warnbat ────────────────────────────────
        has_warnbat  = "warnbat" in msg
        target_topic = OUTPUT_TOPIC_V30 if has_warnbat else OUTPUT_TOPIC_V29

        # ── Message enrichi ──────────────────────────────────────────────────
        transformed = {
            # Identité
            "device_id":    unique_id,
            "serial_no":    msg.get("serialNo"),
            "timestamp":    timestamp,
            "message_type": msg.get("messageType", "obd"),
            "event_flag":   msg.get("event_flag"),

            # Batterie
            "battery": {
                "soc_pct":           soc,
                "soh_pct":           soh,
                "max_cell_voltage":  max_cell_v,
                "min_cell_voltage":  min_cell_v,
                "cell_voltage_delta": cell_voltage_delta,
                "ext_voltage":       ext_bat_vol,
                "int_voltage":       int_bat_vol,
                "is_low":            is_low_bat,
                "is_charging":       is_recharging,
                "soc_alert":         soc_alert,
            },

            # État du véhicule
            "vehicle_state": {
                "is_ready":      is_ready,
                "is_vready":     is_vready,
                "voyage_mil_on": is_voyage_mil,
                "throttle_pct":  throttle_pct,
                "pow_cont1":     pow_cont1,
                "pow_cont2":     pow_cont2,
                "ref_sw":        ref_sw,
            },

            # Commandes (inputs)
            "inputs": {
                "start":    start_input,
                "recharge": recharge_input,
                "kick":     kick_input,
                "cut":      cut_input,
                "belts":    belts_input,
            },

            # Erreurs
            "errors": {
                "battery": err_battery,
                "motor":   err_motor,
                "any":     has_error,
            },

            # Historique
            "history": {
                "prev_start_h":  start_history,
                "prev_kick_h":   kick_history,
                "lkick_h":       lkick_history,
                "cut_seconds":   cut_seconds,
                "start_level":   start_level,
                "start_h_belts": msg.get("noilStartHBelts"),
                "prev_cut_s":    msg.get("noilPrvsCutS"),
                "prev_start_l":  msg.get("noilPrvsStartL"),
            },

            # Localisation
            "location": {
                "latitude":  latitude,
                "longitude": longitude,
                "altitude":  altitude,
                "plus_code": plus_code,
                "has_gps":   has_gps,
            },

            # Métadonnées pipeline
            "meta": {
                "source_topic":  INPUT_TOPIC,
                "target_topic":  target_topic,
                "has_warnbat":   has_warnbat,
                "warnbat":       msg.get("warnbat"),  # None si absent
            },
        }

        return transformed, target_topic

    except Exception as e:
        logger.error(f"Erreur de transformation : {e}", exc_info=True)
        return None


# ─── Delivery callback ─────────────────────────────────────────────────────────
def delivery_report(err, msg):
    if err:
        logger.error(f"Échec livraison : {err}")
    else:
        logger.debug(f"Livré → {msg.topic()} [p{msg.partition()}] @{msg.offset()}")


# ─── Boucle principale ─────────────────────────────────────────────────────────
running = True

def handle_shutdown(signum, frame):
    global running
    logger.info("Signal d'arrêt reçu, arrêt propre en cours…")
    running = False

signal.signal(signal.SIGTERM, handle_shutdown)
signal.signal(signal.SIGINT,  handle_shutdown)


def run():
    consumer = create_consumer()
    producer  = create_producer()
    logger.info("Transform démarrée — en attente de messages…")

    try:
        while running:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug(f"Fin de partition {msg.partition()} @{msg.offset()}")
                else:
                    raise KafkaException(msg.error())
                continue

            # Désérialisation
            try:
                payload = json.loads(msg.value().decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                logger.warning(f"Message non-JSON ignoré (offset {msg.offset()}) : {e}")
                continue

            logger.info(
                f"Message reçu | device={payload.get('uniqueId')} "
                f"| SOC={payload.get('noilSocBat')}% "
                f"| warnbat={'oui' if 'warnbat' in payload else 'non'} "
                f"| offset={msg.offset()}"
            )

            # Transformation
            result = transform(payload)
            if result is None:
                continue

            transformed, target_topic = result

            logger.info(
                f"→ routage vers '{target_topic}' "
                f"(warnbat {'présent' if transformed['meta']['has_warnbat'] else 'absent'})"
            )

            # Publication
            producer.produce(
                topic=target_topic,
                key=str(transformed["device_id"]).encode("utf-8"),
                value=json.dumps(transformed).encode("utf-8"),
                callback=delivery_report,
            )
            producer.poll(0)

    finally:
        logger.info("Flush producer et fermeture consumer…")
        producer.flush()
        consumer.close()
        logger.info("Transform arrêtée proprement.")


if __name__ == "__main__":
    run()
