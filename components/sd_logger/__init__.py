import esphome.codegen as cg
import esphome.config_validation as cv
from esphome.const import CONF_ID, CONF_URL

# Create namespace
sdlog_ns = cg.esphome_ns.namespace("sdlog")
SDLogger = sdlog_ns.class_("SDLogger", cg.Component)

# YAML Config Keys
CONF_UPLOAD_URL = "upload_url"
CONF_BEARER_TOKEN = "bearer_token"
CONF_LOG_PATH = "log_path"
CONF_GZIP = "gzip"
CONF_UPLOAD_INTERVAL = "upload_interval_ms"
CONF_LIVE_THROTTLE = "live_throttle_ms"
CONF_BACKOFF_INITIAL = "backoff_initial_ms"
CONF_BACKOFF_MAX = "backoff_max_ms"

CONFIG_SCHEMA = cv.Schema({
    cv.GenerateID(): cv.declare_id(SDLogger),

    cv.Required(CONF_UPLOAD_URL): cv.string,
    cv.Optional(CONF_BEARER_TOKEN, default=""): cv.string,

    cv.Optional(CONF_LOG_PATH, default="/sdcard/logs"): cv.string,
    cv.Optional(CONF_GZIP, default=True): cv.boolean,

    cv.Optional(CONF_UPLOAD_INTERVAL, default=300000): cv.positive_int,
    cv.Optional(CONF_LIVE_THROTTLE, default=30000): cv.positive_int,

    cv.Optional(CONF_BACKOFF_INITIAL, default=30000): cv.positive_int,
    cv.Optional(CONF_BACKOFF_MAX, default=900000): cv.positive_int,
}).extend(cv.COMPONENT_SCHEMA)


async def to_code(config):
    var = cg.new_Pvariable(config[CONF_ID])
    await cg.register_component(var, config)

    cg.add(var.set_upload_url(config[CONF_UPLOAD_URL]))
    cg.add(var.set_bearer_token(config[CONF_BEARER_TOKEN]))
    cg.add(var.set_log_path(config[CONF_LOG_PATH]))
    cg.add(var.set_gzip(config[CONF_GZIP]))
    cg.add(var.set_upload_interval_ms(config[CONF_UPLOAD_INTERVAL]))
    cg.add(var.set_live_throttle_ms(config[CONF_LIVE_THROTTLE]))

    cg.add(var.set_backoff(
        config[CONF_BACKOFF_INITIAL],
        config[CONF_BACKOFF_MAX]
    ))

    # Sensors will be attached manually from YAML using on_value hooks, no lambdas.
