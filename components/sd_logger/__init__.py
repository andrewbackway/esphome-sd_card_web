import esphome.codegen as cg
import esphome.config_validation as cv
from esphome.const import CONF_ID
from esphome.components import sensor, time as time_

sdlog_ns = cg.esphome_ns.namespace("sdlog")
SDLogger = sdlog_ns.class_("SDLogger", cg.Component)

CONF_UPLOAD_URL = "upload_url"
CONF_BEARER_TOKEN = "bearer_token"
CONF_LOG_PATH = "log_path"
CONF_UPLOAD_INTERVAL = "upload_interval"
CONF_BACKOFF_INITIAL = "backoff_initial"
CONF_BACKOFF_MAX = "backoff_max"
CONF_SENSORS = "sensors"
CONF_GZIP = "gzip"
CONF_TIME_ID = "time_id"

CONFIG_SCHEMA = cv.Schema({
    cv.GenerateID(): cv.declare_id(SDLogger),

    cv.Required(CONF_UPLOAD_URL): cv.url,
    cv.Required(CONF_BEARER_TOKEN): cv.string_strict,

    cv.Optional(CONF_LOG_PATH, default="/logs"): cv.string_strict,
    cv.Optional(CONF_UPLOAD_INTERVAL, default="5min"): cv.positive_time_period_milliseconds,
    cv.Optional(CONF_BACKOFF_INITIAL, default="30s"): cv.positive_time_period_milliseconds,
    cv.Optional(CONF_BACKOFF_MAX, default="1h"): cv.positive_time_period_milliseconds,

    # Explicit, safe default: if omitted, log none
    cv.Optional(CONF_SENSORS, default=[]): cv.ensure_list(cv.use_id(sensor.Sensor)),

    # Optional gzip compression
    cv.Optional(CONF_GZIP, default=False): cv.boolean,

    # Optional time source for real timestamps
    cv.Optional(CONF_TIME_ID): cv.use_id(time_.RealTimeClock),
}).extend(cv.COMPONENT_SCHEMA)


async def to_code(config):
    var = cg.new_Pvariable(config[CONF_ID])
    await cg.register_component(var, config)

    cg.add(var.set_upload_url(config[CONF_UPLOAD_URL]))
    cg.add(var.set_bearer_token(config[CONF_BEARER_TOKEN]))
    cg.add(var.set_log_path(config[CONF_LOG_PATH]))
    cg.add(var.set_upload_interval_ms(config[CONF_UPLOAD_INTERVAL]))
    cg.add(var.set_backoff_initial_ms(config[CONF_BACKOFF_INITIAL]))
    cg.add(var.set_backoff_max_ms(config[CONF_BACKOFF_MAX]))
    cg.add(var.set_gzip_enabled(config[CONF_GZIP]))

    # Compile-time flag for gzip
    if config[CONF_GZIP]:
        cg.add_build_flag("-DUSE_SDLOGGER_GZIP")

    if CONF_TIME_ID in config:
        t = await cg.get_variable(config[CONF_TIME_ID])
        cg.add(var.set_time(t))

    for s in config[CONF_SENSORS]:
        sens = await cg.get_variable(s)
        cg.add(var.add_tracked_sensor(sens))
