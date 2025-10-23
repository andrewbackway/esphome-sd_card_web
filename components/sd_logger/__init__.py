import esphome.codegen as cg
import esphome.config_validation as cv
from esphome.components import time as time_comp
from esphome.components import sensor as sensor_comp
from esphome.components import binary_sensor 
from esphome.const import (
    CONF_ID
)

AUTO_LOAD = ["sensor", "binary_sensor", "time", "json"]
sd_logger_ns = cg.esphome_ns.namespace("sd_logger")
SdLogger = sd_logger_ns.class_("SdLogger", cg.Component)

CONF_UPLOAD_URL = "upload_url"
CONF_BEARER_TOKEN = "bearer_token"
CONF_LOG_PATH = "log_path"
CONF_BACKOFF_INITIAL = "backoff_initial"
CONF_BACKOFF_MAX = "backoff_max"
CONF_SENSORS = "sensors"

CONF_PING_URL = "ping_url"
CONF_PING_INTERVAL = "ping_interval"
CONF_PING_TIMEOUT = "ping_timeout"

CONF_SYNC_ONLINE = "sync_online"
CONF_SYNC_SENDING_BACKLOG = "sync_sending_backlog"

CONFIG_SCHEMA = cv.Schema(
    {
        cv.GenerateID(): cv.declare_id(SdLogger),

        cv.Required("time_id"): cv.use_id(time_comp.RealTimeClock),
        cv.Required(CONF_UPLOAD_URL): cv.string,
        cv.Optional(CONF_BEARER_TOKEN, default=""): cv.string,
        cv.Required(CONF_LOG_PATH): cv.string,
        cv.Required(CONF_BACKOFF_INITIAL): cv.positive_time_period_milliseconds,
        cv.Required(CONF_BACKOFF_MAX): cv.positive_time_period_milliseconds,

        cv.Optional(CONF_PING_URL, default=""): cv.string,
        cv.Optional(CONF_PING_INTERVAL, default="30s"): cv.positive_time_period_milliseconds,
        cv.Optional(CONF_PING_TIMEOUT,  default="3s"):  cv.positive_time_period_milliseconds,

        cv.Required(CONF_SENSORS): cv.ensure_list(cv.use_id(sensor_comp.Sensor)),

        cv.Optional(CONF_SYNC_ONLINE): binary_sensor.binary_sensor_schema(
            default_name="Sync Online"
        ),
        cv.Optional(CONF_SYNC_SENDING_BACKLOG): binary_sensor.binary_sensor_schema(
            default_name="Sync Sending Backlog"
        ),
    }
)


async def to_code(config):
    var = cg.new_Pvariable(config[CONF_ID])
    await cg.register_component(var, config)

    # Time reference
    time_var = await cg.get_variable(config["time_id"])
    cg.add(var.set_time(time_var))

    # Config values
    cg.add(var.set_upload_url(config[CONF_UPLOAD_URL]))
    cg.add(var.set_bearer_token(config[CONF_BEARER_TOKEN]))
    cg.add(var.set_log_path(config[CONF_LOG_PATH]))
    cg.add(var.set_backoff_initial_ms(config[CONF_BACKOFF_INITIAL]))
    cg.add(var.set_backoff_max_ms(config[CONF_BACKOFF_MAX]))

    if CONF_PING_URL in config:
        cg.add(var.set_ping_url(config[CONF_PING_URL]))
    cg.add(var.set_ping_interval_ms(config[CONF_PING_INTERVAL].total_milliseconds))
    cg.add(var.set_ping_timeout_ms(config[CONF_PING_TIMEOUT].total_milliseconds))

    # Attach sensors
    sensor_vec = []
    for s in config[CONF_SENSORS]:
        sv = await cg.get_variable(s)
        sensor_vec.append(sv)
    cg.add(var.set_sensors(sensor_vec))

    # Create/attach binary sensors via schema (IDs generated correctly)
    if CONF_SYNC_ONLINE in config:
        bs = await binary_sensor_comp.new_binary_sensor(config[CONF_SYNC_ONLINE])
        cg.add(var.set_sync_online_binary_sensor(bs))

    if CONF_SYNC_SENDING_BACKLOG in config:
        bs2 = await binary_sensor_comp.new_binary_sensor(config[CONF_SYNC_SENDING_BACKLOG])
        cg.add(var.set_sync_sending_backlog_binary_sensor(bs2))