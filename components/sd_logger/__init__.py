import esphome.codegen as cg
import esphome.config_validation as cv
from esphome.components import time as time_comp
from esphome.components import sensor as sensor_comp
from esphome.components import binary_sensor as binary_sensor_comp
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

CONFIG_SCHEMA = cv.Schema(
    {
        cv.GenerateID(): cv.declare_id(SdLogger),

        cv.Required("time_id"): cv.use_id(time_comp.RealTimeClock),
        cv.Required(CONF_UPLOAD_URL): cv.string,
        cv.Optional(CONF_BEARER_TOKEN, default=""): cv.string,
        cv.Required(CONF_LOG_PATH): cv.string,
        cv.Required(CONF_BACKOFF_INITIAL): cv.positive_time_period_milliseconds,
        cv.Required(CONF_BACKOFF_MAX): cv.positive_time_period_milliseconds,

        cv.Required(CONF_SENSORS): cv.ensure_list(cv.use_id(sensor_comp.Sensor)),
    }
)

async def to_code(config):
    var = cg.new_Pvariable(config[CONF_ID])
    await cg.register_component(var, config)

    time_var = await cg.get_variable(config["time_id"])
    cg.add(var.set_time(time_var))

    cg.add(var.set_upload_url(config[CONF_UPLOAD_URL]))
    cg.add(var.set_bearer_token(config[CONF_BEARER_TOKEN]))
    cg.add(var.set_log_path(config[CONF_LOG_PATH]))
    cg.add(var.set_backoff_initial_ms(config[CONF_BACKOFF_INITIAL]))
    cg.add(var.set_backoff_max_ms(config[CONF_BACKOFF_MAX]))

    # Attach sensors
    for s in config[CONF_SENSORS]:
        sensor_var = await cg.get_variable(s)
        cg.add(var.add_sensor(sensor_var))
        
    # Auto-create "Sync Online" binary sensor
    sync_online = cg.new_Pvariable(cg.generate_id("sd_logger_sync_online"))
    sync_online.set_name(f"{config[CONF_ID]} Sync Online")
    cg.add(var.set_sync_online_binary_sensor(sync_online))

    # Auto-create "Sync Sending Backlog" binary sensor
    sync_backlog = cg.new_Pvariable(cg.generate_id("sd_logger_sync_backlog"))
    sync_backlog.set_name(f"{config[CONF_ID]} Sending Backlog")
    cg.add(var.set_sync_sending_backlog_binary_sensor(sync_backlog))