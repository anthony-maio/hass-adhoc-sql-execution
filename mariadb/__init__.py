import asyncio
from functools import partial
import logging
import voluptuous as vol
from homeassistant.const import CONF_HOST, CONF_USERNAME, CONF_PASSWORD
import homeassistant.helpers.config_validation as cv
import aiomysql

_LOGGER = logging.getLogger(__name__)

DOMAIN = "mariadb"

CONF_DATABASE = "database"
CONF_ACTION1 = "action1"
CONF_ACTION2 = "action2"
CONF_ACTION3 = "action3"

SERVICE_EXECUTE = "execute"
SERVICE_EXECUTE_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_HOST): cv.string,
        vol.Required(CONF_USERNAME): cv.string,
        vol.Required(CONF_PASSWORD): cv.string,
        vol.Required(CONF_DATABASE): cv.string,
        vol.Required(CONF_ACTION1): cv.string,
        vol.Optional(CONF_ACTION2): cv.string,
        vol.Optional(CONF_ACTION3): cv.string,
    }
)

async def async_setup(hass, config):

    async def execute(call):
        host = call.data.get(CONF_HOST)
        username = call.data.get(CONF_USERNAME)
        password = call.data.get(CONF_PASSWORD)
        database = call.data.get(CONF_DATABASE)
        action1 = call.data.get(CONF_ACTION1)
        action2 = call.data.get(CONF_ACTION2)
        action3 = call.data.get(CONF_ACTION3)

        cnx = await aiomysql.connect(user=username, password=password, host=host, db=database)
        async with cnx.cursor() as cursor:
            try:
                _LOGGER.debug("mariaDB: about to execute %s", action1)
                await cursor.execute(action1)
            except aiomysql.Error as e:
                _LOGGER.error("mariaDB: ERROR %s", e)
            
            if action2 is not None:
                _LOGGER.debug("mariaDB: about to execute %s", action2)
                await cursor.execute(action2)
                something = cursor.lastrowid
                _LOGGER.debug("mariaDB: After Action 2, Last Row Id = %s", something)

            if action3 is not None:
                _LOGGER.debug("mariaDB: about to execute %s", action3)
                await cursor.execute(action3)

        cnx.close()

    hass.services.async_register(DOMAIN, SERVICE_EXECUTE, execute, schema=SERVICE_EXECUTE_SCHEMA)

    return True
