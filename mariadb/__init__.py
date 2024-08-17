import asyncio
import logging
import voluptuous as vol
from homeassistant.const import CONF_HOST, CONF_PORT, CONF_USERNAME, CONF_PASSWORD
import homeassistant.helpers.config_validation as cv
import aiomysql

_LOGGER = logging.getLogger(__name__)
DOMAIN = "mariadb"
CONF_DATABASE = "database"
CONF_ACTION1 = "action1"
CONF_ACTION2 = "action2"
CONF_ACTION3 = "action3"
SERVICE_EXECUTE = "execute"

DEFAULT_PORT = 3306

SERVICE_EXECUTE_SCHEMA = vol.Schema({
    vol.Required(CONF_HOST): cv.string,
    vol.Optional(CONF_PORT, default=DEFAULT_PORT): cv.port,
    vol.Required(CONF_USERNAME): cv.string,
    vol.Required(CONF_PASSWORD): cv.string,
    vol.Required(CONF_DATABASE): cv.string,
    vol.Required(CONF_ACTION1): cv.string,
    vol.Optional(CONF_ACTION2): cv.string,
    vol.Optional(CONF_ACTION3): cv.string,
})

async def connect_to_database(username, password, host, database, loop, port=DEFAULT_PORT):
    try:
        connection = await aiomysql.connect(
            user=username,
            password=password,
            host=host,
            port=port,
            db=database,
            loop=loop
        )
        return connection
    except aiomysql.Error as e:
        _LOGGER.error("Failed to connect to MySQL at %s:%s (database: %s): %s", 
                      host, port, database, e)
        raise  # Re-raise the exception

async def async_setup(hass, config):
    async def execute(call):
        host = call.data.get(CONF_HOST)
        port = call.data.get(CONF_PORT, DEFAULT_PORT)
        username = call.data.get(CONF_USERNAME)
        password = call.data.get(CONF_PASSWORD)
        database = call.data.get(CONF_DATABASE)
        action1 = call.data.get(CONF_ACTION1)
        action2 = call.data.get(CONF_ACTION2)
        action3 = call.data.get(CONF_ACTION3)
        loop = hass.loop
        
        try:
            connection = await connect_to_database(username, password, host, database, loop, port)
        
            if connection:
                try:
                    async with connection.cursor() as cursor:
                        _LOGGER.debug("mariaDB: about to execute %s", action1)
                        await cursor.execute(action1)
                    
                        if action2 is not None:
                            _LOGGER.debug("mariaDB: about to execute %s", action2)
                            await cursor.execute(action2)
                            something = cursor.lastrowid
                            _LOGGER.debug("mariaDB: After Action 2, Last Row Id = %s", something)
                    
                        if action3 is not None:
                            _LOGGER.debug("mariaDB: about to execute %s", action3)
                            await cursor.execute(action3)
                
                    await connection.commit()
                except aiomysql.Error as e:
                    _LOGGER.error("mariaDB: ERROR %s", e)
                finally:
                    connection.close()
            else:
                _LOGGER.error("mariaDB: Unable to establish connection to the database.")
        except Exception as e:
            _LOGGER.error("An error occurred while connecting to or interacting with the database: %s", e)


    hass.services.async_register(DOMAIN, SERVICE_EXECUTE, execute, schema=SERVICE_EXECUTE_SCHEMA)
    return True