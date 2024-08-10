# MariaDB/MySQL AdHoc SQL Execution

### Note: MariaDB is a fork of MySQL that can work as a drop-in replacement for most MySQL use cases.  I will use the terms interchangably, but the component uses mariadb primarily because that's the open source one these days.  I think.  Some corp bought MySQL and AWS uses MariaDB so I'm going with that.

MariaDB Execute is a custom component for Home Assistant that allows you to execute adhoc SQL queries against a MariaDB/MySQL database from within Home Assistant. This component is designed to be flexible, allowing you to specify database credentials with each service call, enabling the use of multiple databases.
**NOTE** Do not use this unless you know what you are doing with it. It is an extremely powerful component making queries to any MySQL/MariaDB you have credentials to including your Home Assistant DB - and you can really eff those things up if you don't know what you're doing.

## Features

- Execute custom SQL queries directly from Home Assistant.
- Supports multiple actions within a single service call.
- Asynchronous operations should play nice with HASS event loop
- No need to configure the component in `configuration.yaml`, instead I make you configure it on every database call. This is a more versatile option.

## Installation

1. Download the `mariadb` directory and place it inside your `custom_components` directory in Home Assistant.

   - The directory structure should look like this:
     ```
     custom_components/
     └── mariadb/
         ├── __init__.py
         ├── manifest.json
         └── (other files)
     ```

2. Restart Home Assistant to load the custom component.

3. You get one service. ONE:

## Service: `mariadb.execute`

The `mariadb.execute` service allows you to execute SQL queries against your MariaDB/MySQL database. Credentials and database details are specified with each call, providing flexibility for connecting to different databases.

### Service Data Attributes

- `host` (required): The hostname or IP address of the MariaDB server.
- `username` (required): The username for the MariaDB database.
- `password` (required): The password for the MariaDB database.
- `database` (required): The database name to execute the query in.
- `action1` (required): The first SQL query to execute.
- `action2` (optional): The second SQL query to execute.
- `action3` (optional): The third SQL query to execute.

### Example Usage - Note: BAD Example Usage

```yaml
service: mariadb.execute
data:
  host: "192.168.1.100"
  username: "your_user"
  password: "your_password"
  database: "home_assistant"
  action1: "UPDATE sensors SET state='on' WHERE entity_id='sensor.living_room_temperature';"
  action2: "INSERT INTO logs (event) VALUES ('Sensor updated');"
```

### License

You can use this code for any purpose for free. Change it for your use case.  If you do something cool, open a PR back to share it with everyone.
What license is that? MIT? IDGAF? Go with that. 
 