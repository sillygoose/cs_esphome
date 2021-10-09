# **CS/ESPHome**
Python data collection utility for the CircuitSetup Expandable 6 Channel ESP32 Energy Meter using the ESPHome API writing sensor data and integrations to an InfluxDB2 database.

## What's new
#### 0.1.6
Renaming of files and repos to avoid using `esphome`

#### 0.1.5
New database schema supports reporting sensor and location values and integration (today, month, and year totals).

#### 0.1.4
CS_ESP_DEBUG environment variable
- ignores debug options in YAML file when not defined

#### 0.1.3
queries
- sensors with integration write daily, monthly, and annual totals

#### 0.1.2
More improvements
- Can delete and/or create the database if requested
- More tasks to prepare for future database queries
- Improved error handling

#### 0.1.0
Coming together but still a prototype.
- ESPHome sensors written to InfluxDB2
- YAML file configuration support

#
### Requirements
- Python 3.8 or later (currently using 3.9.5 from an Ubuntu Hirsute Docker container)
- Python packages used include (but the list in the `setup.py` file is the definitive list of packages)
    - asyncio
    - aioesphomeapi
    - python-dateutil
    - influxdb-client
    - python-configuration
    - pyyaml

- CircuitSetup Expandable 6 Channel ESP32 Energy Meter hardware (developed using a 24 channel version)
- Docker (a Dockerfile is supplied to allow running in a Docker container, I run this on a Raspberry Pi4 with 8GB memory that also has containers running instances of Portainer, InfluxDB2, Telegraf, Grafana, and other useful applications)

## Installation
1.  Clone the **CS/ESPHome** repository and install the Python packages:

```
    git clone https://github.com/sillygoose/cs_esphome.git
    cd cs_esphome
    pip3 install -e .
```

2.  Rename the `example.secrets.yaml` file to `cs_esphome_secrets.yaml`, if you plan on using secrets.  The `cs_esphome_secrets.yaml` file is tagged in the `.gitignore` file and will not be included in your repository but if you wish you can put `cs_esphome_secrets.yaml` in any parent directory as **CS/ESPHome** will start in the current directory and look in each parent directory up to your home directory for it (or just the current directory if you are not running in a user profile).

    Edit `cs_esphome.yaml` and `cs_esphome_secrets.yaml` to match your CircuitSetup hardware, you will need the URL, port, and password for the CircuitSetup ESPHome API.  If interfacing to InfluxDB you need the host URL,  site name, bucket, and login credentials.

    There are some other fields to configure for the log files, time zone, etc, these should be easy to figure out.

3.  Test that **CS/ESPHome** connects to your CircuitSetup hardware and the InfluxDB database:

    `python3 cs_esphome/cs_esphome.py`

    Run the InfluxDB dashbaord and confirm that the database is being ppoulated with records.
#
### Docker setup
Once you have the working `cs_esphome.yaml` and `cs_esphome_secrets.yaml` files you can build a Docker container that runs **CS/ESPHome**:

```
    sudo docker build --no-cache -t cs_esphome:your-tag .
    sudo docker image tag cs_esphome:your-tag cs_esphome:latest
    sudo docker-compose up -d
```

where `your-tag` is a string of your choosing (the `--no-cache` option will force Docker to pull the latest version of **CS/ESPHome** from GitHub).  The `docker-compose.yaml` file assumes the image to be `cs_esphome:latest`, the second command adds this tag so you can use the docker-compose file to start the new instance and keep the old image as a backup until the new version checks out.

As an example, suppose you download the current **CS/ESPHome** build of 1.0.0.  Then to create and run the Docker container you would use

```
    sudo docker build --no-cache -t cs_esphome:1.0.0 .
    sudo docker image tag cs_esphome:1.0.0 cs_esphome:latest
    sudo docker-compose up -d
```
#
## InfluxDB2 Schemas
Data is organized in InfluxDB2 using the following schemas, refer to the Flux queries for examples of pulling data from InfluxDB2 for dashboard or other use.

    Power:
        _measurement    power
        _location       location (used to group devices by location such as 'kitchen' for all kitchen circuits)
        _field          device(s) (W), line (W), l1 (W), l2 (W)

    Energy:
        _measurement    energy
        _device         device(s), location(s) with a leading underscore
        _field          today (Wh), month (Wh), year (Wh)

    Voltage:
        _measurement    voltage
        _field          line (V), l1 (V), l2 (V)

    Frequency:
        _measurement    frequency
        _field          frequency (Hz)

    Power factor:
        _measurement    power_factor
        _field          pf

    Current:
        _measurement    current
        _field          total (A), missing (A)

#
## Example Dashboards
To be provided

#
## Thanks
Thanks for the following packages used to build this software:
- ESPHome Python API
    - https://github.com/esphome/aioesphomeapi
- InfluxDB2 Python API
    - https://influxdb-client.readthedocs.io/en/stable/api.html
- YAML configuration file support
    - https://python-configuration.readthedocs.io
- Tricks for managing startup and shutdown
    - https://github.com/wbenny/python-graceful-shutdown
