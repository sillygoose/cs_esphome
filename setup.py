#!/usr/bin/env python

"""CS/ESPHome setup."""
from pathlib import Path
from setuptools import setup

VERSION = "0.3.9"
URL = "https://github.com/sillygoose/cs_esphome.git"

setup(
    name="CS/ESPHome",
    version=VERSION,
    description="CircuitSetup/ESPHome energy data collection utility",
    long_description=Path("README.md").read_text(),
    long_description_content_type="text/markdown",
    url=URL,
    download_url="{}/tarball/{}".format(URL, VERSION),
    author="Rick Naro",
    author_email="sillygoose@me.com",
    license="MIT",
    install_requires=[
        "asyncio",
        "aioesphomeapi",
        "influxdb-client",
        "python-dateutil",
        "python-configuration",
        "pyyaml",
    ],
    zip_safe=True,
)
