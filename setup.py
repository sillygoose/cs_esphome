#!/usr/bin/env python

"""esphome-cs24 setup."""
from pathlib import Path
from setuptools import setup

VERSION = "0.0.1"
URL = "https://github.com/sillygoose/esphome-cs24.git"

setup(
    name="esphome-cs24",
    version=VERSION,
    description="CircuitSetup power monitoring data collection utility",
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
