import re
from setuptools import setup, find_packages

with open('requirements.txt') as requirements_file:
    install_requirements = requirements_file.read().splitlines()

setup(
    name        = "sync-infra-configurations",
    version     = "0.0.0",
    description = "sync-infra-configurations",
    author      = "suzuki-navi",
    packages    = find_packages(),
    install_requires = install_requirements,
    include_package_data = True,
    entry_points = {
        "console_scripts": [
            "sync-infra-configurations = sync_infra_configurations.main:main",
        ]
    },
)

