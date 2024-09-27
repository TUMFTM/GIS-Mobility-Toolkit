import os, yaml
from setuptools import setup, find_packages

_cdir = os.path.dirname(__file__)
with open(os.path.join(_cdir, "requirements.yaml"), mode="r") as f:
    doc = yaml.safe_load(f)
    install_reqs = []
    if "python ==" in doc["dependencies"][0]:
        del doc["dependencies"][0]
    for req in doc["dependencies"]:
        if isinstance(req, dict):
            if "pip" in req:
                install_reqs += req["pip"]
        elif req != "pip":
            install_reqs.append(req)

find_packages(include=('module*', ))

setup(
    name='spatial-mobility-metrics',
    version='0.1',
    description=
    'This repo. implements methods that enables users to manage spatial distributed mobility metrics.',
    author='Paper_Team',
    author_email='david.ziegler@tum.de',
    package_dir={"smm": "smm"},
    include_package_data=True,
    packages=['smm'],  #same as name
    install_requires=list(
        set(install_reqs)),  #external packages as dependencies
)
