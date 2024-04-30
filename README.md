# GIS-Mobility-Toolkit
This repository is created in preparation for an ITSC publication. The GIS-Mobility-Toolkit allows to derive land-use data in a structured way from OSM data. It can be fully integrated into the Mapnik; OSM ecosystem and also brings additional features for geographical data merging.

The full code should be released after the publications acceptance.

## Getting started

For default setup pyyaml is required, then:
```shell
pip install .
```


## Development

1. Setup the python project by installing the required packages via the requirements.yaml with conda or mamba in a prefered venv:
```shell
mamba env create  -f  requirements.yaml
```

2. Secondly open the **smm** folder in vs-code and make sure to have the following extension installed:


```txt
Name: Command Variable
Id: rioj7.command-variable
Description: Calculate command variables for launch.json and tasks.json
Version: 1.61.2
Publisher: rioj7
VS Marketplace Link: https://marketplace.visualstudio.com/items?itemName=rioj7.command-variable
```

3. Debug your modules with the **Python: Aktuelle Datei(Module)** setting. It should be directly executable