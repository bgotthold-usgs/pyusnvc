# pyusnvc


The US National Vegetation Classification is the US national system for vegetation classification and mapping. This package helps to manage the data for a distribution provided by the USGS.

## Description


This package handles getting the source data for the USNVC provided by NatureServe and housed in a ScienceBase Item into a format for distribution via a REST API. We provide it as a package in order to support full transparency on what we are doing with the data and to serve as a building block for anyone else that may want to do something similar.

The core usnvc module contains functions that process source data from a [ScienceBase Item](https://www.sciencebase.gov/catalog/item/5aa827a2e4b0b1c392ef337a). Source data originate in a Microsoft Access database that is an attachment on the ScienceBase Item. This file is transformed to a SQLite database via a desktop tool and also attached to the ScienceBase Item with a "Source Data" title to indicate the data to be processed. We transformed the data to SQLite this way for convenience in processing the data on non-Windows systems after running into challenges with ODBC drivers that might allow Python connections to the MS Access database itself.

The core functions of the package include the following:

* build_unit() - Takes an element_global_id integer value and builds a single document from all of the related database tables in the source database.
* build_hierarchy() - Called from within build_unit() to develop the hierarchy above and immediately below a given element_global_id.

Other functions, documented within the usnvc module, handle various parts of the database connection and unit assembly process.

## Dependencies


The package uses some basic Python tools in Python 3.x and above along with the following specific dependencies:

* pandas - Used for reading data from the SQLite database via the read_sql_query method and and outputting various data structures. The logic for assembling related information from the database is handled with SQL queries.
* sciencebasepy - Used for working with the source item in ScienceBase to retrieve the database.
* pycountry - Used in the get_place_code_data() function to retrieve a full country name for the structure representing global distribution of a given USNVC unit.

It is recommended that you set up a discrete Python environment for this project using your tool of choice. The install_requires section of the setup.py should create your dependencies for you on install. You can install from source with a local clone or directly from the source repo with...

``pip install git+git://github.com/usgs-bcb/pyusnvc.git@master``

...or...

``pip install git+git://github.com/usgs-bcb/pyusnvc.git@develop``

...for the latest.


The code is made to be run in any environment. An example Python script is provided in the example_scripts folder showing to create a local cache of every USNVC Unit document.

## Provisional Software Statement


Under USGS Software Release Policy, the software codes here are considered preliminary, not released officially, and posted to this repo for informal sharing among colleagues.

This software is preliminary or provisional and is subject to revision. It is being provided to meet the need for timely best science. The software has not received final approval by the U.S. Geological Survey (USGS). No warranty, expressed or implied, is made by the USGS or the U.S. Government as to the functionality of the software and related material nor shall the fact of release constitute any such warranty. The software is provided on the condition that neither the USGS nor the U.S. Government shall be held liable for any damages resulting from the authorized or unauthorized use of the software.