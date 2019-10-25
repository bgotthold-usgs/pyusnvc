"""
This script example runs a simple serial loop over all element_global_id values in the Units table and caches a file
for each unit into a local directory. By default, it places the files into a folder below the one where you execute the
script called 'cache.' You can change this with the cache_path parameter.

By default, the script will run against the latest version of the USNVC (2.03) in ScienceBase. This can be changed to
run against 2.02 by setting version=2.02 on the cache_unit() function.

The first iteration of the loop will fire the db_connection() function to pull the ScienceBase Item as metadata and the
source database file (SQLite version) and cache those locally. Every subsequent iteration will check for that file first
and not bother ScienceBase again.
"""

### TO RUN LOCALLY ###
# install the pyusnvc module pip install -e ./
# python example_scripts/run_usncv.py
######################

import pyusnvc
import os
import json
import time
from zipfile import ZipFile
from sciencebasepy import SbSession


# two different versions possible
usnvc_source_items = [
    {
        "version": 2.02,
        "id": "5aa827a2e4b0b1c392ef337a",
        "source_title": "Source Data",
        "file_name": "NVC v2.03 2019-03.db"
    },
    {
        "version": 2.03,
        "id": "5cb74a8ae4b0c3b0065d7b2d",
        "source_title": "Source Data",
        "file_name": "NVC v2.03 2019-03.db"
    }
]

# default to latest version
version = usnvc_source_items[len(usnvc_source_items) - 1]
path = './'


def main():
    get_sb_item(version)
    for element_global_id in pyusnvc.usnvc.all_keys(path + version['file_name']):
        cache_unit(element_global_id, cache_path='./cache',
                   file_name=path + version['file_name'])


def get_sb_item(version_config):
    """
    Gets data file from the ScienceBase Item,
    checking to see if the file already exists in the local path, and downloading/unzipping if necessary.
    :param version_config: what sb item to get
    """

    if version_config is None:
        return None

    if os.path.exists(f'{version_config["id"]}.json'):
        with open(f'{version_config["id"]}.json', 'r') as f:
            source_item = json.load(f)
            f.close()
    else:
        sb = SbSession()
        source_item = sb.get_item(version_config["id"])
        with open(f'{version_config["id"]}.json', 'w') as f:
            json.dump(source_item, f)
            f.close()

    source_sb_file = next(
        (f for f in source_item["files"] if f["title"] == version_config["source_title"]), None)

    if source_sb_file is None:
        return None

    probable_file_name = source_sb_file["name"].replace(".zip", ".db")
    if os.path.exists(probable_file_name):
        return

    zip_file = sb.download_file(
        source_sb_file["url"],
        source_sb_file["name"]
    )

    with ZipFile(zip_file, 'r') as zip_ref:
        db_file = zip_ref.namelist()[0]
        zip_ref.extractall()
        zip_ref.close()


def cache_unit(element_global_id, cache_path=None, file_name=None, version_number=None):
    """
    Builds and caches a USNVC unit to a JSON document at a specified path.

    :param element_global_id: Integer element_global_id value to build the unit from.
    :param cache_path: accessible storage path
    :file_name: location of source data
    :return: None if the file already exists or True if the document is created and successfully cached
    """
    if cache_path and os.path.exists(f'{cache_path}/{element_global_id}.json'):
        return None

    if cache_path and not os.path.exists(cache_path):
        os.makedirs(cache_path)

    if version_number == None:
        version_number = version['version']

    unit_doc = pyusnvc.usnvc.build_unit(
        element_global_id, file_name, version_number)

    if cache_path:
        print(f'creating {cache_path}/{element_global_id}.json')
        with open(f'{cache_path}/{element_global_id}.json', 'w') as f:
            f.write(json.dumps(unit_doc, indent=4))
            f.close()
        return True
    return json.dumps(unit_doc, indent=4)


if __name__ == "__main__":
    main()
