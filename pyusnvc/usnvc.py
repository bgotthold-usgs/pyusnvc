import pandas as pd
import sqlite3
import os
from zipfile import ZipFile
from sciencebasepy import SbSession
from datetime import datetime
import pycountry
import json
import numpy
import math
import copy
from genson import SchemaBuilder

"""
This script is the core usnvc package. Starting with sourcedata from ScienceBase,
these functions extract transform and load the data into a human readable form.
"""

def get_source_item(version=2.03, force=False):
    """
    Checks the local path for the ScienceBase Source item (cached as a JSON document) and updates as necessary.
    Retrieves the ScienceBase Item and caches it as a document. Retrieves the data file from the ScienceBase Item, 
    checking to see if the file already exists in the local path, and downloading/unzipping if necessary.
 
    :param version: Which numbered version of the USNVC source to retrieve
    :param force: Set to True to force cache of ScienceBase source info regardless of pre-existence
    """
    # 2 versions in ScienceBase at the moment. Need a way to consistently query ScienceBase for versions of source.
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

    version_config = next((i for i in usnvc_source_items if i["version"] == version), None)

    if version_config is None:
        raise ValueError("Version number does not correspond to a valid source item")

    source_item_metadata_file = f'{version_config["id"]}.json'

    if os.path.exists(f'{version_config["id"]}.json') and not force:
        with open(source_item_metadata_file, 'r') as f:
            source_item_metadata = json.load(f)
            f.close()
    else:
        sb = SbSession()
        source_item_metadata = sb.get_item(version_config["id"])
        with open(source_item_metadata_file, 'w') as f:
            json.dump(source_item_metadata, f)
            f.close()

    source_sb_file = next(
        (f for f in source_item_metadata["files"] if f["title"] == version_config["source_title"]), None)

    if source_sb_file is None:
        raise ValueError("The source item metadata contained no reference to source data file")

    # This is a clunky way of handling identification of the source data file
    source_data_file = source_sb_file["name"].replace(".zip", ".db")

    if not os.path.exists(source_data_file) or force:
        zip_file = sb.download_file(
            source_sb_file["url"],
            source_sb_file["name"]
        )

        with ZipFile(zip_file, 'r') as zip_ref:
            # Assumes only one file. Need to revisit this.
            source_data_file = zip_ref.namelist()[0]
            zip_ref.extractall()
            zip_ref.close()
    
    return {
        "source_item_metadata_filename": source_item_metadata_file, 
        "source_data_filename": source_data_file
    }


def db_connection(source_data_filename):
    try:
        return sqlite3.connect(source_data_filename)
    except:
        return None


def clean_string(text):
    """
    Function for basic cleaning of cruft from strings.

    :param text: string text
    :return: cleaned string
    """
    replacements = {'&amp;': '&', '&lt;': '<', '&gt;': '>'}
    for x, y in replacements.items():
        text = text.replace(x, y)
    return text


def get_place_code_data(abbreviation, uncertainty=False):
    """
    Takes an abbreviation for a 2 character country code and uses the pycountry package to return the full name.

    :param abbreviation: Two-character country code
    :param uncertainty: True/False value indicating if the country code for distribution is uncertain
    :return: Data object used to enhance the original list of countries for NVCS distribution
    """
    code_data = {
        "Abbreviation": abbreviation,
        "Uncertainty": uncertainty,
        "Name": "Unknown"
    }

    country_info = pycountry.countries.get(alpha_2=abbreviation)
    if country_info is not None:
        code_data["Name"] = country_info.name

    return code_data


def all_keys(source_data_filename):
    """
    Pulls together a list of all element_global_id keys from the USNVC source. This can be used to set up a message
    queue with all of the items to be processed.

    :param source_data_filename: location of source data
    :return: List of all element_global_id values in the Unit table of the SQLite database
    """
    db = db_connection(source_data_filename)

    identifiers = pd.read_sql_query(
        "SELECT element_global_id FROM Unit",
        db
    )

    return identifiers["element_global_id"].tolist()


def logical_nvcs_root(source_data_filename):
    """
    Creates a logical root document with _id 0 for the root of the USNVC.

    ::param source_data_filename: location of source data
    :return: Dictionary with the bare minimum properties necessary to establish the root.
    """
    db = db_connection(source_data_filename)

    classes = pd.read_sql_query(
        "SELECT element_global_id FROM Unit WHERE PARENT_ID IS NULL",
        db
    )

    return {
        "_id": int(0),
        "title": "US National Vegetation Classification",
        "parent": None,
        "ancestors": None,
        "children": classes["element_global_id"].tolist(),
        "Hierarchy": {
            "unitSort": str(0)
        }
    }


def build_hierarchy(element_global_id, source_data_filename):
    """
    This function builds the hierarchy immediately above and below a given Unit.

    :param element_global_id: Integer element_global_id value to build the hierarchy around.
    ::param source_data_filename: location of source data
    :return: List of dictionaries containing the basic identification information for ancestors all the way up the
    hierarchy, the unit for the provided element_global_id, and immediate children of the unit in the hierarchy
    """
    db = db_connection(source_data_filename)

    full_hierarchy = list()

    this_unit = pd.read_sql_query(
        f"SELECT element_global_id, PARENT_ID, hierarchylevel, classificationCode,\
        databaseCode, translatedName, colloquialName, unitsort\
        FROM Unit\
        WHERE element_global_id = {element_global_id}",
        db
    )
    full_hierarchy.extend(this_unit.to_dict("records"))

    immediate_children = pd.read_sql_query(
        f"SELECT element_global_id, hierarchylevel, classificationCode,\
        databaseCode, translatedName, colloquialName, unitsort\
        FROM Unit\
        WHERE PARENT_ID = {element_global_id}",
        db
    )
    full_hierarchy.extend(immediate_children.to_dict("records"))

    parent_id = this_unit.iloc[0]["PARENT_ID"]

    ancestors = []
    while parent_id is not None:
        ancestor = pd.read_sql_query(
            f"SELECT element_global_id, PARENT_ID, hierarchylevel, classificationCode,\
            databaseCode, translatedName, colloquialName, unitsort\
            FROM Unit\
            WHERE element_global_id = {parent_id}",
            db
        )
        if len(ancestor.index) > 0:
            ancestors.append(ancestor.to_dict("records")[0])
            parent_id = ancestor.iloc[0]["PARENT_ID"]
        else:
            parent_id = None
    full_hierarchy.extend(ancestors)

    hierarchy_list = list()
    for unit in full_hierarchy:
        if unit["hierarchyLevel"] in ["Class", "Subclass", "Formation", "Division"]:
            unit["Display Title"] = f'{unit["classificationCode"]} {unit["colloquialName"]} {unit["hierarchyLevel"]}'
        elif unit["hierarchyLevel"] in ["Macrogroup", "Group"]:
            unit["Display Title"] = f'{unit["classificationCode"]} {unit["translatedName"]}'
        else:
            unit["Display Title"] = f'{unit["databaseCode"]} {unit["translatedName"]}'
        hierarchy_list.append(unit)

    return {
        "Children": list(map(int, immediate_children["element_global_id"].tolist())),
        "Hierarchy": hierarchy_list,
        "Ancestors": list(map(int, [a["element_global_id"] for a in ancestors]))
    }


def build_unit(element_global_id, source_data_filename, version_number, change_log_function=None):
    """
    Main function that builds a given Unit from all the related data tables in the relational database as a single
    document for adding to a document database or indexing system. This function is designed to be run in a
    multi-processing mode against a list of IDs or set of messages in a queue.

    :param element_global_id: Integer element_global_id value to build the unit from.
    :param source_data_filename: location of source data
    :param version_number: do some specific processing based on version
    :param change_log_function: Optional function to log document providence 
    :return: Dictionary object containing a logical set of high level properties patterned after the current online
    "USNVC Explorer" application. The structure is designed to provide a logical and human-readable view of the
    core information for a given unit.
    """
    db = db_connection(source_data_filename)

    # Get requested unit by element_global_id
    this_unit = pd.read_sql_query(
        f"SELECT * FROM Unit \
        LEFT OUTER JOIN UnitDescription \
        ON Unit.element_global_id = UnitDescription.ELEMENT_GLOBAL_ID \
        LEFT OUTER JOIN d_classif_confidence \
        ON UnitDescription.classif_confidence_id = d_classif_confidence.D_CLASSIF_CONFIDENCE_ID \
        WHERE Unit.element_global_id = {element_global_id}",
        db
    ).iloc[0]

    # unitDoc template and initial properties
    previous_unitDoc = {}
    unitDoc = {
        "Date Processed": datetime.utcnow().isoformat(),
        "Identifiers": {
            "element_global_id": element_global_id,
            "Database Code": this_unit["databaseCode"],
            "Classification Code": this_unit["classificationCode"]
        },
        "Overview": {
            "Scientific Name": this_unit["scientificName"],
            "Formatted Scientific Name": clean_string(this_unit["formattedScientificName"]),
            "Translated Name": this_unit["translatedName"]
        },
        "Vegetation": {},
        "Environment": {},
        "Distribution": {},
        "Plot Sampling and Analysis": {},
        "Confidence Level": {},
        "Conservation Status": {},
        "Hierarchy": {},
        "Concept History": {},
        "Synonymy": {},
        "State Crosswalk": {},
        "Authorship": {},
        "References": []
    }

    if change_log_function:
        change_log_function(str(element_global_id), 'pyusnvc/usnvc.py', 'build_unit',
                            'Create', 'Create base usnvc unit doc',
                            previous_unitDoc, unitDoc)
        previous_unitDoc = copy.deepcopy(unitDoc)

    if type(this_unit["colloquialName"]) is str:
        unitDoc["Overview"]["Colloquial Name"] = this_unit["colloquialName"]
    if type(this_unit["typeConceptSentence"]) is str:
        unitDoc["Overview"]["Type Concept Sentence"] = clean_string(
            this_unit["typeConceptSentence"])
    if type(this_unit["typeConcept"]) is str:
        unitDoc["Overview"]["Type Concept"] = clean_string(
            this_unit["typeConcept"])
    if type(this_unit["diagnosticCharacteristics"]) is str:
        unitDoc["Overview"]["Diagnostic Characteristics"] = clean_string(
            this_unit["diagnosticCharacteristics"])
    if type(this_unit["Rationale"]) is str:
        unitDoc["Overview"]["Rationale for Nominal Species or Physiognomic Features"] = clean_string(
            this_unit["Rationale"])
    if type(this_unit["classificationComments"]) is str:
        unitDoc["Overview"]["Classification Comments"] = clean_string(
            this_unit["classificationComments"])
    if type(this_unit["otherComments"]) is str:
        unitDoc["Overview"]["Other Comments"] = clean_string(
            this_unit["otherComments"])

    if type(this_unit["similarNVCtypesComments"]) is str:
        unitDoc["Overview"]["Similar NVC Type Comments"] = clean_string(
            this_unit["similarNVCtypesComments"])
    
    if change_log_function:
        change_log_function(str(element_global_id), 'pyusnvc/usnvc.py', 'build_unit',
                            'Add data', 'Add basic data to existing usnvc unit doc',
                            previous_unitDoc, unitDoc)
        previous_unitDoc = copy.deepcopy(unitDoc)

    thisSimilarUnits = pd.read_sql_query(
        f"SELECT * FROM UnitXSimilarUnit WHERE ELEMENT_GLOBAL_ID = {element_global_id}",
        db
    )
    if len(thisSimilarUnits.index) > 0:
        d_thisSimilarUnits = thisSimilarUnits.to_dict("records")
        for d in d_thisSimilarUnits:
            d.update((k, int(v))
                     for k, v in d.items() if isinstance(v, numpy.int64))
        for d in d_thisSimilarUnits:
            d.update((k, None) for k, v in d.items()
                     if isinstance(v, float) and math.isnan(v))
        unitDoc["Overview"]["Similar NVC Types"] = d_thisSimilarUnits

    if this_unit["hierarchyLevel"] in ["Class", "Subclass", "Formation", "Division"]:
        unitDoc["Overview"]["Display Title"] = this_unit["classificationCode"] + " " + this_unit[
            "colloquialName"] + " " + this_unit["hierarchyLevel"]
    elif this_unit["hierarchyLevel"] in ["Macrogroup", "Group"]:
        unitDoc["Overview"]["Display Title"] = this_unit["classificationCode"] + \
            " " + this_unit["translatedName"]
    else:
        unitDoc["Overview"]["Display Title"] = this_unit["databaseCode"] + \
            " " + this_unit["translatedName"]

    unitDoc["title"] = unitDoc["Overview"]["Display Title"]

    if type(this_unit["Physiognomy"]) is str:
        unitDoc["Vegetation"]["Physiognomy and Structure"] = clean_string(
            this_unit["Physiognomy"])
    if type(this_unit["Floristics"]) is str:
        unitDoc["Vegetation"]["Floristics"] = clean_string(
            this_unit["Floristics"])
    if type(this_unit["Dynamics"]) is str:
        unitDoc["Vegetation"]["Dynamics"] = clean_string(this_unit["Dynamics"])

    if type(this_unit["Environment"]) is str:
        unitDoc["Environment"]["Environmental Description"] = clean_string(
            this_unit["Environment"])

    if type(this_unit["spatialPattern"]) is str:
        unitDoc["Environment"]["Spatial Pattern"] = clean_string(
            this_unit["spatialPattern"])

    if type(this_unit["Range"]) is str:
        unitDoc["Distribution"]["Geographic Range"] = this_unit["Range"]

    if type(this_unit["Nations"]) is str:
        unitDoc["Distribution"]["Nations"] = {
            "Raw List": this_unit["Nations"], "Nation Info": []}
        for nation in this_unit["Nations"].split(","):
            if nation.endswith("?"):
                placeCodeUncertainty = True
            else:
                placeCodeUncertainty = False

            unitDoc["Distribution"]["Nations"]["Nation Info"].append(
                get_place_code_data(nation, placeCodeUncertainty))

    if type(this_unit["Subnations"]) is str:
        unitDoc["Distribution"]["Subnations"] = {
            "Raw List": this_unit["Subnations"]}

    thisDistribution = pd.read_sql_query(
        f"SELECT curr_presence_absence_desc, curr_presence_absence_cd, dist_confidence_cd, dist_confidence_desc,\
        ISO_Nation_cd, Subnation_cd, Subnation_name\
        FROM UnitXSubnation\
        JOIN d_curr_presence_absence\
        ON UnitXSubnation.d_curr_presence_absence_id = d_curr_presence_absence.d_curr_presence_absence_id\
        JOIN d_dist_confidence\
        ON UnitXSubnation.d_dist_confidence_id = d_dist_confidence.d_dist_confidence_id\
        JOIN d_subnation\
        ON UnitXSubnation.SUBNATION_ID = d_subnation.Subnation_id\
        WHERE UnitXSubnation.ELEMENT_GLOBAL_ID = {element_global_id}",
        db
    )
    if len(thisDistribution.index) > 0:
        unitDoc["Distribution"]["States/Provinces Raw Data"] = thisDistribution.to_dict(
            "records")

    if version_number == 2.02:
        thisUSFSDistribution1994 = pd.read_sql_query(
            f"SELECT usfs_ecoregion_name, usfs_ecoregion_class_cd, usfs_ecoregion_concat_cd,\
            occurrence_status_cd, occurrence_status_desc, display_value\
            FROM UnitXEcoregionUsfs1994\
            JOIN d_usfs_ecoregion1994\
            ON UnitXEcoregionUsfs1994.usfs_ecoregion_id = d_usfs_ecoregion1994.usfs_ecoregion_id\
            JOIN d_occurrence_status\
            ON UnitXEcoregionUsfs1994.d_occurrence_status_id = d_occurrence_status.d_occurrence_status_id\
            WHERE UnitXEcoregionUsfs1994.element_global_id = {element_global_id}",
            db
        )
        if len(thisUSFSDistribution1994.index) > 0:
            unitDoc["Distribution"]["1994 USFS Ecoregion Raw Data"] = thisUSFSDistribution1994.to_dict(
                "records")

    thisUSFSDistribution2007 = pd.read_sql_query(
        f"SELECT d_usfs_ecoregion2007.*, d_occurrence_status.*\
        FROM UnitXEcoregionUsfs2007\
        JOIN d_usfs_ecoregion2007\
        ON UnitXEcoregionUsfs2007.usfs_ecoregion_2007_id = d_usfs_ecoregion2007.usfs_ecoregion_2007_id\
        JOIN d_occurrence_status\
        ON UnitXEcoregionUsfs2007.d_occurrence_status_id = d_occurrence_status.d_occurrence_status_id\
        WHERE UnitXEcoregionUsfs2007.element_global_id = {element_global_id}",
        db
    )
    if len(thisUSFSDistribution2007.index) > 0:
        unitDoc["Distribution"]["2007 USFS Ecoregion Raw Data"] = thisUSFSDistribution2007.to_dict(
            "records")

    if type(this_unit["tncEcoregions"]) is int:
        unitDoc["Distribution"]["TNC Ecoregions"] = this_unit["tncEcoregions"]

    if type(this_unit["omernikEcoregions"]) is int:
        unitDoc["Distribution"]["Omernik Ecoregions"] = this_unit["omernikEcoregions"]

    if type(this_unit["federalLands"]) is int:
        unitDoc["Distribution"]["Federal Lands"] = this_unit["federalLands"]

    if type(this_unit["plotCount"]) is int:
        unitDoc["Plot Sampling and Analysis"]["Plot Count"] = this_unit["plotCount"]
    if type(this_unit["plotSummary"]) is str:
        unitDoc["Plot Sampling and Analysis"]["Plot Summary"] = this_unit["plotSummary"]
    if type(this_unit["plotTypal"]) is str:
        unitDoc["Plot Sampling and Analysis"]["Plot Type"] = this_unit["plotTypal"]
    if type(this_unit["plotArchived"]) is str:
        unitDoc["Plot Sampling and Analysis"]["Plot Archive"] = this_unit["plotArchived"]
    if type(this_unit["plotConsistency"]) is str:
        unitDoc["Plot Sampling and Analysis"]["Plot Consistency"] = this_unit["plotConsistency"]
    if type(this_unit["plotSize"]) is str:
        unitDoc["Plot Sampling and Analysis"]["Plot Size"] = this_unit["plotSize"]
    if type(this_unit["plotMethods"]) is str:
        unitDoc["Plot Sampling and Analysis"]["Plot Methods"] = this_unit["plotMethods"]

    unitDoc["Confidence Level"]["Confidence Level"] = this_unit["CLASSIF_CONFIDENCE_DESC"]
    if type(this_unit["confidenceComments"]) is str:
        unitDoc["Confidence Level"]["Confidence Level Comments"] = clean_string(
            this_unit["confidenceComments"])

    if type(this_unit["grank"]) is str:
        unitDoc["Conservation Status"]["Global Rank"] = this_unit["grank"]
    if type(this_unit["grankReviewDate"]) is str:
        unitDoc["Conservation Status"]["Global Rank Review Date"] = this_unit["grankReviewDate"]
    if type(this_unit["grankAuthor"]) is str:
        unitDoc["Conservation Status"]["Global Rank Author"] = this_unit["grankAuthor"]
    if type(this_unit["grankReasons"]) is str:
        unitDoc["Conservation Status"]["Global Rank Reasons"] = this_unit["grankReasons"]

    unitDoc["Hierarchy"]["parent_id"] = str(this_unit["PARENT_ID"])
    unitDoc["Hierarchy"]["hierarchyLevel"] = this_unit["hierarchyLevel"]
    unitDoc["Hierarchy"]["d_classification_level_id"] = int(
        this_unit["D_CLASSIFICATION_LEVEL_ID"])
    unitDoc["Hierarchy"]["unitsort"] = this_unit["unitSort"]
    unitDoc["Hierarchy"]["parentkey"] = this_unit["parentKey"]
    unitDoc["Hierarchy"]["parentname"] = this_unit["parentName"]

    try:
        unitDoc["parent"] = int(this_unit["PARENT_ID"])
    except:
        unitDoc["parent"] = int(0)

    if type(this_unit["lineage"]) is str:
        unitDoc["Concept History"]["Concept Lineage"] = this_unit["lineage"]

    for hist_obj in [
        ("UnitPredecessor", "Predecessors Raw Data"),
        ("UnitObsoleteName", "Obsolete Units Raw Data"),
        ("UnitObsoleteParent", "Obsolete Parents Raw Data")
    ]:
        df_hist_data = pd.read_sql_query(
            f"SELECT * FROM {hist_obj[0]} WHERE element_global_id = {element_global_id}",
            db
        )
        if len(df_hist_data.index) > 0:
            unitDoc["Concept History"][hist_obj[1]
                                       ] = df_hist_data.to_dict("records")

    if type(this_unit["Synonymy"]) is str:
        unitDoc["Synonymy"]["Synonymy"] = this_unit["Synonymy"]

    if type(this_unit["primaryConceptSource"]) is str:
        unitDoc["Authorship"]["Concept Author"] = this_unit["primaryConceptSource"]
    if type(this_unit["descriptionAuthor"]) is str:
        unitDoc["Authorship"]["Description Author"] = this_unit["descriptionAuthor"]
    if type(this_unit["Acknowledgements"]) is str:
        unitDoc["Authorship"]["Acknowledgements"] = this_unit["Acknowledgements"]
    if type(this_unit["versionDate"]) is str:
        unitDoc["Authorship"]["Version Date"] = this_unit["versionDate"]

    thisUnitReferences = pd.read_sql_query(
        f"SELECT ShortCitation, FullCitation\
        FROM UnitXReference\
        JOIN Reference\
        ON UnitXReference.reference_id = Reference.reference_id\
        WHERE UnitXReference.element_global_id = {element_global_id}",
        db
    )
    for index, this_unit in thisUnitReferences.iterrows():
        unitDoc["References"].append({
            "Short Citation": this_unit["ShortCitation"],
            "Full Citation": this_unit["FullCitation"]
        })

    this_hierarchy = build_hierarchy(element_global_id, source_data_filename)
    unitDoc["Hierarchy"]["Cached Hierarchy"] = this_hierarchy["Hierarchy"]

    if len(this_hierarchy["Children"]) > 0:
        unitDoc["children"] = this_hierarchy["Children"]

    if len(this_hierarchy["Ancestors"]) > 0:
        unitDoc["ancestors"] = this_hierarchy["Ancestors"]
    else:
        unitDoc["ancestors"] = [int(0)]
    if version_number == 2.03:
        state_crosswalks = pd.read_sql_query(
            f"SELECT * FROM UnitCrosswalk\
            JOIN d_subnation ON\
            UnitCrosswalk.subnation_id = d_subnation.Subnation_id\
            WHERE UnitCrosswalk.element_global_id = {element_global_id}",
            db
        )
        if len(state_crosswalks.index) > 0:
            unitDoc["State Crosswalk"]["Crosswalk Raw Data"] = state_crosswalks.to_dict(
                "records")
            unitDoc["State Crosswalk"]["States Using USNVC Type"] = [
                i["Subnation_cd"] for i in unitDoc["State Crosswalk"]["Crosswalk Raw Data"]
                if i["linkage"] == "1 direct" and i["ISO_Nation_cd"] == "US"
            ]
    if change_log_function:
        change_log_function(str(element_global_id), 'pyusnvc/usnvc.py', 'build_unit',
                            'Finish Unit Doc', 'Finished building usnvc unit doc',
                            previous_unitDoc, unitDoc)
        previous_unitDoc = copy.deepcopy(unitDoc)
    return unitDoc


def get_schema(source_data_filename, cache_file=True, schema_path=None, schema_file=None, force=False):
    """
    Retrieves the schema documentation (JSON Schema) or builds it if it doesn't exist (or forced).
    Schema build process will run through all documents to ensure that we fully sample the dataset.

    :param source_data_filename: Filename of the source data
    :param cache_file: If true, caches the JSON schema as a JSON document
    :param schema_path: Location where the schema file should be cached; defaults to the resources path within the pyusnvc package
    :param schema_file: Filename of the JSON schema file; required if cache_file is true
    :param force: Can force the re-creation of the schema file if necessary
    :return: Returns a Python dictionary object containing the JSON Schema created with the genson package.
    This is a barebones schema with just the basic data structure in place. It needs to be further developed
    with full documentation, but the basic version can be used as a simple data validator.
    """
    if cache_file:
        if schema_file is None:
            raise ValueError("You must supply a schema_file name in order to cache the schema to a file")
        if schema_path is None:
            resources_folder = os.path.join(os.path.dirname(__file__), 'resources')
            schema_path = os.path.join(resources_folder, schema_file)
        else:
            schema_path = os.path.join(schema_path, schema_file)

    if os.path.exists(schema_path) and not force:
        with open(schema_path, 'r') as f:
            schema = json.load(f)
            f.close()
        return schema

    builder = SchemaBuilder()
    builder.add_schema({"type": "object", "properties": {}})
    for element_global_id in all_keys(source_data_filename):
        builder.add_object(
            build_unit(
                element_global_id,
                source_data_filename,
                2.03
            )
        )

    schema = builder.to_schema()

    if cache_file:
        with open(schema_path, 'w') as f:
            json.dump(schema, f)
            f.close()

    return schema
