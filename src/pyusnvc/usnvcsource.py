# -*- coding: utf-8 -*-
"""
This is a skeleton file that can serve as a starting point for a Python
console script. To run this script uncomment the following lines in the
[options.entry_points] section in setup.cfg:

    console_scripts =
         fibonacci = pyusnvc.skeleton:run

Then run `python setup.py install` which will install the command `fibonacci`
inside your current environment.
Besides console scripts, the header (i.e. until _logger...) of this file can
also be used as template for Python modules.

Note: This skeleton file can be safely removed if not needed!
"""

import argparse
import sys
import logging
import os
import pandas as pd

from pyusnvc import __version__

__author__ = "sbristol@usgs.gov"
__copyright__ = "sbristol@usgs.gov"
__license__ = "unlicense"

_logger = logging.getLogger(__name__)


def preprocess_usnvc(path):

    response = {
        'unitXSimilarUnit': None,
        'nvcsDistribution': None,
        'usfsEcoregionDistribution1994': None,
        'usfsEcoregionDistribution2007': None,
        'unitPredecessors': None,
        'obsoleteUnits': None,
        'obsoleteParents': None,
        'unitReferences': None,
        'nvcsUnits': None
    }

    path = path + ''
    processFiles = {}
    for root, d_names, f_names in os.walk(path):
        for f in f_names:
            if f.endswith(".txt"):
                processFiles[f] = os.path.join(root, f)

    # Unit Attributes, Hierarchy, and Descriptions
    # The following code block merges the unit and unit description tables into one
    #  dataframe that serves as the core data for processing.
    units = pd.read_csv(processFiles["unit.txt"], sep='\t', encoding="ISO-8859-1", dtype={
                        "element_global_id": str, "parent_id": str, "classif_confidence_id": int})
    unitDescriptions = pd.read_csv(
        processFiles["unitDescription.txt"], sep='\t', encoding="ISO-8859-1", dtype={"element_global_id": str})
    codes_classificationConfidence = pd.read_csv(
        processFiles["d_classif_confidence.txt"], sep='\t', encoding="ISO-8859-1", dtype={"D_CLASSIF_CONFIDENCE_ID": int})
    codes_classificationConfidence.rename(
        columns={'D_CLASSIF_CONFIDENCE_ID': 'classif_confidence_id'}, inplace=True)
    response['nvcsUnits'] = pd.merge(units, unitDescriptions,
                         how='left', on='element_global_id')
    response['nvcsUnits'] = pd.merge(response['nvcsUnits'], codes_classificationConfidence,
                         how='left', on='classif_confidence_id')
    del units
    del unitDescriptions
    del codes_classificationConfidence

    # Unit References
    # The following dataframes assemble the unit by unit references into a merged
    #  dataframe for later query and processing when building the unit documents.
    unitByReference = pd.read_csv(processFiles["UnitXReference.txt"], sep='\t',
                                  encoding="ISO-8859-1", dtype={"element_global_id": str, "reference_id": str})
    references = pd.read_csv(
        processFiles["reference.txt"], sep='\t', encoding="ISO-8859-1", dtype={"reference_id": str})
    response['unitReferences'] = pd.merge(left=unitByReference, right=references,
                              left_on='reference_id', right_on='reference_id')
    del unitByReference
    del references

    # Unit Predecessors
    # The following codeblock retrieves the unit predecessors for processing.
    response['unitPredecessors'] = pd.read_csv(processFiles["unitPredecessor.txt"], sep='\t',
                                   encoding="ISO-8859-1", dtype={"element_global_id": str, "predecessor_id": str})

    # Obsolete records
    # The following codeblock retrieves the two tables that contain references to
    #  obsolete units or names. We may want to examine this in future versions to
    #  move from simply capturing notes about obsolescence to keeping track of what
    #  is actually changing. Alternatively, we can keep with a whole dataset
    #  versioning construct if that works better for the community, but as soon as
    #  we start minting individual DOIs for the units, making them citable, that
    #  changes the dynamic in how we manage the data moving forward.
    response['obsoleteUnits'] = pd.read_csv(processFiles["unitObsoleteName.txt"],
                                sep='\t', encoding="ISO-8859-1", dtype={"element_global_id": str})
    response['obsoleteParents'] = pd.read_csv(
        processFiles["unitObsoleteParent.txt"], sep='\t', encoding="ISO-8859-1", dtype={"element_global_id": str})

    # Unit Distribution - Nations and Subnations
    # The following codeblock assembles the four tables that make up all the code
    #  references for the unit by unit distribution at the national level and then
    #  in North American states and provinces. I played around with adding a little
    #  bit of value to the nations structure by looking up names and setting up
    #  objects that contain name, abbreviation, uncertainty (true/false), and an
    #  info API reference. But I also kept the original raw string/list of national
    #  abbreviations. That process would be a lot smarter if I did it here by pulling
    #  together a distinct list of all referenced nation codes/abbreviations and then
    #  building a lookup dataframe on those. I'll revisit at some point or if the
    #  code bogs down, but the REST API call is pretty quick.
    unitXSubnation = pd.read_csv(
        processFiles["UnitXSubnation.txt"], sep='\t', encoding="ISO-8859-1", dtype=str)
    codes_CurrentPresAbs = pd.read_csv(
        processFiles["d_curr_presence_absence.txt"], sep='\t', encoding="ISO-8859-1", dtype=str)
    codes_DistConfidence = pd.read_csv(
        processFiles["d_dist_confidence.txt"], sep='\t', encoding="ISO-8859-1", dtype=str)
    codes_Subnations = pd.read_csv(
        processFiles["d_subnation.txt"], sep='\t', encoding="ISO-8859-1", dtype=str)
    response['nvcsDistribution'] = pd.merge(left=unitXSubnation, right=codes_CurrentPresAbs,
                                left_on='d_curr_presence_absence_id', right_on='D_CURR_PRESENCE_ABSENCE_ID')
    response['nvcsDistribution'] = pd.merge(left=response['nvcsDistribution'], right=codes_DistConfidence,
                                left_on='d_dist_confidence_id', right_on='D_DIST_CONFIDENCE_ID')
    response['nvcsDistribution'] = pd.merge(left=response['nvcsDistribution'], right=codes_Subnations,
                                left_on='subnation_id', right_on='subnation_id')
    del unitXSubnation
    del codes_CurrentPresAbs
    del codes_DistConfidence
    del codes_Subnations

    # USFS Ecoregions
    # There is a coded list of USFS Ecoregion information in the unit descriptions,
    #  but this would have to be parsed and referenced out anyway and the base
    #  information seems to come through a "unitX..." set of tables. This codeblock
    #  sets those data up for processing.
    unitXUSFSEcoregion1994 = pd.read_csv(
        processFiles["UnitXEcoregionUsfs1994.txt"], sep='\t', encoding="ISO-8859-1", dtype=str)
    codes_USFSEcoregions1994 = pd.read_csv(
        processFiles["d_usfs_ecoregion1994.txt"], sep='\t', encoding="ISO-8859-1", dtype=str)
    unitXUSFSEcoregion2007 = pd.read_csv(
        processFiles["UnitXEcoregionUsfs2007.txt"], sep='\t', encoding="ISO-8859-1", dtype=str)
    codes_USFSEcoregions2007 = pd.read_csv(
        processFiles["d_usfs_ecoregion2007.txt"], sep='\t', encoding="ISO-8859-1", dtype=str)
    codes_OccurrenceStatus = pd.read_csv(
        processFiles["d_occurrence_status.txt"], sep='\t', encoding="ISO-8859-1", dtype=str)
    response['usfsEcoregionDistribution1994'] = pd.merge(
        left=unitXUSFSEcoregion1994, right=codes_USFSEcoregions1994, left_on='usfs_ecoregion_id', right_on='USFS_ECOREGION_ID')
    response['usfsEcoregionDistribution1994'] = pd.merge(left=response['usfsEcoregionDistribution1994'], right=codes_OccurrenceStatus,
                                             left_on='d_occurrence_status_id', right_on='D_OCCURRENCE_STATUS_ID')
    response['usfsEcoregionDistribution2007'] = pd.merge(
        left=unitXUSFSEcoregion2007, right=codes_USFSEcoregions2007, left_on='usfs_ecoregion_2007_id', right_on='usfs_ecoregion_2007_id')
    response['usfsEcoregionDistribution2007'] = pd.merge(left=response['usfsEcoregionDistribution2007'], right=codes_OccurrenceStatus,
                                             left_on='d_occurrence_status_id', right_on='D_OCCURRENCE_STATUS_ID')
    del unitXUSFSEcoregion1994
    del codes_USFSEcoregions1994
    del unitXUSFSEcoregion2007
    del codes_USFSEcoregions2007
    del codes_OccurrenceStatus

    # Similar Units
    # The similar units table has references to units that are similar to another
    #  with specific notes recorded by the editors.
    response['unitXSimilarUnit'] = pd.read_csv(
        processFiles["UnitXSimilarUnit.txt"], sep='\t', encoding="ISO-8859-1", dtype=str)

    return response


def parse_args(args):
    """Parse command line parameters

    Args:
      args ([str]): command line parameters as list of strings

    Returns:
      :obj:`argparse.Namespace`: command line parameters namespace
    """
    parser = argparse.ArgumentParser(
        description="USNVC data processor")
    parser.add_argument(
        "--version",
        action="version",
        version="pyusnvc {ver}".format(ver=__version__))
    parser.add_argument(
        dest="path",
        help="File system path to files to process",
        type=str,
        metavar="STR")
    parser.add_argument(
        "-v",
        "--verbose",
        dest="loglevel",
        help="set loglevel to INFO",
        action="store_const",
        const=logging.INFO)
    parser.add_argument(
        "-vv",
        "--very-verbose",
        dest="loglevel",
        help="set loglevel to DEBUG",
        action="store_const",
        const=logging.DEBUG)
    return parser.parse_args(args)


def setup_logging(loglevel):
    """Setup basic logging

    Args:
      loglevel (int): minimum loglevel for emitting messages
    """
    logformat = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
    logging.basicConfig(level=loglevel, stream=sys.stdout,
                        format=logformat, datefmt="%Y-%m-%d %H:%M:%S")


def main(args):
    """Main entry point allowing external calls

    Args:
      args ([str]): command line parameter list
    """
    args = parse_args(args)
    setup_logging(args.loglevel)
    _logger.debug("Starting USNVC source file processing...")
    preprocess_usnvc(args.path)
    _logger.info("Script ends here")


def run():
    """Entry point for console_scripts
    """
    main(sys.argv[1:])


if __name__ == "__main__":
    run()
