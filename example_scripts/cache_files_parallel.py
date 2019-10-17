"""
This script example uses the joblib package to run the process in parallel over all element_global_id values in the
Units table and caches a file for each unit into a local directory. By default, it places the files into a folder below
the one where you execute the script called 'cache.' You can change this with the cache_path parameter.

Note that you will need to install joblib separately as it is not included as a dependency of the pyusnvc package.

By default, the script will run against the latest version of the USNVC (2.03) in ScienceBase. This can be changed to
run against 2.02 by setting version=2.02 on the cache_unit() function.

The first iteration of the loop will fire the db_connection() function to pull the ScienceBase Item as metadata and the
source database file (SQLite version) and cache those locally. Every subsequent process will check for that file first
and not bother ScienceBase again.
"""

import pyusnvc
from joblib import Parallel, delayed

Parallel(n_jobs=10, prefer="threads")(
    delayed(pyusnvc.usnvc.index_unit)(element_global_id) for element_global_id in pyusnvc.usnvc.all_keys()
)
