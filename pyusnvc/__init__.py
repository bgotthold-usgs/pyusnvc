# pyusnvc package

import pkg_resources

from . import usnvc

__version__ = pkg_resources.require("pyusnvc")[0].version


def get_package_metadata():
    d = pkg_resources.get_distribution('pyusnvc')
    for i in d._get_metadata(d.PKG_INFO):
        print(i)

