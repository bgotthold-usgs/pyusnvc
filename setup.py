from setuptools import setup
import glob

setup(
    name='pyusnvc',
    version='0.0.4',
    description='Processing codes for building the USNVC distribution',
    url='http://github.com/usgs-bcb/pyusnvc',
    author='R. Sky Bristol',
    author_email='bcb@usgs.gov',
    license='unlicense',
    packages=['pyusnvc'],
    data_files=[('pyusnvc', glob.glob('pyusnvc/resources/*') + glob.glob('pyusnvc/*.py'))],
    include_package_data=True,
    install_requires=[
        'pandas',
        'numpy',
        'sciencebasepy',
        'pycountry',
        'genson',
    ],
    zip_safe=False
)
