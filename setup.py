from setuptools import setup

setup(
    name='pyusnvc',
    version='0.0.4',
    description='Processing codes for building the USNVC distribution',
    url='http://github.com/usgs-bcb/pyusnvc',
    author='R. Sky Bristol',
    author_email='bcb@usgs.gov',
    license='unlicense',
    packages=['pyusnvc'],
    data_files=[('pyusnvc', ['pyusnvc/resources/usnvc_unit_schema_2.03.json'])],
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
