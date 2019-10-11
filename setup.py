from setuptools import setup

setup(
    name='pyusnvc',
    version='0.0.1',
    description='Processing codes for building the USNVC distribution',
    url='http://github.com/usgs-bcb/pyusnvc',
    author='R. Sky Bristol',
    author_email='bcb@usgs.gov',
    license='unlicense',
    packages=['pyusnvc'],
    install_requires=[
        'pandas',
        'sciencebasepy',
        'pycountry'
    ],
    zip_safe=False
)
