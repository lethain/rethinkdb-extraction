from distutils.core import setup

setup(
    name='rethinkdb_extraction',
    version='0.1.0',
    author='Will Larson',
    author_email='lethain@gmail.com',
    packages=['rethinkdb_extraction'],
    url='http://pypi.python.org/pypi/extraction/',
    license='LICENSE.txt',
    description='Example of using RethinkDB with Extraction.',
    long_description=open('README.rst').read(),
    install_requires=[
        "extraction",
        "rethinkdb",
        ],
)
