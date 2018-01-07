from setuptools import setup
from os import path
from pipenv.project import Project
from pipenv.utils import convert_deps_to_pip
from pynndb import __version__

here = path.abspath(path.dirname(__file__))
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

pfile = Project(chdir=False).parsed_pipfile
requirements = convert_deps_to_pip(pfile['packages'], r=False)
test_requirements = convert_deps_to_pip(pfile['dev-packages'], r=False)

setup(
    name='pynndb',
    version=__version__,
    packages=['pynndb'],
    url='https://github.com/oddjobz/pynndb',
    license='MIT',
    author='Gareth Bult',
    author_email='oddjobz@linux.co.uk',
    description='Database library for Python based on LMDB storage engine',
    long_description=long_description,
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 3 - Alpha',
        # Indicate who your project is intended for
        'Intended Audience :: Developers',
        'Topic :: Database :: Database Engines/Servers',
        # Pick your license as you wish (should match "license" above)
         'License :: OSI Approved :: MIT License',
        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 3.6',
    ],
    keywords=['pynndb', 'database', 'LMDB', 'python', 'ORM'],
    install_required=requirements,
    test_requires=test_requirements
)
