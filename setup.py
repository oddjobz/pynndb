import os
import codecs
import re
from setuptools import setup
from pipenv.project import Project
from pipenv.utils import convert_deps_to_pip

here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

pfile = Project(chdir=False).parsed_pipfile
requirements = convert_deps_to_pip(pfile['packages'], r=False)
test_requirements = convert_deps_to_pip(pfile['packages'], r=False)
requirements.append('Pipfile')

def read(*parts):
    with codecs.open(os.path.join(here, *parts), 'r') as fp:
        return fp.read()


def find_version(*file_paths):
    version_file = read(*file_paths)
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]", version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")


setup(
    name='pynndb',
    version=find_version("pynndb", "__init__.py"),
    packages=['pynndb', 'pynndb_shell'],
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
        'Development Status :: 4 - Beta',
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
    install_requires=requirements,
    test_requires=test_requirements,
    data_files=[('', ['Pipfile'])],
    entry_points = {
        'console_scripts': [
            'pynndb = pynndb_shell.__init__:main'
        ]
    }
)
