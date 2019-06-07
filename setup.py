import os
import codecs
import re
from setuptools import setup

here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

with open('requirements.txt') as io:
    reqs = io.read().splitlines()
with open('requirements-dev.txt') as io:
    test_requirements = io.read().splitlines()

requirements = []
for r in reqs:
    if '#' not in r:
        requirements.append(r)


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
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Database :: Database Engines/Servers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.6',
    ],
    keywords=['pynndb', 'database', 'LMDB', 'python', 'ORM'],
    install_requires=requirements,
    tests_require=test_requirements,
    data_files=[('', ['requirements.txt', 'requirements-dev.txt'])],
    entry_points={
        'console_scripts': [
            'pynndb = pynndb_shell.__init__:main'
        ]
    }
)
