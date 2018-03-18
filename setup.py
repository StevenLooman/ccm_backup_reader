import sys

import os.path

from setuptools import setup
from setuptools import find_packages
from setuptools.command.test import test as TestCommand


class PyTest(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = [
            '--strict',
            '--verbose',
            '--tb=long',
            'tests']
        self.test_suite = True

    def run_tests(self):
        import pytest
        errcode = pytest.main(self.test_args)
        sys.exit(errcode)


here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, 'README.rst'), encoding='utf-8') as f:
    LONG_DESCRIPTION = f.read()


REQUIRES=[
]


setup(
    name='ccm_backup_reader',
    version='0.0.1',
    description='IBM Synergy/CCM backup reader',
    long_description=LONG_DESCRIPTION,
    author='Steven Looman',
    author_email='steven.looman@gmail.com',
    packages=['ccm_backup_reader'],
    install_requires=REQUIRES,
    tests_require=['pytest'],
    cmdclass={'test': PyTest},
    scripts=[
        'scripts/ccm_backup_dumper.py',
        'scripts/ccm_backup_to_sqlite.py',
    ]
)
