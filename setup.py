#!/usr/bin/env python
# encoding: utf-8
#
# This file is part of es-cli.
# Copyright (C) 2017 CERN.
#
# es-cli is free software; you can redistribute it
# and/or modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation; either version 2 of the
# License, or (at your option) any later version.
#
# es-cli is distributed in the hope that it will be
# useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with es-cli; if not, write to the
# Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston,
# MA 02111-1307, USA.
#
# In applying this license, CERN does not
# waive the privileges and immunities granted to it by virtue of its status
# as an Intergovernmental Organization or submit itself to any jurisdiction.
from setuptools import setup, find_packages


URL = 'https://github.com/inspirehep/es-cli'


def do_setup(url=URL):
    setup(
        author='CERN',
        author_email='admin@inspirehep.net',
        description='Small Elasticsearch management cli',
        install_requires=[
            'autosemver',
            'click',
            'elasticsearch',
            'six',
        ],
        license='GPLv2',
        name='es-cli',
        package_data={'': ['CHANGELOG', 'AUTHORS']},
        packages=find_packages(),
        setup_requires=['autosemver'],
        zip_safe=False,
        url=URL,
        classifiers=[
            'Intended Audience :: Developers',
            'License :: OSI Approved :: GNU General Public License v2 (GPLv2)',
            'Operating System :: OS Independent',
            'Programming Language :: Python',
            'Topic :: Software Development :: Libraries :: Python Modules',
            'Programming Language :: Python :: 2',
            'Programming Language :: Python :: 2.7',
            'Development Status :: 4 - Beta',
        ],
        entry_points={
            'console_scripts': ['es-cli=es_cli.cli:main'],
        },
        autosemver={
            'bugtracker_url': URL + '/issues/',
        },
    )


if __name__ == '__main__':
    do_setup()
