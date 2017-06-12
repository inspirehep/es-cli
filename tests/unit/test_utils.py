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
from __future__ import absolute_import, division, print_function

import pytest

from es_cli import utils


@pytest.mark.parametrize(
    'index_url,expected',
    [
        (
            'https://some:thing@some.host/my/index',
            ('https://some:thing@some.host/my', 'index'),
        ),
        (
            'index',
            ('', 'index'),
        ),
    ],
    ids=[
        'proto, user, pass, host, extra path and index name',
        'only index name',
    ]
)
def test_split_index_url(index_url, expected):
    result = utils.split_index_url(index_url)

    assert result == expected


@pytest.mark.parametrize(
    'indices,expected',
    [
        [
            [],
            ({}, set()),
        ],
        [
            [{'one': '1'}],
            ({'one': '1'}, set()),
        ],
        [
            [{'one': '1'}, {'two': '2'}],
            ({'one': '1', 'two': '2'}, set()),
        ],
        [
            [
                {'mappings': {'one': '1'}},
                {'mappings': {'two': '2'}},
            ],
            ({'mappings': {'one': '1', 'two': '2'}}, set()),
        ],
        [
            [
                {'mappings': {'one': '1', 'two': '2'}},
                {'mappings': {'one': 'new1', 'four': '4'}},
            ],
            (
                {
                    'mappings': {'one': 'new1', 'two': '2', 'four': '4'},
                },
                set(['mappings.one']),
            ),

        ],
        [
            [
                {'settings': {'one': '1', 'two': '2'}},
                {'settings': {'one': 'new1', 'four': '4'}},
            ],
            (
                {
                    'settings': {'one': 'new1', 'four': '4'},
                },
                set(['settings']),
            ),
        ],
    ],
    ids=[
        'merge no bodies return empty body',
        'merge one body, returns a copy of it',
        'merge two non-colliding (top level) indices',
        'merge two non-colliding (first mappings level) indices',
        'merge two colliding (first mappings level) indices',
        'merge two colliding (top level) indices',
    ]
)
def test_positive_merge_index_bodies(indices, expected):
    result = utils.merge_index_bodies(indices)

    assert result == expected
    # make sure we always get a copy, not the actual object
    if indices:
        assert id(result[0]) != id(indices[0])
