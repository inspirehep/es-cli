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

import pytest

from es_cli import utils


@pytest.mark.parametrize(
    'index_url,expected',
    [
        (
            'https://some:thing@some.host/my/index',
            ('https://some:thing@some.host/my', 'index'),
        ),
    ],
    ids=[
        'proto, user, pass, host, extra path and index name'
    ]
)
def test_split_index_url(index_url, expected):
    result = utils.split_index_url(index_url)

    assert result == expected
