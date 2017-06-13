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

import copy
import json
import os
import re
import time
from functools import wraps

import click
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import RequestError
from elasticsearch.helpers import reindex, scan
from urllib3.util.timeout import Timeout

from six.moves import urllib

_ERROR1_RE = re.compile(u'mapper \[(?P<field_name>[^]]+)\]')
_BAD_FIELDS_ACK_RESPONSES = {}
_TRY_TO_FIX_RESPONSES = {}


def split_index_url(index_url):
    """Split an index url (complete or not) into the server and index name.
    """
    index_name = urllib.parse.urlparse(index_url).path.rsplit('/')[-1]
    if not index_name:
        raise Exception("No index passed for url %s." % index_url)

    connection_url = index_url[:- (len(index_name) + 1)]

    return (connection_url, index_name)


def with_two_connections(func):
    """Handles the passing of the two from/to index connection urls.

    For example:
        from_index='http://somewhere/index1'
        to_index='index2'

    Or:
        from_index='http://somewhere/index1'
        to_index='http://somewhere.else/index2'

    will be transformed to the params:
        from_cli=Elasticsearch(['http://somewhere'])
        from_index='index1'
        to_cli=Elasticsearch(['http://somewhere'])
        to_index='index2'

    and respectively:
        from_cli=Elasticsearch(['http://somewhere'])
        from_index='index1'
        to_cli=Elasticsearch(['http://somewhere.else'])
        to_index='index2'


    If the hosts are the same for both index connections, it will return the
    same connection object for them.
    """
    @wraps(func)
    def _decorator(*args, **kwargs):
        from_connection = kwargs.get('from_index')
        if not from_connection:
            raise TypeError(
                '%s takes at least a "from_index" argument that was not '
                'passed' % func
            )

        to_connection = kwargs.get('to_index')
        if not to_connection:
            raise TypeError(
                '%s takes at least a "to_index" argument that was not '
                'passed' % func
            )

        from_index, from_connection_url = split_index_url(
            kwargs.get('from_index', ''),
        )
        to_index, to_connection_url = split_index_url(
            kwargs.get('to_index', ''),
        )

        from_cli = Elasticsearch(
            [from_connection_url],
            verify_certs=False,
        )
        if from_connection_url == to_connection_url:
            to_cli = from_cli
        else:
            to_cli = Elasticsearch(
                [to_connection],
                verify_certs=False,
            )

        kwargs.update({
            'from_cli': from_cli,
            'to_cli': to_cli,
            'from_index': from_index,
            'to_index': to_index,
        })

        return func(*args, **kwargs)

    return _decorator


def save_errors(errors, dst_file_name='errors.json'):
    errors = [
        err['index']
        for err in errors
    ]
    errors = json.dumps(errors)
    with open(dst_file_name, 'w') as errors_fd:
        errors_fd.write(errors)


def _reindex(*args, **kwargs):
    errors_file = kwargs.pop('errors_file', 'errors.json')
    start_time = time.time()
    tot_docs, errors = reindex(*args, **kwargs)
    # reindex return '0' when there are no errors, but a list of errors
    # otherwise, here I just normalize to list
    errors = errors or []
    end_time = time.time()
    click.echo(
        'Reindexed %s docs in %ds' % (tot_docs, end_time - start_time)
    )
    click.echo(
        '%.0f docs per second' % (tot_docs / (end_time - start_time))
    )
    click.echo('Failed docs: %d' % len(errors))
    if errors:
        save_errors(errors, errors_file)
        click.echo(
            'Written errors list in %s file (in case you want '
            'process them later).' % errors_file
        )

    return tot_docs, errors


def _copy_index(
    index_from, index_to, connect_url, chunk, autofix, interactive=True,
):
    cli = Elasticsearch([connect_url], verify_certs=False)
    tot_docs, errors = _reindex(
        client=cli,
        source_index=index_from,
        target_index=index_to,
        query=None,
        target_client=None,
        chunk_size=chunk,
        scroll='5m',
        bulk_kwargs={
            'raise_on_error': False,
            'stats_only': False,
            'params': {
                'request_timeout': Timeout(read=30),
            },
        },
    )


def _get_dump_index_name(dump_dir):
    _, _, files = next(os.walk(dump_dir), (None, None, []))
    first_dump_file = next(
        (fname for fname in files if fname.endswith('-0.json')),
        None,
    )
    if first_dump_file is None:
        raise Exception(
            'No dump file (<index_name>-0.json) found on dump dir "%s"'
            % dump_dir
        )

    index_name = first_dump_file[:-len('-0.json')]
    return index_name


def _get_dump_files(dump_dir, index_name):
    _, _, files = next(os.walk(dump_dir), (None, None, []))
    dump_file_match = re.compile(index_name + '-\d+.json')
    dump_files = [
        os.path.join(dump_dir, fname) for fname
        in files
        if dump_file_match.match(fname)
    ]
    dump_files.sort(
        key=lambda x: int(x.rsplit('-', 1)[-1].rsplit('.', 1)[0])
    )
    return dump_files


def _load_index(index, cli, dump_dir='.', with_create=True, yes_all=False):
    click.echo(
        'Loading dump from dir %s into index %s' % (dump_dir, index)
    )
    old_index_name = _get_dump_index_name(dump_dir)

    if with_create:
        index_metadata = json.load(
            open(os.path.join(dump_dir, '%s-metadata.json' % old_index_name))
        )
        try:
            cli.indices.create(
                index=index,
                body=index_metadata[old_index_name],
            )
        except RequestError as err:
            if len(err.args) < 1:
                raise

            if err.args[1] != 'index_already_exists_exception':
                raise

            if not yes_all and not click.confirm(
                'Index %s already exists, do you want me to recreate it?'
                % index
            ):
                raise

            cli.indices.delete(index=index)
            cli.indices.create(
                index=index,
                body=index_metadata[old_index_name],
            )

    dump_fnames = _get_dump_files(dump_dir, old_index_name)
    loaded_docs = 0
    for dump_fname in dump_fnames:
        click.echo('    Loading file %s' % dump_fname)
        with open(dump_fname) as dump_fd:
            loaded_docs += _load_file_to_index(dump_fd, index, cli)

    click.echo('Loaded %d documents' % loaded_docs)


def _load_file_to_index(dump_fd, index, cli):
    """

    TODO: use bulk to batch the loads
    """
    loaded_docs = 0
    for document in dump_fd.readlines():
        document = json.loads(document)
        doc_type = document['_type']
        doc_id = document['_id']
        doc_body = document['_source']
        cli.create(
            index=index,
            id=doc_id,
            doc_type=doc_type,
            body=doc_body,
        )
        loaded_docs += 1

    return loaded_docs


def _dump_index(index_name, cli, batch=1000):
    dump_fname = '%s-metadata.json' % index_name
    click.echo('Dumping index %s info at %s' % (index_name, dump_fname))
    index_info = cli.indices.get(index_name)
    with open(dump_fname, 'w') as dump_fd:
        dump_fd.write(json.dumps(index_info, indent=4))

    click.echo('Dumping %s in batches of %d' % (index_name, batch))
    try:
        dump_file_index = 0
        dumped_docs = 0
        dump_fname = '%s-%d.json' % (index_name, dump_file_index)
        dump_fd = open(dump_fname, 'w')
        click.echo('    Creating file %s' % dump_fname)
        for result in scan(cli, index=index_name, size=batch):
            dump_fd.write(json.dumps(result) + '\n')
            dumped_docs += 1
            if dumped_docs >= batch:
                dump_file_index += 1
                dumped_docs = 0
                dump_fd.close()
                dump_fname = '%s-%d.json' % (index_name, dump_file_index)
                dump_fd = open(dump_fname, 'w')
                click.echo('    Creating file %s' % dump_fname)
    finally:
        dump_fd.close()


def _extract_bad_field(error_str):
    match = _ERROR1_RE.search(error_str)
    if match:
        return match.groupdict().get('field_name')

    raise Exception('Unable to extract bad field from %s' % error_str)


def _fix_bad_field(index, recid, bad_field, cli):
    keys = bad_field.split('.')
    original_record = cli.get(index, recid)
    cur_elem = original_record['_source']
    for key in keys[:-2]:
        cur_elem = cur_elem[key]

    cur_elem.pop(keys[-2])

    return original_record


def _handle_illegal_argument_exception(
    index_from, index_to, cli, recid, error, yesall=False,
):
    field_name = _extract_bad_field(error['caused_by']['reason'])
    if field_name not in _BAD_FIELDS_ACK_RESPONSES:
        if yesall:
            _BAD_FIELDS_ACK_RESPONSES[field_name] = True
        else:
            _BAD_FIELDS_ACK_RESPONSES[field_name] = click.confirm(
                'Do you want me to try to automatically fix bad field %s? I '
                'might delete it' % field_name
            )

    if _BAD_FIELDS_ACK_RESPONSES[field_name]:
        new_record = _fix_bad_field(
            index=index_from,
            recid=recid,
            bad_field=field_name,
            cli=cli,
        )
        cli.index(
            index=index_to,
            doc_type=new_record['_type'],
            body=new_record['_source'],
        )
        click.echo('Fixed record %s' % recid)

    return None


def _try_to_migrate(index_from, index_to, cli, recid, error, yesall=False):
    err_type = error['caused_by']['type']
    if err_type not in _TRY_TO_FIX_RESPONSES:
        if yesall:
            _TRY_TO_FIX_RESPONSES[err_type] = True
        else:
            _TRY_TO_FIX_RESPONSES[err_type] = click.confirm(
                'Record %s has an error type %s, do you want me to try to fix '
                'this kind of errors?' % (recid, err_type)
            )

    if not _TRY_TO_FIX_RESPONSES[err_type]:
        return None

    fn_name = '_handle_' + err_type
    if fn_name not in globals():
        print(
            "I don't know how to handle %s, skipping record %s" % (
                err_type,
                recid,
            )
        )
        return None

    return globals()[fn_name](
        index_from=index_from,
        index_to=index_to,
        cli=cli,
        recid=recid,
        error=error,
        yesall=yesall,
    )


def _merge_mappings(base_mappings, to_merge_mappings):
    merged_mappings = copy.deepcopy(base_mappings)
    overwritten_mappings = []
    for mapping_name, mapping_body in to_merge_mappings.items():
        if mapping_name in base_mappings:
            overwritten_mappings.append(mapping_name)

        merged_mappings[mapping_name] = mapping_body

    return merged_mappings, overwritten_mappings


def _merge_two_index_bodies(base_index_body, to_merge_index_body):
    merged_index_body = copy.deepcopy(base_index_body)
    overwritten_fields = set()
    for key in to_merge_index_body.keys():
        if key == 'mappings':
            (
                merged_index_body['mappings'],
                overwritten_mappings
            ) = _merge_mappings(
                base_index_body.get('mappings', {}),
                to_merge_index_body.get('mappings', {}),
            )
            overwritten_fields = overwritten_fields.union(
                set(
                    'mappings.' + mapping_name
                    for mapping_name in overwritten_mappings
                )
            )
        else:
            if key in base_index_body:
                overwritten_fields.add(key)

            merged_index_body[key] = to_merge_index_body[key]

    return merged_index_body, overwritten_fields


def merge_index_bodies(index_bodies):
    if not index_bodies:
        return {}, set()

    base_index_body = index_bodies[0]

    if len(index_bodies) < 2:
        return copy.deepcopy(base_index_body), set()

    merged_index_body = {}
    for to_merge_index_body in index_bodies[1:]:
        merged_index_body, overwritten_fields = _merge_two_index_bodies(
            base_index_body,
            to_merge_index_body,
        )

    return merged_index_body, overwritten_fields
