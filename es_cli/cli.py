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

import json
import logging
import os
import time

import click
from elasticsearch import Elasticsearch
from urllib3.util.timeout import Timeout

from . import utils

DEFAULT_NODE = ('localhost', 9200)


def _merge_mapping_files(mapping_file_paths):
    mappings = [
        json.loads(open(mapping_fname).read())
        for mapping_fname in mapping_file_paths
    ]
    index_body, overwritten_fields = utils.merge_index_bodies(mappings)
    if overwritten_fields:
        click.confirm(
            (
                'The following fields conflicted while merging the multiple '
                'mappings, will use the latest instance of them:\n    {fields}'
                '\n\nFull merged index body:\n{body}\n'
                'Do you want to continue?'
            ).format(
                fields='\n    '.join(overwritten_fields),
                body='\n    '.join(
                    json.dumps(index_body, indent=4).splitlines(),
                ),
            ),
            abort=True,
        )

    index_body_str = json.dumps(index_body)
    return index_body_str


@click.command()
@click.argument('index_from')
@click.argument('index_to')
@click.argument('recid')
@click.argument('error_type')
@click.argument('error_message')
@click.option(
    '-c',
    '--connect-url',
    help='Server to connect to, in the form http://user:pass@server:port',
    default=DEFAULT_NODE[0],
)
def force_migrate_record(
    index_from, index_to, recid, error_type, error_message, connect_url,
):
    cli = Elasticsearch([connect_url], verify_certs=False)
    error = {
        'caused_by': {
            'type': error_type,
            'reason': error_message,
        }
    }
    utils._try_to_migrate(
        index_from=index_from,
        index_to=index_to,
        cli=cli,
        recid=recid,
        error=error,
        yesall=True,
    )


@click.command()
@click.argument('index_from')
@click.argument('index_to')
@click.option(
    '-c',
    '--connect-url',
    help='Server to connect to, in the form http://user:pass@server:port',
    default=DEFAULT_NODE[0],
)
@click.option(
    '-b',
    '--batch',
    help='Size of the batches to use',
    default=500,
)
@click.option(
    '-a',
    '--autofix',
    help='Try to fix any failed record after copying them over.',
    default=False,
)
def copy_index(index_from, index_to, connect_url, batch, autofix):
    utils._copy_index(index_from, index_to, connect_url, batch, autofix)


@click.command(
    short_help='Dump an index to files on disk.',
    help=(
        'Creates a file dump of the given index to disk. The index passed '
        'must be a full url to it, for example:\n\n'
        '    https://user:pass@my.es/index_name\n\n'
        'The files created will be a set of files named "<index>-N.json" with '
        'the contents of the index, one document per line, and another file '
        '"<index>-metadata.json" with the index metadata info (mapping, '
        'alias, ...).'
    )
)
@click.argument('index_url')
@click.option(
    '-o',
    '--out-dir',
    help='Directory to put the files into, will be created if does not exist.',
    default='.',
)
@click.option(
    '-b',
    '--batch',
    help='Size of the batches to use',
    default=1000,
)
def dump_index(index_url, out_dir, batch):
    start_time = time.time()
    connect_url, index_name = utils.split_index_url(index_url)
    cli = Elasticsearch([connect_url], verify_certs=False)
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    os.chdir(out_dir)
    utils._dump_index(
        index_name=index_name,
        cli=cli,
        batch=batch,
    )
    end_time = time.time()
    click.echo('Finished in %s seconds' % str(end_time - start_time))


@click.command(
    short_help='Load an exported index.',
    help=(
        'Creates a new index from a file dump from disk. The index passed '
        'must be a full url to it, for example:\n\n'
        '    https://user:pass@my.es/index_name\n\n'
        'The files to load should be a set of files named "<index>-N.json" '
        'with the contents of the index, one document per line, and an extra '
        'file "<index>-metadata.json" with the index metadata info (mapping, '
        'alias, ...). That\'s the same format generated by the dump command.'
    )
)
@click.option(
    '-y',
    '--yes-all',
    help='Assume yes to all the questions',
    default=False,
    is_flag=True,
)
@click.argument('index_url')
@click.argument('path_to_dump_dir')
def load_index_dump(yes_all, index_url, path_to_dump_dir):
    start_time = time.time()
    connect_url, index_name = utils.split_index_url(index_url)
    cli = Elasticsearch([connect_url], verify_certs=False)
    utils._load_index(
        index=index_name,
        cli=cli,
        dump_dir=path_to_dump_dir,
        yes_all=yes_all,
    )
    end_time = time.time()
    click.echo('Finished in %s seconds' % str(end_time - start_time))


@click.command()
@click.argument('name')
@click.option(
    '-m',
    '--mapping',
    default=None,
    help='Mapping file for the index, you can repeat it for many.',
    multiple=True,
)
@click.option(
    '-c',
    '--connect-url',
    help='Server to connect to, in the form http://user:pass@server:port',
    default=DEFAULT_NODE[0],
)
def create_index(name, mapping, connect_url):
    cli = Elasticsearch([connect_url], verify_certs=False)

    index_body_str = _merge_mapping_files(mapping_file_paths=mapping)
    cli.indices.create(
        index=name,
        body=index_body_str,
    )


@click.command()
@click.argument('name')
@click.option(
    '-c',
    '--connect-url',
    help='Server to connect to, in the form http://user:pass@server:port',
    default=DEFAULT_NODE[0],
)
def delete_index(name, connect_url):
    cli = Elasticsearch([connect_url], verify_certs=False)
    cli.indices.delete(index=name)


@click.command(
    short_help='Remap an index.',
    help=(
        'Remaps a full index with the given mapping. The index_url must be a '
        'a full url to it, for example:\n\n'
        '    https://user:pass@my.es/index_name\n\n'
        'It will output any errors in the remapping in json files.'
    )
)
@click.option(
    '-m',
    '--mapping',
    default=None,
    help='Mapping file for the index, you can repeat it for many.',
    multiple=True,
)
@click.argument('index_url')
def remap(mapping, index_url):
    connect_url, orig_index = utils.split_index_url(index_url)
    cli = Elasticsearch([connect_url], verify_certs=False)
    tmp_index = 'remapping_tmp_' + orig_index

    aliases = cli.indices.get_alias(
        index=orig_index
    ).get(orig_index, {}).get('aliases', {}).keys()

    index_body_str = _merge_mapping_files(mapping_file_paths=mapping)

    click.echo(
        '(Re)Creating temporary index (mappings), named %s'
        % tmp_index
    )
    cli.indices.delete(index=tmp_index, ignore=[400, 404])
    cli.indices.create(
        index=tmp_index,
        body=index_body_str,
    )

    click.echo(
        'Created temporary index, will start dumping the data from the old '
        'one, this might take some time (~40 docs/sec).'
    )
    click.confirm('Do you want to continue?', abort=True)
    errors_file = 'reindex_%s_errors.json' % tmp_index
    _, errors = utils._reindex(
        errors_file=errors_file,
        client=cli,
        source_index=orig_index,
        target_index=tmp_index,
        query=None,
        target_client=None,
        chunk_size=500,
        scroll='5m',
        bulk_kwargs={
            'params': {
                'request_timeout': Timeout(read=60),
            },
        },
    )
    if errors:
        click.confirm(
            'There were some errors, want to continue?',
            abort=True,
        )

    click.echo(
        'Populated temporary index, will recreate original index (this will '
        'remove it\'s contents).'
    )
    click.confirm('Do you want to continue?', abort=True)
    cli.indices.delete(orig_index)

    click.echo('Adding aliases (if any) to the temporary index.')
    for alias in aliases:
        cli.indices.put_alias(index=tmp_index, name=alias)

    cli.indices.create(
        index=orig_index,
        body=index_body_str,
    )

    click.echo(
        'Recreated original index (mappings), will repopulate '
        'with the data from the temporary one.'
    )
    click.confirm('Do you want to continue?', abort=True)
    errors_file = 'reindex_%s_errors.json' % orig_index
    _, errors = utils._reindex(
        errors_file=errors_file,
        client=cli,
        source_index=tmp_index,
        target_index=orig_index,
        query=None,
        target_client=None,
        chunk_size=500,
        scroll='5m',
        bulk_kwargs={
            'params': {
                'request_timeout': Timeout(read=60),
            },
        },
    )
    if errors:
        click.confirm(
            'There were some errors, want to continue?',
            abort=True,
        )

    click.echo('Original index repopulated, will cleanup the temporary index.')
    click.confirm('Do you want to continue?', abort=True)
    cli.indices.delete(tmp_index)
    click.echo('Restoring alias on the original index.')
    for alias in aliases:
        cli.indices.put_alias(index=orig_index, name=alias)
    click.echo('Done')


@click.group()
def cli_main():
    pass


def main():
    logging.captureWarnings(True)
    cli_main.add_command(create_index)
    cli_main.add_command(copy_index)
    cli_main.add_command(delete_index)
    cli_main.add_command(remap)
    cli_main.add_command(force_migrate_record)
    cli_main.add_command(dump_index)
    cli_main.add_command(load_index_dump)
    cli_main()


if __name__ == '__main__':
    main()
