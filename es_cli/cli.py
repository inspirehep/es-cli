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

import logging
import os
import time
from urllib3.util.timeout import Timeout

import click
from elasticsearch import Elasticsearch

import utils

DEFAULT_NODE = ('localhost', 9200)


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
@click.argument('index_url')
@click.argument('path_to_dump_dir')
def load_index_dump(index_url, path_to_dump_dir):
    start_time = time.time()
    connect_url, index_name = utils.split_index_url(index_url)
    cli = Elasticsearch([connect_url], verify_certs=False)
    utils._load_index(
        index=index_name,
        cli=cli,
        dump_dir=path_to_dump_dir,
    )
    end_time = time.time()
    click.echo('Finished in %s seconds' % str(end_time - start_time))


@click.command()
@click.argument('name')
@click.option(
    '-m',
    '--mapping',
    default=None,
    help='Mapping file for the index.',
)
@click.option(
    '-c',
    '--connect-url',
    help='Server to connect to, in the form http://user:pass@server:port',
    default=DEFAULT_NODE[0],
)
def create_index(name, mapping, connect_url):
    cli = Elasticsearch([connect_url], verify_certs=False)
    if mapping is None:
        body = ''
    else:
        with open(mapping) as mapping_fd:
            body = mapping_fd.read()

    cli.indices.create(
        index=name,
        body=body,
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


@click.command()
@click.argument('name')
@click.option(
    '-m',
    '--mapping',
    default=None,
    help='Mapping file for the index.',
)
@click.option(
    '-c',
    '--connect-url',
    help='Server to connect to, in the form http://user:pass@server:port',
    default=DEFAULT_NODE[0],
)
@click.option(
    '-a',
    '--autofix',
    help='Try to fix any failed record after copying them over.',
    default=False,
)
def remap(name, mapping, connect_url, autofix):
    cli = Elasticsearch([connect_url], verify_certs=False)
    tmp_index = 'remapping_tmp_' + name

    aliases = cli.indices.get_alias(
        index=name
    ).get(
        name, {}
    ).get(
        'aliases', {}
    ).keys()

    with open(mapping) as mapping_fd:
        body = mapping_fd.read()

    click.echo(
        '(Re)Creating temporary index (mapping and aliases), named %s'
        % tmp_index
    )
    cli.indices.delete(index=tmp_index, ignore=[400, 404])
    cli.indices.create(
        index=tmp_index,
        body=body,
    )
    for alias in aliases:
        cli.indices.put_alias(index=tmp_index, name=alias)

    click.echo(
        'Created temporary index, will start dumping the data from the old '
        'one, this might take some time (~40 docs/sec).'
    )
    click.confirm('Do you want to continue?', abort=True)
    errors_file = 'reindex_%s_errors.json' % tmp_index
    _, errors = utils._reindex(
        errors_file=errors_file,
        client=cli,
        source_index=name,
        target_index=tmp_index,
        query=None,
        target_client=None,
        chunk_size=500,
        scroll='5m',
        bulk_kwargs={
            'params': {
                'request_timeout': Timeout(read=30),
            },
        },
    )
    if errors:
        click.confirm(
            (
                'Got %d errors, saved in the file "%s", '
                'want to continue?'
            ) % (len(errors), errors_file),
            abort=True,
        )

    click.echo(
        'Populated temporary index, will recreate original index (this will '
        'remove it\'s contents).'
    )
    click.confirm('Do you want to continue?', abort=True)
    cli.indices.delete(name)
    cli.indices.create(
        index=name,
        body=body,
    )
    for alias in aliases:
        cli.indices.put_alias(index=name, name=alias)

    click.echo(
        'Recreated original index (mapping and aliases), will repopulate '
        'with the data from the temporary one.'
    )
    click.confirm('Do you want to continue?', abort=True)
    errors_file = 'reindex_%s_errors.json' % name
    _, errors = utils._reindex(
        errors_file=errors_file,
        client=cli,
        source_index=tmp_index,
        target_index=name,
        query=None,
        target_client=None,
        chunk_size=500,
        scroll='5m',
        bulk_kwargs={
            'params': {
                'request_timeout': Timeout(read=30),
            },
        },
    )

    if errors:
        click.confirm(
            (
                'Got %d errors, saved in the file "%s", '
                'want to continue?'
            ) % (len(errors), errors_file),
            abort=True,
        )

    click.echo('Original index repopulated, will cleanup the temporary index.')
    click.confirm('Do you want to continue?', abort=True)
    cli.indices.delete(tmp_index)
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
