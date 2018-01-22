# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest

from pigeon import CONFIG, extract_crash_id, parse_queues


def test_basic(client, rabbitmq_helper):
    crash_id = 'de1bb258-cbbf-4589-a673-34f800160918'
    events = client.build_crash_save_events(client.crash_id_to_path(crash_id))
    assert client.run(events) is None

    item = rabbitmq_helper.next_item()
    assert item == crash_id


def test_non_s3_event(client, rabbitmq_helper):
    events = {
        'Records': [
            {
                'eventSource': 'aws:lonnen',
            }
        ]
    }
    assert client.run(events) is None

    # Verify that no rabbit message got created
    item = rabbitmq_helper.next_item()
    assert item is None


def test_non_put_event(client, rabbitmq_helper):
    events = {
        'Records': [
            {
                'eventSource': 'aws:lonnen',
            }
        ]
    }
    assert client.run(events) is None

    # Verify that no rabbit message got created
    item = rabbitmq_helper.next_item()
    assert item is None


def test_multiple_queues(client, rabbitmq_helper):
    queues = [(100, 'normal'), (100, 'submitter')]

    with CONFIG.override(queues=queues):
        # Rebuild the connection using the overridden values
        rabbitmq_helper.build_conn()

        # Create a crash_id event and run pigeon
        crash_id = 'de1bb258-cbbf-4589-a673-34f800160918'
        events = client.build_crash_save_events(client.crash_id_to_path(crash_id))
        assert client.run(events) is None

        # Verify the crash_id shows up in both queues
        for throttle, queue in queues:
            assert rabbitmq_helper.next_item(queue) == crash_id


def test_queue_throttling(client, rabbitmq_helper, mock_randint_always_20):
    queues = [(100, 'normal'), (15, 'submitter'), (0, 'devnull')]

    with CONFIG.override(queues=queues):
        # Rebuild the connection using the overridden values
        rabbitmq_helper.build_conn()

        # Create a crash_id event and run pigeon
        crash_id = 'de1bb258-cbbf-4589-a673-34f800160918'
        events = client.build_crash_save_events(client.crash_id_to_path(crash_id))
        assert client.run(events) is None

        # Verify the crash_id shows up in both queues
        assert rabbitmq_helper.next_item('normal') == crash_id
        assert rabbitmq_helper.next_item('submitter') is None
        assert rabbitmq_helper.next_item('devnull') is None


def test_env_tag(client, rabbitmq_helper, capsys):
    with CONFIG.override(env='stage'):
        crash_id = 'de1bb258-cbbf-4589-a673-34f800160918'
        #                                        ^ accept
        events = client.build_crash_save_events(client.crash_id_to_path(crash_id))
        assert client.run(events) is None

        item = rabbitmq_helper.next_item()
        assert item == crash_id

        stdout, stderr = capsys.readouterr()
        assert '|1|count|socorro.pigeon.accept|#env:stage\n' in stdout


def test_defer(client, rabbitmq_helper, capsys):
    crash_id = 'de1bb258-cbbf-4589-a673-34f801160918'
    #                                        ^ defer
    events = client.build_crash_save_events(client.crash_id_to_path(crash_id))
    assert client.run(events) is None

    item = rabbitmq_helper.next_item()
    assert item is None

    stdout, stderr = capsys.readouterr()
    assert '|1|count|socorro.pigeon.defer|' in stdout


def test_accept(client, rabbitmq_helper, capsys):
    crash_id = 'de1bb258-cbbf-4589-a673-34f800160918'
    #                                        ^ accept
    events = client.build_crash_save_events(client.crash_id_to_path(crash_id))
    assert client.run(events) is None

    item = rabbitmq_helper.next_item()
    assert item == crash_id

    stdout, stderr = capsys.readouterr()
    assert '|1|count|socorro.pigeon.accept|' in stdout


def test_junk(client, rabbitmq_helper, capsys):
    crash_id = 'de1bb258-cbbf-4589-a673-34f802160918'
    #                                        ^ junk
    events = client.build_crash_save_events(client.crash_id_to_path(crash_id))
    assert client.run(events) is None

    item = rabbitmq_helper.next_item()
    assert item is None

    stdout, stderr = capsys.readouterr()
    assert '|1|count|socorro.pigeon.junk|' in stdout


def test_junk_in_stage(client, rabbitmq_helper, capsys):
    with CONFIG.override(env='stage'):
        crash_id = 'de1bb258-cbbf-4589-a673-34f802160918'
        #                                        ^ junk
        events = client.build_crash_save_events(client.crash_id_to_path(crash_id))
        assert client.run(events) is None

        item = rabbitmq_helper.next_item()
        assert item == crash_id

        stdout, stderr = capsys.readouterr()
        assert '|1|count|socorro.pigeon.accept|' in stdout


@pytest.mark.parametrize('data, expected', [
    # Raw crash file
    (
        'v2/raw_crash/de1/20160918/de1bb258-cbbf-4589-a673-34f800160918',
        'de1bb258-cbbf-4589-a673-34f800160918'
    ),

    # Other files that get saved in the same bucket
    ('v1/dump_names/de1bb258-cbbf-4589-a673-34f800160918', None),
    ('v1/upload_file_minidump/de1bb258-cbbf-4589-a673-34f800160918', None),

    # Test-like files we might have pushed places to make sure things are working
    ('v2/raw_crash/de1/20160918/test', None),
    ('foo/bar/test', None),

    # This is a crash from -prod which currently has a 2 in the accept/defer place
    (
        'v2/raw_crash/edd/20170404/edd0cf02-9e6f-443a-b098-8274b2170404',
        'edd0cf02-9e6f-443a-b098-8274b2170404'
    ),
])
def test_extract_crash_id(data, expected, client):
    record = client.build_crash_save_events(data)['Records'][0]
    assert extract_crash_id(record) == expected


@pytest.mark.parametrize('data, expected', [
    # Single queue as a string
    ('socorro.normal', [(100, 'socorro.normal')]),
    ('  socorro.normal\n ', [(100, 'socorro.normal')]),

    # Test throttle number
    ('15:socorro.normal', [(15, 'socorro.normal')]),
    ('  15 : socorro.normal\n ', [(15, 'socorro.normal')]),

    # Test multiple queues
    ('socorro.normal , 10:socorro.submitter', [(100, 'socorro.normal'), (10, 'socorro.submitter')])
])
def test_parse_queues(data, expected):
    assert parse_queues(data) == expected
