import datetime

import asynctest.mock
import pytest

from exptools.time import (
  utcnow, localnow,
  as_local, as_utc,
  format_utc, format_utc_short, format_local, format_local_short,
  parse_utc, parse_local,
  diff_sec,
  format_sec, format_sec_fixed, format_sec_short,
  job_elapsed_time,
  format_job_count,
  format_estimated_time,
)

class _mocked_datetime(datetime.datetime):
  @classmethod
  def utcnow(cls):
    return cls(2000, 1, 2, 3, 4, 5, 678901)

  @classmethod
  def localnow(cls):
    return cls(2000, 1, 2, 3, 4, 5, 678901)


@asynctest.mock.patch('datetime.datetime', new=_mocked_datetime)
def test_utcnow():
  assert format_utc(utcnow()) == '2000-01-02 03:04:05.678901'


@asynctest.mock.patch('datetime.datetime', new=_mocked_datetime)
def test_localnow():
  assert format_utc(localnow()) == '2000-01-02 03:04:05.678901'


def test_as_utc():
  t = parse_utc('2000-01-02 03:04:05.678901')
  assert t == as_utc(t)


def test_as_local():
  t = parse_local('2000-01-02 03:04:05.678901')
  assert t == as_local(t)


def test_as_utc_as_local_as_utc():
  t = parse_utc('2000-01-02 03:04:05.678901')
  assert t == as_utc(as_local(t))

  t = parse_local('2000-01-02 03:04:05.678901')
  assert t == as_local(as_utc(t))


def test_parse_utc_format_utc():
  assert format_utc(parse_utc('2000-01-02 03:04:05.678901')) == '2000-01-02 03:04:05.678901'


def test_parse_local_format_local():
  assert format_local(parse_local('2000-01-02 03:04:05.678901')) == '2000-01-02 03:04:05.678901'


def test_parse_utc_format_utc_short():
  assert format_utc_short(parse_utc('2000-01-02 03:04:05.678901')) == '2000-01-02 03:04:05'


def test_parse_local_format_local_short():
  assert format_local_short(parse_local('2000-01-02 03:04:05.678901')) == '2000-01-02 03:04:05'


def test_diff_sec():
  t1 = parse_utc('2000-01-02 03:04:05.678901')
  t2 = parse_utc('2000-01-02 02:03:04.678900')
  assert diff_sec(t1, t2) == 3600 + 60 + 1 + 0.000001


def test_format_sec():
  assert format_sec(0) == '0 seconds'
  assert format_sec(1) == '1 second'
  assert format_sec(2) == '2 seconds'

  assert format_sec(60) == '1 minute'
  assert format_sec(60 * 2) == '2 minutes'
  assert format_sec(60 * 2 + 1) == '2 minutes 1 second'

  assert format_sec(3600) == '1 hour'
  assert format_sec(3600 * 2) == '2 hours'
  assert format_sec(3600 * 2 + 60) == '2 hours 1 minute'
  assert format_sec(3600 * 2 + 60 + 1) == '2 hours 1 minute 1 second'

  assert format_sec(86400) == '1 day'
  assert format_sec(86400 * 2) == '2 days'
  assert format_sec(86400 * 2 + 3600) == '2 days 1 hour'
  assert format_sec(86400 * 2 + 3600 + 60) == '2 days 1 hour 1 minute'
  assert format_sec(86400 * 2 + 3600 + 60 + 1) == '2 days 1 hour 1 minute 1 second'

  assert format_sec(86400 * 7) == '1 week'
  assert format_sec(86400 * 7 + 1) == '1 week 1 second'
  assert format_sec(86400 * 7 + 60) == '1 week 1 minute'
  assert format_sec(86400 * 7 + 3600) == '1 week 1 hour'
  assert format_sec(86400 * 7 + 86400) == '1 week 1 day'


def test_format_sec_fixed():
  assert format_sec_fixed(2) == '0:00:02'
  assert format_sec_fixed(60 * 2 + 1) == '0:02:01'
  assert format_sec_fixed(3600 * 2 + 60 + 1) == '2:01:01'


def test_format_short():
  assert format_sec_short(0) == '0s'
  assert format_sec_short(1) == '1s'
  assert format_sec_short(2) == '2s'

  assert format_sec_short(60) == '1m  0s'
  assert format_sec_short(60 * 2) == '2m  0s'
  assert format_sec_short(60 * 2 + 1) == '2m  1s'

  assert format_sec_short(3600) == '1h  0m'
  assert format_sec_short(3600 * 2) == '2h  0m'
  assert format_sec_short(3600 * 2 + 60) == '2h  1m'
  assert format_sec_short(3600 * 2 + 60 + 1) == '2h  1m'

  assert format_sec_short(86400) == '1d  0h'
  assert format_sec_short(86400 * 2) == '2d  0h'
  assert format_sec_short(86400 * 2 + 3600) == '2d  1h'
  assert format_sec_short(86400 * 2 + 3600 + 60) == '2d  1h'
  assert format_sec_short(86400 * 2 + 3600 + 60 + 1) == '2d  1h'

  assert format_sec_short(86400 * 7) == '1w  0d'
  assert format_sec_short(86400 * 7 + 1) == '1w  0d'
  assert format_sec_short(86400 * 7 + 60) == '1w  0d'
  assert format_sec_short(86400 * 7 + 3600) == '1w  0d'
  assert format_sec_short(86400 * 7 + 86400) == '1w  1d'


def test_job_elapsed_time_finished_job():
  job = {'finished': '2000-01-02 03:04:05.678901', 'duration': 10.}
  assert job_elapsed_time(job) == 10.


@asynctest.mock.patch('exptools.time.utcnow')
def test_job_elapsed_time_started_job(mock_utcnow):
  mock_utcnow.return_value = parse_utc('2000-01-02 03:04:15.678901')
  job = {'finished': None, 'started': '2000-01-02 03:04:05.678901'}
  assert job_elapsed_time(job) == 10.


@asynctest.mock.patch('termcolor.colored')
def test_format_job_count(mock_colored):
  mock_colored.side_effect = lambda s, *args, **kwargs: s

  queue_state = {
    'finished_jobs': [{'succeeded': True}],
    'started_jobs': [3, 4, 5],
    'queued_jobs': [6, 7, 8, 9],
  }
  assert format_job_count(queue_state, True) == 'S:1 F:0 A:3 Q:4'

  queue_state = {
    'finished_jobs': [{'succeeded': True}, {'succeeded': False}, {'succeeded': False}],
    'started_jobs': [3, 4, 5],
    'queued_jobs': [6, 7, 8, 9],
  }
  assert format_job_count(queue_state, False) == 'S:1 F:2 A:3 Q:4'


@pytest.mark.asyncio
@asynctest.mock.patch('datetime.datetime', new=_mocked_datetime)
@asynctest.mock.patch('termcolor.colored')
async def test_format_estimated_time(mock_colored):
  mock_colored.side_effect = lambda s, *args, **kwargs: s

  remaining_time = 10.

  queue_state = {
    'finished_jobs': [{'succeeded': True}, {'succeeded': False}, {'succeeded': False}],
    'started_jobs': [3, 4, 5],
    'queued_jobs': [6, 7, 8, 9],
    'concurrency': 1.1,
  }

  assert (await format_estimated_time(remaining_time, queue_state, True) ==
          'S:1 F:2 A:3 Q:4  Remaining %ds  Finish by %s  Concurrency 1.1' % (
            remaining_time,
            format_local_short(utcnow() + datetime.timedelta(seconds=10))))

  assert (await format_estimated_time(10., queue_state, False) ==
          'S:1 F:2 A:3 Q:4  Remaining %ds  Finish by %s  Concurrency 1.1' % (
            remaining_time,
            format_local_short(utcnow() + datetime.timedelta(seconds=10))))
