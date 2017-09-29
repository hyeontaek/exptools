from exptools.time import *

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
