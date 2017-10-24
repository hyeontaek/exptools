'''Provide time related functions.'''

__all__ = [
    'utcnow', 'localnow',
    'as_local', 'as_utc',
    'format_utc', 'format_utc_short', 'format_local', 'format_local_short',
    'parse_utc', 'parse_local',
    'diff_sec',
    'format_sec', 'format_sec_fixed', 'format_sec_short',
    'job_elapsed_time',
    'format_job_count',
    'format_estimated_time',
    ]

import datetime

import termcolor
import tzlocal
import pytz

def utcnow():
  '''Return the current time in UTC.'''
  return datetime.datetime.utcnow().replace(tzinfo=pytz.utc)

def localnow():
  '''Return the current time in the local timezone.'''
  return as_local(utcnow())

def as_utc(time):
  '''Convert a time to a UTC time.'''
  return time.astimezone(pytz.utc)

def as_local(time):
  '''Convert a time to a local time.'''
  return time.astimezone(tzlocal.get_localzone())

def format_utc(time):
  '''Format a time in UTC.'''
  return as_utc(time).strftime('%Y-%m-%d %H:%M:%S.%f')

def format_utc_short(time):
  '''Format a time in UTC in a short format.'''
  return as_utc(time).strftime('%Y-%m-%d %H:%M:%S')

def format_local(time):
  '''Format a time in the local timezone.'''
  return as_local(time).strftime('%Y-%m-%d %H:%M:%S.%f')

def format_local_short(time):
  '''Format a time in the local timezone in a short format.'''
  return as_local(time).strftime('%Y-%m-%d %H:%M:%S')

def parse_utc(time):
  '''Parse a UTC time.'''
  return datetime.datetime.strptime(time, '%Y-%m-%d %H:%M:%S.%f').replace(tzinfo=pytz.utc)

def parse_local(time):
  '''Parse a local time.'''
  return datetime.datetime.strptime(time, '%Y-%m-%d %H:%M:%S.%f')\
      .replace(tzinfo=tzlocal.get_localzone())

def diff_sec(time1, time2):
  '''Calculate the difference between two times in seconds.'''
  #return (as_utc(time1) - as_utc(time2)).total_seconds()
  return (time1 - time2).total_seconds()

def format_sec(sec):
  '''Format seconds in a human-readable format.'''
  sec = round(sec)
  output = ''
  if sec >= 86400 * 7:
    value = int(sec / (86400 * 7))
    sec -= value * 86400 * 7
    output += '%d week%s ' % (value, 's' if value != 1 else '')
  if sec >= 86400:
    value = int(sec / 86400)
    sec -= value * 86400
    output += '%d day%s ' % (value, 's' if value != 1 else '')
  if sec >= 3600:
    value = int(sec / 3600)
    sec -= value * 3600
    output += '%d hour%s ' % (value, 's' if value != 1 else '')
  if sec >= 60:
    value = int(sec / 60)
    sec -= value * 60
    output += '%d minute%s ' % (value, 's' if value != 1 else '')
  value = sec
  if value > 0 or output == '':
    output += '%d second%s ' % (value, 's' if value != 1 else '')
  return output.rstrip()

def format_sec_fixed(sec):
  '''Format seconds in a fixed format.'''
  return '%d:%02d:%02d' % (int(sec / 3600), int(sec % 3600 / 60), int(round(sec % 60)))

def format_sec_short(sec, max_component_count=2):
  '''Format seconds in a short format.'''
  sec = round(sec)

  component_count = 0
  output = ''

  def _add_component(unit, unit_secs):
    nonlocal sec
    nonlocal output
    nonlocal component_count
    if (component_count == 0 and sec >= unit_secs) or \
       (component_count == 0 and unit == 's') or \
       component_count > 0:
      if component_count < max_component_count:
        if component_count < max_component_count - 1 and unit != 's':
          value = int(sec / unit_secs)
        else:
          value = round(sec / unit_secs)
        sec -= value * unit_secs
        output += '%2d%s ' % (value, unit)
        component_count += 1

  _add_component('w', 86400 * 7)
  _add_component('d', 86400)
  _add_component('h', 3600)
  _add_component('m', 60)
  _add_component('s', 1)

  return output.strip()

def job_elapsed_time(job):
  '''Format elapsed time of a job.'''
  now = utcnow()
  if job['finished']:
    sec = job['duration']
  else:
    sec = diff_sec(now, parse_utc(job['started']))
  return sec

def format_job_count(queue_state, use_color):
  '''Format job count.'''

  state = queue_state
  if use_color:
    colored = termcolor.colored
  else:
    colored = lambda s, *args, **kwargs: s

  succeeded = len(list(filter(lambda job: job['succeeded'], state['finished_jobs'])))
  failed = len(list(filter(lambda job: not job['succeeded'], state['finished_jobs'])))
  started = len(state['started_jobs'])
  queued = len(state['queued_jobs'])

  output = ''
  output += colored('S:' + str(succeeded), 'green', attrs=['reverse'])
  output += ' '
  if failed == 0:
    output += colored('F:' + str(failed), 'green', attrs=['reverse'])
  else:
    output += colored('F:' + str(failed), 'red', attrs=['reverse'])
  output += ' '
  output += colored('A:' + str(started), 'cyan', attrs=['reverse'])
  output += ' '
  output += colored('Q:' + str(queued), 'blue', attrs=['reverse'])
  return output

async def format_estimated_time(estimator, queue_state, oneshot, use_color):
  '''Format the estimated time with colors.'''

  state = queue_state
  if use_color:
    colored = termcolor.colored
  else:
    colored = lambda s, *args, **kwargs: s

  remaining_time, _ = await estimator.estimate_remaining_time(state, oneshot)
  remaining_str = format_sec_short(remaining_time)

  current_time = utcnow()

  finish_by = current_time + datetime.timedelta(seconds=remaining_time)
  finish_by_local_str = format_local_short(finish_by)

  concurrency = state['concurrency']

  output = ''
  output += format_job_count(state, use_color)
  output += colored(
      f'  Remaining {remaining_str}' + \
      f'  Finish by {finish_by_local_str}' + \
      f'  Concurrency {concurrency}', 'blue')
  return output
