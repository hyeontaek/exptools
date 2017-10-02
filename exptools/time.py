'''Provide time related functions.'''

__all__ = [
    'utcnow', 'localnow',
    'as_local', 'as_utc',
    'format_utc', 'format_local',
    'parse_utc', 'parse_local',
    'diff_sec',
    'format_sec']

import datetime
import pytz
import tzlocal

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
  return as_utc(time).strftime('%Y-%m-%d %H:%M:%S')

def format_local(time):
  '''Format a time in the local timezone.'''
  return as_local(time).strftime('%Y-%m-%d %H:%M:%S')

def parse_utc(time):
  '''Parse a UTC time.'''
  return datetime.datetime.strptime(time, '%Y-%m-%d %H:%M:%S').replace(tzinfo=pytz.utc)

def parse_local(time):
  '''Parse a local time.'''
  return datetime.datetime.strptime(time, '%Y-%m-%d %H:%M:%S')\
      .replace(tzinfo=tzlocal.get_localzone())

def diff_sec(time1, time2):
  '''Calculate the difference between two times in seconds.'''
  #return (as_utc(time1) - as_utc(time2)).total_seconds()
  return (time1 - time2).total_seconds()

def format_sec(sec):
  '''Format seconds as human-readable shorthands.'''
  sec = round(sec)
  output = ''
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
