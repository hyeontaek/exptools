import json
import os

def convert(base_dir):
  if os.path.exists('queue.json'):
    queue = json.load(open('queue.json'))
    for key in ['finished_jobs', 'started_jobs', 'queued_jobs']:
      jobs = queue[key]
      for job in jobs:
        job['param']['_']['hash_id'] = job['param']['_']['param_id'].replace('p-', 'h-')
        job['param']['_']['param_id'] = 'p-none'
    json.dump(queue, open('new_queue.json', 'w'))

  if os.path.exists('history.json'):
    history = json.load(open('history.json'))
    history = {key.replace('p-', 'h-'): value for key, value in history.items()}
    json.dump(history, open('new_history.json', 'w'))

  for filename in os.listdir(base_dir):
    if filename.startswith('p-'):
      new_filename = filename.replace('p-', 'h-')

      path = os.path.join(base_dir, filename)
      new_path = os.path.join(base_dir, new_filename)
      os.rename(path, new_path)

if __name__ == '__main__':
  convert('output')
