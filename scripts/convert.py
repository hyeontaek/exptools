import json
import os

def convert(base_dir):
  if os.path.exists('queue.json'):
    os.unlink('queue.json')

  history = json.load(open('history.json'))
  new_history = {key.replace('p-', 'h-'): value for key, value in history.items()}
  json.dump(open('new_history.json'), new_history)

  for filename in os.listdir(base_dir):
    if filename.startswith('p-'):
      new_filename = filename.replace('p-', 'h-')

      path = os.path.join(base_dir, filename)
      new_path = os.path.join(base_dir, new_filename)
      os.rename(path, new_path)

if __name__ == '__main__':
  convert('output')
