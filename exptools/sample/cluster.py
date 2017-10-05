from threading import Lock

def get_cluster():
  return Lock(), {'concurrency': 2, 'ps': 2, 'worker': 6}
