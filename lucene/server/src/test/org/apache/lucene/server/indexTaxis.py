import socket
import struct
import time
import sys
import json
import os
import subprocess
import threading
import http.client

# TODO
#   - index lat/lon as geopoint!

host1 = '10.17.4.12'
host2 = '10.17.4.92'
#host1 = '127.0.0.1'
#host2 = '127.0.0.1'

DO_REPLICA = False

class BinarySend:
  def __init__(self, host, port, command, chunkSize):
    self.socket = socket.socket()
    self.socket.connect((host, port))
    # BINARY_MAGIC header:
    self.socket.sendall(struct.pack('>i', 0x3414f5c))
    command = 'bulkCSVAddDocument'.encode('utf-8')
    self.socket.sendall(('%c' % len(command)).encode('utf-8'))
    self.socket.sendall(command)

  def add(self, bytes):
    self.socket.sendall(bytes)

  def finish(self):
    self.socket.shutdown(socket.SHUT_WR)

  def close(self):
    self.socket.close()

def launchServer(host, stateDir, port):
  command = r'ssh mike@%s "cd /l/newluceneserver/lucene; java -Xmx4g -verbose:gc -cp build/replicator/lucene-replicator-7.0.0-SNAPSHOT.jar:build/facet/lucene-facet-7.0.0-SNAPSHOT.jar:build/highlighter/lucene-highlighter-7.0.0-SNAPSHOT.jar:build/expressions/lucene-expressions-7.0.0-SNAPSHOT.jar:build/analysis/common/lucene-analyzers-common-7.0.0-SNAPSHOT.jar:build/analysis/icu/lucene-analyzers-icu-7.0.0-SNAPSHOT.jar:build/queries/lucene-queries-7.0.0-SNAPSHOT.jar:build/join/lucene-join-7.0.0-SNAPSHOT.jar:build/queryparser/lucene-queryparser-7.0.0-SNAPSHOT.jar:build/suggest/lucene-suggest-7.0.0-SNAPSHOT.jar:build/core/lucene-core-7.0.0-SNAPSHOT.jar:build/server/lucene-server-7.0.0-SNAPSHOT.jar:server/lib/\* org.apache.lucene.server.Server -port %s -stateDir %s -interface %s"' % (host, port, stateDir, host)
  print('%s: server command %s' % (host, command))
  p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

  # Read lines until we see the server is started, then launch bg thread to read future lines:
  while True:
    line = p.stdout.readline()
    line = line.decode('utf-8')
    if line == '':
      raise RuntimeError('server on %s failed to start' % host)
    print('%s: %s' % (host, line))

    if 'listening on port' in line:
      x = line.split()[-1]
      x = x.replace('.', '')
      ports = (int(y) for y in x.split('/'))
      break
    
  thread = threading.Thread(target=readServerOutput, args=(host, p))
  thread.start()
  return ports

def readServerOutput(host, p):
  while True:
    line = p.stdout.readline()
    if line == b'':
      break
    print('%s: %s' % (host, line.decode('utf-8').rstrip()))

def send(host, port, command, args):
  args = json.dumps(args).encode('utf-8')
  h = http.client.HTTPConnection(host, port)
  h.request('POST', '/%s' % command, args, {'Content-Length': str(len(args))})
  r = h.getresponse()
  s = r.read()
  if r.status != 200:
    raise RuntimeError('FAILED: %s:%s:\n%s' % (host, port, s.decode('utf-8')))
    
  return s.decode('utf-8')

def run(command, logFile = None):
  if logFile is not None:
    command += ' >> %s 2>&1' % logFile
  print('RUN: %s' % command)
  result = subprocess.call(command, shell=True)
  if result != 0:
    if logFile is not None:
      print(open(logFile).read())
    raise RuntimeError('command failed with result %s: %s' % (result, command))

def rmDir(host, path):
  run('ssh %s rm -rf %s' % (host, path))

os.chdir('/l/newluceneserver/lucene')
if '-rebuild' in sys.argv:
  run('ant clean jar', 'ant.log')
  #run('ssh %s rm -rf /l/newluceneserver' % host2)
  for host in (host1, host2):
    if host not in ('10.17.4.12', '127.0.0.1'):
      run('rsync -a /l/newluceneserver/ mike@%s:/l/newluceneserver/' % host)

ROOT_DIR = '/b/taxis'

rmDir(host1, '%s/server1' % ROOT_DIR)

if DO_REPLICA:
  rmDir(host2, '%s/server2' % ROOT_DIR)

port1, binaryPort1 = launchServer(host1, '%s/server1/state' % ROOT_DIR, 4000)
if DO_REPLICA:
  port2, binaryPort2 = launchServer(host2, '%s/server2/state' % ROOT_DIR, 5000)

try:
  send(host1, port1, 'createIndex', {'indexName': 'index', 'rootDir': '%s/server1/index' % ROOT_DIR})
  if DO_REPLICA:
    send(host2, port2, 'createIndex', {'indexName': 'index', 'rootDir': '%s/server2/index' % ROOT_DIR})
  send(host1, port1, "liveSettings", {'indexName': 'index', 'index.ramBufferSizeMB': 128., 'maxRefreshSec': 5.0})
  if DO_REPLICA:
    send(host2, port2, "settings", {'indexName': 'index',
                                    'index.verbose': False,
                                    'directory': 'MMapDirectory',
                                    'nrtCachingDirectory.maxSizeMB': 0.0,
                                    'index.merge.scheduler.auto_throttle': False})
  send(host1, port1, "settings", {'indexName': 'index',
                                  'index.verbose': False,
                                  'directory': 'MMapDirectory',
                                  'nrtCachingDirectory.maxSizeMB': 0.0,
                                  'index.merge.scheduler.auto_throttle': False})
  

  fields = {'indexName': 'index',
            'fields':
            {
              'vendor_id': {'type': 'atom'},
              'vendor_name': {'type': 'text'},
              'cab_color': {'type': 'atom'},
              'pick_up_date_time': {'type': 'long', 'search': True},
              'drop_off_date_time': {'type': 'long', 'search': True},
              'passenger_count': {'type': 'int', 'search': True},
              'trip_distance': {'type': 'double', 'search': True},
              'pick_up_lat': {'type': 'double', 'search': True},
              'pick_up_lon': {'type': 'double', 'search': True},
              'drop_off_lat': {'type': 'double', 'search': True},
              'drop_off_lon': {'type': 'double', 'search': True},
              'payment_type': {'type': 'atom'},
              'trip_type': {'type': 'atom'},
              'rate_code': {'type': 'atom'},
              'fare_amount': {'type': 'double', 'search': True},
              'surcharge': {'type': 'double', 'search': True},
              'mta_tax': {'type': 'double', 'search': True},
              'extra': {'type': 'double', 'search': True},
              'ehail_fee': {'type': 'double', 'search': True},
              'improvement_surcharge': {'type': 'double', 'search': True},
              'tip_amount': {'type': 'double', 'search': True},
              'tolls_amount': {'type': 'double', 'search': True},
              'total_amount': {'type': 'double', 'search': True},
              'store_and_fwd_flag': {'type': 'atom'}}}

  send(host1, port1, 'registerFields', fields)
  if DO_REPLICA:
    send(host2, port2, 'registerFields', fields)

  if DO_REPLICA:
    send(host1, port1, 'startIndex', {'indexName': 'index', 'mode': 'primary', 'primaryGen': 0})
    send(host2, port2, 'startIndex', {'indexName': 'index', 'mode': 'replica', 'primaryAddress': host1, 'primaryGen': 0, 'primaryPort': binaryPort1})
  else:
    send(host1, port1, 'startIndex', {'indexName': 'index'})

  b = BinarySend(host1, binaryPort1, 'bulkCSVAddDocument', 65536)
  b.add(b'index\n')

  id = 0
  tStart = time.time()
  nextPrint = 100000
  replicaStarted = False

  with open('/lucenedata/nyc-taxi-data/alltaxis.csv', 'rb') as f:
    while True:
      doc = f.readline()
      if len(doc) == 0:
        break
      #b.add(doc + b'\n')
      b.add(doc)
      #print('doc: %s' % doc)
      id += 1
      if id >= nextPrint:
        dps = id / (time.time()-tStart)
        if False and id >= 2000000:
          if DO_REPLICA:
            x = json.loads(send(host2, port2, 'search', {'indexName': 'index', 'queryText': '*:*', 'retrieveFields': ['geonameid']}));
          else:
            x = json.loads(send(host1, port1, 'search', {'indexName': 'index', 'queryText': '*:*', 'retrieveFields': ['geonameid']}));
          print('%d docs...%d hits; %.1f docs/sec' % (id, x['totalHits'], dps))
        else:
          print('%d docs... %.1f docs/sec' % (id, dps))

        while nextPrint <= id:
          nextPrint += 100000

  b.add(b']}')
  b.finish()
  bytes = b.socket.recv(4)
  len = struct.unpack('>i', bytes)
  bytes = b.socket.recv(len)
  print('GOT ANSWER:\n%s' % bytes.decode('utf-8'))
  b.close()

  dps = id / (time.time()-tStart)
  print('Total: %.1f docs/sec' % dps)

  print('Now stop index...')
  send(host1, port1, 'stopIndex', {'indexName': 'index'})
  print('Done stop index...')
  
finally:
  # nocommit why is this leaving leftover files?  it should close all open indices gracefully?
  send(host1, port1, 'shutdown', {})
  if DO_REPLICA:
    send(host2, port2, 'shutdown', {})
