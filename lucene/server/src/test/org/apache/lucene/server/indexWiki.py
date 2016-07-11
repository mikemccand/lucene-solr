import time
import sys
import json
import os
import subprocess
import threading
import http.client

# TODO
#   - test commit
#   - test killing server, promoting new primary, etc.
#   - test 2nd replica

#host1 = '10.17.4.12'
#host2 = '10.17.4.92'
host1 = '127.0.0.1'
host2 = '127.0.0.1'

class ChunkedSend:

  def __init__(self, host, port, command, chunkSize):
    self.h = http.client.HTTPConnection(host, port)
    self.h.putrequest('POST', '/%s' % command)
    self.h.putheader('Transfer-Encoding', 'chunked')
    self.h.endheaders()
    self.chunkSize = chunkSize
    self.pending = []
    self.pendingBytes = 0

  def add(self, data):
    self.pending.append(data)
    self.pendingBytes += len(data)
    self.sendChunks(False)

  def sendChunks(self, finish):
    if self.pendingBytes == 0:
      return

    if finish or self.pendingBytes > self.chunkSize:
      s = ''.join(self.pending)
      upto = 0
      while True:
        chunk = s[self.chunkSize*upto:self.chunkSize*(1+upto)]
        if len(chunk) == 0:
          break
        if not finish and len(chunk) < self.chunkSize:
          break
        m = []
        m.append('%s\r\n' % hex(len(chunk))[2:])
        m.append(chunk)
        #print 'send: %s' % chunk                                                                                                                            
        m.append('\r\n')
        try:
          self.h.send(''.join(m).encode('utf-8'))
        except:
          # Something went wrong; see if server told us what:                                                                                                
          r = self.h.getresponse()
          s = r.read()
          raise RuntimeError('\n\nServer error:\n%s' % s)
        upto += 1
      del self.pending[:]
      self.pending.append(chunk)
      self.pendingBytes = len(chunk)

  def finish(self):
    """                                                                                                                                                      
    Finishes the request and returns the resonse.                                                                                                            
    """

    self.sendChunks(True)

    # End chunk marker:                                                                                                                                      
    self.h.send(b'0\r\n\r\n')

    r = self.h.getresponse()
    if r.status == 200:
      size = r.getheader('Content-Length')
      s = r.read().decode('utf-8')
      self.h.close()
      return json.loads(s)
    elif r.status == http.client.BAD_REQUEST:
      s = r.read()
      self.h.close()
      raise RuntimeError('\n\nServer error:\n%s' % s.decode('utf-8'))
    else:
      raise RuntimeError('Server returned HTTP %s, %s' % (r.status, r.reason))

def launchServer(host, stateDir, port):
  command = r'ssh mike@%s "cd /l/newluceneserver/lucene; java -Xmx2g -verbose:gc -cp build/replicator/lucene-replicator-7.0.0-SNAPSHOT.jar:build/facet/lucene-facet-7.0.0-SNAPSHOT.jar:build/highlighter/lucene-highlighter-7.0.0-SNAPSHOT.jar:build/expressions/lucene-expressions-7.0.0-SNAPSHOT.jar:build/analysis/common/lucene-analyzers-common-7.0.0-SNAPSHOT.jar:build/analysis/icu/lucene-analyzers-icu-7.0.0-SNAPSHOT.jar:build/queries/lucene-queries-7.0.0-SNAPSHOT.jar:build/join/lucene-join-7.0.0-SNAPSHOT.jar:build/queryparser/lucene-queryparser-7.0.0-SNAPSHOT.jar:build/suggest/lucene-suggest-7.0.0-SNAPSHOT.jar:build/core/lucene-core-7.0.0-SNAPSHOT.jar:build/server/lucene-server-7.0.0-SNAPSHOT.jar:server/lib/\* org.apache.lucene.server.Server -port %s -stateDir %s -interface %s"' % (host, port, stateDir, host)
  print('%s: server command %s' % (host, command))
  p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

  # Read lines until we see the server is started:
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
    print('FAILED SEND to %s:%s:\n%s' % (host, port, s.decode('utf-8')))
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
  if host2 != host1:
    run('rsync -a /l/newluceneserver/ mike@%s:/l/newluceneserver/' % host1)

rmDir(host1, '/b/scratch/server1')
rmDir(host2, '/b/scratch/server2')

port1, binaryPort1 = launchServer(host1, '/b/scratch/server1/state', 4000)
port2, binaryPort2 = launchServer(host2, '/b/scratch/server2/state', 5000)

try:
  send(host1, port1, 'createIndex', {'indexName': 'index', 'rootDir': '/b/scratch/server1/index'})
  send(host2, port2, 'createIndex', {'indexName': 'index', 'rootDir': '/b/scratch/server2/index'})
  send(host1, port1, "liveSettings", {'indexName': 'index', 'index.ramBufferSizeMB': 128., 'maxRefreshSec': 5.0})
  send(host2, port2, "settings", {'indexName': 'index', 'index.verbose': True, 'directory': 'MMapDirectory', 'nrtCachingDirectory.maxSizeMB': 0.0})
  send(host1, port1, "settings", {'indexName': 'index', 'index.verbose': False, 'directory': 'MMapDirectory', 'nrtCachingDirectory.maxSizeMB': 0.0})

  fields = {'indexName': 'index',
            'fields': {'body':
                       {'type': 'text',
                        'highlight': True,
                        'store': True},
                       'title':
                       {'type': 'text',
                        'highlight': True,
                        'store': True},
                       'id':
                       {'type': 'int',
                        'store': True,
                        'sort': True}}}

  send(host1, port1, 'registerFields', fields)
  send(host2, port2, 'registerFields', fields)

  send(host1, port1, 'startIndex', {'indexName': 'index', 'mode': 'primary', 'primaryGen': 0})
  #send(host2, port2, 'startIndex', {'indexName': 'index', 'mode': 'replica', 'primaryAddress': host1, 'primaryGen': 0, 'primaryPort': binaryPort1})

  b = ChunkedSend(host1, port1, 'bulkAddDocument', 65536)
  b.add('{"indexName": "index", "documents": [')

  id = 0
  tStart = time.time()
  with open('/lucenedata/enwiki/enwiki-20120502-lines-1k-fixed-utf8.txt', 'r') as f:
    f.readline()
    while True:
      line = f.readline()
      if line == '':
        break
      title, date, body = line.split('\t')
      if id > 0:
        b.add(',')
      b.add(json.dumps({'fields': {'body': body, 'title': title, 'id': id}}))
      id += 1

      if id == 1:
        print('now start up replica!')
        send(host2, port2, 'startIndex', {'indexName': 'index', 'mode': 'replica', 'primaryAddress': host1, 'primaryGen': 0, 'primaryPort': binaryPort1})
        
      if id % 100000 == 0:
        dps = id / (time.time()-tStart)
        if id >= 2000000:
          x = json.loads(send(host2, port2, 'search', {'indexName': 'index', 'queryText': '*:*', 'retrieveFields': ['id']}));
          print('%d docs...%d hits; %.1f docs/sec' % (id, x['totalHits'], dps))
        else:
          print('%d docs... %.1f docs/sec' % (id, dps))

    b.add(']}')
  b.finish();
  
finally:
  send(host1, port1, 'shutdown', {})
  send(host2, port2, 'shutdown', {})
