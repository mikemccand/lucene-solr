import os
import subprocess

remoteHost = '10.17.4.12'

def run(command, logFile = None):
  if logFile is not None:
    command += ' >> %s 2>&1' % logFile
  print('RUN: %s' % command)
  result = subprocess.call(command, shell=True)
  if result < 0:
    raise RuntimeError('command failed with result %s: %s' % (result, command))

os.chdir('/l/newluceneserver/lucene')
run('ant jar', 'ant.log')
run('ssh %s rm -rf /l/newluceneserver' % remoteHost)
run('rsync -a /l/newluceneserver/ mike@%s:/l/newluceneserver/' % remoteHost)
