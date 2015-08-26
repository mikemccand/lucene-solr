import base64
import cgi
import subprocess
import wsgiref.simple_server
import socket

#import sys
#sys.path.insert(0, '/home/changingbits/webapps/examples/htdocs')
import localconstants

def application(environ, startResponse):

  path = environ.get('PATH_INFO', '')
  print('path %s' % path)
  if path != '/' and not path.startswith('/tokenize.py'):
    # e.g. /favicon.ico:
    startResponse('404 NOT FOUND', [('Content-Type', 'text/plain')])
    return ['Not Found'.encode('utf-8')]

  _l = []
  w = _l.append

  args = cgi.parse(environ=environ)
  #print('GOT ARGS: %s' % str(args))
  if 'text' in args:
    text = args['text'][0]
  else:
    text = 'dns is down'

  if 'syns' in args:
    syns = args['syns'][0]
  else:
    syns = '''dns, domain name service'''

  w('<html>')
  w('<body>')
  w('<h2>Analyzer</h2>')

  w('<table>')
  w('<tr>')
  w('<td valign=top>')
  w('<form method=GET action="/tokenize.py">')
  w('<table>')
  w('<tr>')
  w('<td>')
  w('Text to analyze:<br>')
  w('<textarea name="text" cols=50 rows=10>')
  if text is not None:
    w(text)
  w('</textarea>')
  w('</td>')
  w('<td>')
  w('Synonyms:<br>')
  w('<textarea name="syns" cols=50 rows=10>')
  if syns is not None:
    w(syns)
  w('</textarea>')
  w('</td>')
  w('</tr>')
  w('</table>')
  w('<br>')
  w('<br>')
  w('<input type=submit name=cmd value="Tokenize!">')
  w('</form>')
  w('</td>')
  w('<td valign=top>')
  w('<ul>')
  w('</ul>')
  w('</tr>')
  w('</table>')

  if text is not None:

    p = subprocess.Popen(['java', '-cp', localconstants.STAGE_TOKENIZER_CLASSPATH, 'org.apache.lucene.analysis.EnglishTokenizerDemo', text, syns], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    result, err = p.communicate()
    result = result.decode('utf-8')
    if p.returncode != 0:
      w('<br>')
      w('Sorry, EnglishTokenizerDemo failed:<br>')
      w('<pre>%s</pre>' % err.decode('utf-8'))
      print(result)
    else:
      #print('got dot: %s' % dotString.decode('ascii'))
      print(result)
      i = result.find('DOT:')
      result = result[i+4:].strip()
      #print('dot: %s' % result)
      p = subprocess.Popen(['dot', '-Tpng'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
      pngData, err = p.communicate(result.encode('utf-8'))
      if p.returncode != 0:
        w('<br>')
        w('Sorry, dot failed:<br>')
        w('<pre>%s</pre>' % err.decode('utf-8'))
      else:
        w('<br><br>')
        w('<img src="data:image/png;base64,%s">' % base64.b64encode(pngData).decode('ascii'))
  w('</body>')
  w('</html>')

  html = ''.join(_l)

  headers = []
  headers.append(('Content-Type', 'text/html'))
  headers.append(('Content-Length', str(len(html))))

  startResponse('200 OK', headers)
  return [html.encode('utf-8')]

def main():
  port = 11000
  httpd = wsgiref.simple_server.make_server('0.0.0.0', port, application)
  print('Ready on port %s' % port)
  httpd.serve_forever()

if __name__ == '__main__':
  main()
