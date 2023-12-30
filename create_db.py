#!/usr/bin/env python3

import sys
import orjson as json
import zstandard
from typing import Iterator, Any, Iterable
import traceback
import sqlite3
con = sqlite3.connect("reddit.db")
cur = con.cursor()



def getZstFileJsonStream(path: str, chunk_size=1024*1024*10) -> Iterator[tuple[int, dict]]:
  dctx = zstandard.ZstdDecompressor(max_window_size=2147483648)
  currentString = ""
  def yieldLinesJson():
    nonlocal currentString
    lines = currentString.split("\n")
    currentString = lines[-1]
    for line in lines[:-1]:
      try:
        yield len(line), json.loads(line)
      except:
        #print("Error parsing line: " + line)
        #print("Current string: " + currentString)
        #traceback.print_exc()
        continue
  with open(path, 'rb') as ifh:
    reader = dctx.stream_reader(ifh)
    while True:
      try:
        chunk = reader.read(chunk_size)
      except zstandard.ZstdError:
        print("Error reading file: " + path)
        print(traceback.format_exc())
        break
      if not chunk:
        break
      currentString += chunk.decode("utf-8", "replace")
      
      for line in yieldLinesJson():
        yield line
  for line in yieldLinesJson():
    yield line
  if len(currentString) > 0:
    try:
      yield len(currentString), json.loads(currentString)
    except:
      print("Error parsing line: " + currentString)
      print(traceback.format_exc())
      pass

def processRowComments(row: dict[str, Any], basename: str):
  parent_id = json.dumps(row.get('parent_id')).decode('unicode_escape')
  author = json.dumps(row.get('author')).decode('unicode_escape')
  created_utc = str(row.get('created_utc'))
  id = json.dumps(row.get('name')).decode('unicode_escape')
  body = json.dumps(row.get('body').replace('"', '""')).decode('unicode_escape')

#  print(basename)
#  print(parent_id)
#  print(author)
#  print(created_utc)
#  print(name)
#  print(body)
  cur.execute(
    'INSERT INTO ' + basename + ' VALUES (' +
      parent_id + ',' +
      author + ',' +
      created_utc + ',' +
      id + ',' +
      body + ')'
  );

def processRowSubmissions(row: dict[str, Any], basename: str):
  permalink = json.dumps(row.get('permalink')).decode('unicode_escape')
  title = json.dumps(row.get('title').replace('"', '""')).decode('unicode_escape')
  url = json.dumps(row.get('url')).decode('unicode_escape')
  author = json.dumps(row.get('author')).decode('unicode_escape')
  created_utc = str(row.get('created_utc'))
  num_comments = str(row.get('num_comments'))
  selftext = json.dumps(row.get('selftext').replace('"', '""')).decode('unicode_escape')
  id = json.dumps(row.get('id')).decode('unicode_escape')
#  print(permalink)
#  print(title)
#  print(url)
#  print(author)
#  print(created_utc)
#  print(selftext)
#  print("name:", name)
#  print(basename)
  cur.execute(
    'INSERT INTO ' + basename + ' VALUES (' +
      permalink + ',' +
      title + ',' +
      url + ',' +
      author + ',' +
      created_utc + ',' +
      num_comments + ',' +
      selftext + ',' +
      id + ')'
  );


def processFile(path: str):
  basename = path.split('/')[-1].removesuffix('.zst')
  print(basename)
  jsonStream = getZstFileJsonStream(path)
  if jsonStream is None:
    print(f"Skipping unknown file {path}")
    return

  if basename.endswith('_submissions'):
    cur.execute('CREATE TABLE ' + basename + '(permalink TEXT, title TEXT, url TEXT, author TEXT, created_utc INTEGER, num_comments INTEGER, selftext TEXT, id TEXT)')
    for i, (lineLength, row) in enumerate(jsonStream):
      processRowSubmissions(row, basename)
  elif basename.endswith('_comments'):
    cur.execute('CREATE TABLE ' + basename + '( parent_id TEXT, author TEXT, created_utc INTEGER, id TEXT, body TEXT)')
    for i, (lineLength, row) in enumerate(jsonStream):
      processRowComments(row, basename)
  else:
    print(f"Error: {basename} does not end with '_submissions' or '_comments'")
    return
    

if __name__ == "__main__":
  for sr in sys.argv[1:]:
    processFile(f'{sr}_submissions.zst')
    processFile(f'{sr}_comments.zst')
  con.commit()

