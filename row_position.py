import sys
#import orjson as json
import json
from ZstBlocksFile import ZstBlocksFile

import sqlite3
con = sqlite3.connect("reddit.db")
cur = con.cursor()

cur.execute('PRAGMA foreign_keys=on')
cur.execute('CREATE TABLE files (id INTEGER PRIMARY KEY ASC, filename VARCHAR)')
con.commit()
#cur.execute("CREATE TABLE submissions (fileid INTEGER, postid VARCHAR, blockoffset INTEGER, rowindex INTEGER, FOREIGN KEY('fileid') REFERENCES files(id))")
#cur.execute("CREATE TABLE comments (fileid INTEGER, postid VARCHAR, blockoffset INTEGER, rowindex INTEGER, parentid VARCHAR, FOREIGN KEY('fileid') REFERENCES files(id))")
cur.execute("CREATE TABLE entries (fileid INTEGER, postid VARCHAR, blockoffset INTEGER, rowindex INTEGER, parentid VARCHAR, FOREIGN KEY('fileid') REFERENCES files(id))")


# python zst_blocks.py encode -t -i ../../../RS_2010-06.ndjson -o ../../../RS_2010-06-c3.zst_blocks
## after fix to allow text from stdin
# zstd -dc --long=31 ../../../RS_2010-06.zst | python zst_blocks.py encode -t --stdin  -o ../../../RS_2010-06-c4.zst_blocks

file_id = -1
for filename in sys.argv[1:]:
  file_id += 1
  cur.execute(f"INSERT INTO files VALUES ({file_id}, '{filename.split('/')[-1]}')")

  tablename = "submissions" if filename.startswith("RS") else "comments"

  with open(filename, "rb") as f:
    #rowPositions = list(ZstBlocksFile.generateRowPositions(f))
  #  rowPositions = ZstBlocksFile.streamRowsWithPositions(f)
  #  print(rowPositions)
    count = 0
  #  for rp in rowPositions:
    for row, pos in ZstBlocksFile.streamRowsWithPositions(f):
      parsed_row = json.loads(row)
      parentid = parsed_row['parent_id'] if 'parent_id' in parsed_row else None
      #print(json.loads(row)['id'], pos)
#      cur.execute("INSERT INTO entries VALUES (" +
#        str(file_id) + ',' +
#        "'" + parsed_row['id'] +"'"+ ',' +
#        str(pos.blockOffset) + ',' +
#        str(pos.rowIndex) +
#        '?' + ')', parentid
#      )

      cur.execute("INSERT INTO entries "
        "VALUES (?, ?, ?, ?, ?)",
        (file_id, parsed_row['id'], pos.blockOffset, pos.rowIndex, parentid)
      )
#    pass
#    print("---------------------------------------------")
#    print(pos)
#    print("*^*^*^*^*^*^*^*^*")
#    print(row)
#    print("---------------------------------------------")

#    print(count)
#    print(rp)

con.commit()

#rows = []

#with open(sys.argv[1], "rb") as f:
#  for row in ZstBlocksFile.streamRows(f):
#    rows.append(row)
    #print(len(row))
    #sys.exit()
#
#print(rows[1])

