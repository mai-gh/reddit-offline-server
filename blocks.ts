// @deno-types="npm:@types/express@4"

//import { serve } from "https://raw.githubusercontent.com/denoland/deno_std/0.92.0/http/server.ts";


//import * as fsp from "https://deno.land/std@0.110.0/node/fs/promises.ts";
//import { ZstBlocksFile, type RowPosition } from "./old/zst_blocks_format/js_lib/src/ZstBlocksFile.ts";
import { ZstBlocksFile } from "./old/zst_blocks_format/js_lib/src/ZstBlocksFile.ts";
//import * as zb from "./old/zst_blocks_format/js_lib/src/ZstBlocksFile.ts";

interface RowPosition {
  blockOffset: number;
  rowIndex: number;
}

//const filePath = "XYZ.zst_blocks";
//const filePath = "./old/zst_blocks_format/python_cli/RS_2010-06.zst_blocks";
const filePath = "./RS_2010-06-c3.zst_blocks";
//const filePath = "./RS_2023-04.zst_blocks";
//const rowPosition = new RowPosition(18105854, 140);
//const rowPosition = new RowPosition(0x123456, 42);
//const rowPosition = RowPosition(0x123456, 42);
//const file = await fsp.open(filePath, "r");
const file = await Deno.open(filePath, { read: true, write: false });
try {
//  const row = await ZstBlocksFile.readBlockRowAt(file, rowPosition);
//{blockOffset:18105854, rowIndex: 140})
//  const row = await ZstBlocksFile.readBlockRowAt(file, {blockOffset:0x1, rowIndex: 42});
  const row = await ZstBlocksFile.readBlockRowAt(file, {blockOffset:1506161, rowIndex: 19});
  //const row = await ZstBlocksFile.readBlockRowAt(file, {blockOffset:16971663, rowIndex: 13});
  console.log("RRRR", row)
  const dd = new TextDecoder().decode(row).split('\n');
  console.log("DDDD0: ", dd[0])
  console.log("DDDD1: ", dd[1])
} finally {
//  await file.close();
}



import express, { NextFunction, Request, Response } from "npm:express@4.18.2";
import { marked } from "npm:marked@11.1.0";
import { DB } from "https://deno.land/x/sqlite/mod.ts";
const db = new DB("./old/zst_blocks_format/python_cli/reddit.db", { mode: "read" });
const app = express();
const port: Number = 3000;


const htmlTop: String = `
<!DOCTYPE html>
<html>
  <head>
    <style>
      .disable {
        pointer-events: none;
        color: #111;
      }
      .pageSelect {
        font-family: Arial;
        margin: .2em;
        text-decoration: none;
        font-size: 1em;
        padding: 3px 5px;
      }
    </style>
  </head>
  <body>
`;
const htmlBottom: String = `
  </body>
<html>
`;

//const buildComments = async (subreddit, parentId, depth = 0, out = "") => {  
const buildComments = async (parentId, blockOffset, rowIndex, filename, depth = 0, out = "") => {  
  depth++;
  //const comments = await db.query(`SELECT * FROM ${subreddit}_comments WHERE parent_id LIKE '%${parentId}';`);

//  const file = await Deno.open(filename, { read: true, write: false });
//  const row = await ZstBlocksFile.readBlockRowAt(file, {blockOffset, rowIndex});
  const comment_refs = await db.query(`SELECT parentid, postid, blockoffset, rowindex, filename FROM entries INNER JOIN files ON entries.fileid=files.id WHERE parentid LIKE '%${parentId}';`);
  out += "\n\n";
  for (let cr of comment_refs) {
//    console.log("CR:", cr)
    const [  id, postid, blockOffset, rowIndex, filename] = cr;
    const file = await Deno.open(filename, { read: true, write: false });
    const row = await ZstBlocksFile.readBlockRowAt(file, {blockOffset, rowIndex});
    const parsed_row = JSON.parse(new TextDecoder().decode(row))
//    console.log(parsed_row)
    const { author, created_utc, body} = parsed_row;
    out += `### **_${author} @ ${new Date(created_utc * 1000).toISOString()}:_**\n\n${body}`.replace(/^/gm, "> ".repeat(depth)) + '\n\n'
    out = await buildComments(postid, blockOffset, rowIndex, filename, depth, out);
  }
  depth--;
//  console.log(out)
  return out;
}

app.use((req: Request, res: Response, next: NextFunction) => {
  console.info(`${req.method} request to "${req.url}" by ${req.hostname}`);
  next();
});

app.get("/", async (_: Request, res: Response) => {
  let html = "";
  const subreddits = await db.query(`SELECT name FROM sqlite_schema WHERE type ='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '%\_comments%';`);
  html += htmlTop;
  html += '<ul>'
  for (let [sr] of subreddits) {
    sr = sr.replace(/_submissions$/gm, "");
    html += `<li><a href="/r/${sr}">${sr}</a></li>`
  }
  html += '</ul>' + htmlBottom;
  res.status(200).send(html);
});
app.get("/r", (req: Request, res: Response) => {
  res.redirect('/');
})

app.get("/r/:subreddit", async (req: Request, res: Response) => {
  try {
    const limit = 2000;
    const page = Number(req.query.p) || 0;
    let html = "";
    const [[total]] = await db.query(`SELECT COUNT(*) FROM ${req.params.subreddit}_submissions`);
    const submissions = await db.query(`SELECT title, permalink, author, created_utc, num_comments FROM ${req.params.subreddit}_submissions LIMIT ${limit} OFFSET ${page * limit};`);
    html += htmlTop;
    html += '<ul>'
    for (let s of submissions) {
      const [title, permalink, author, created_utc, num_comments] = s;
      html += `<li>${new Date(created_utc * 1000).toISOString()}  - ${num_comments} - <a href="${permalink}">${title}</a></li>`
    }
    html += '</ul>' +
            `<div style="width: 50%; margin: 0 auto;">` +
            `<a class="pageSelect ${page == 0 > total && 'disable'}" href="?p=0">&lt;&lt;</a>` + 
            `<a class="pageSelect ${page == 0 > total && 'disable'}" href="?p=${page - 1}">&lt;</a>` +
            `<span class="pageSelect">${(page * limit) + 1} - ${(page * limit) + limit > total ? total : (page * limit) + limit} of ${total}</span>` +
            `<a class="pageSelect ${(page * limit) + limit > total && 'disable'}" href="?p=${page + 1}">&gt;</a>` +
            `<a class="pageSelect ${(page * limit) + limit > total && 'disable'}" href="?p=${Math.floor(total / limit)}">&gt;&gt;</a>` +
            `</div>` +
            htmlBottom;
    res.status(200).send(html);
  } catch (ex) {
    console.log(ex)
    res.status(404).send("Not Found.");
  }
});

app.get("/r/:subreddit/comments/:submissionId/:urlTitle/", async (req: Request, res: Response) => {
  try {
    const [ [ postid, blockOffset, rowIndex, filename ] ] = await db.query(`SELECT entries.postid, entries.blockoffset, entries.rowindex, files.filename FROM entries INNER JOIN files ON entries.fileid=files.id WHERE postid='${req.params.submissionId}';`);
//    console.log(pos)

    const file = await Deno.open(filename, { read: true, write: false });
    const row = await ZstBlocksFile.readBlockRowAt(file, {blockOffset, rowIndex});
    const parsed_row = JSON.parse(new TextDecoder().decode(row))
//    console.log(JSON.stringify(parsed_row,null,2))
    const {permalink, title, url, author, created_utc, num_comments, selftext, id, name} = parsed_row;
    const commentsMD = await buildComments(id, blockOffset, rowIndex, filename);
//    res.status(200).send(new TextDecoder().decode(row));

//    const [ [ permalink, title, url, author, timestamp, num_comments, body, id, name] ] = await db.query(`SELECT * FROM ${req.params.subreddit}_submissions WHERE id='${req.params.submissionId}';`);
//    const commentsMD = await buildComments(req.params.subreddit, id);
    res.status(200).send(marked.parse(
      `# ${title}` + '\n\n' +
      `### By: ${author} at ${new Date(created_utc * 1000).toISOString()}` + '\n\n' +
      (new URL(url).pathname == permalink ? "" : url + '\n\n') + 
      `${selftext}` + '\n\n' +
      `${commentsMD}`
    ));
  } catch (ex) {
    console.log(ex)
    res.status(404).send("Not Found.");
  }
});

app.listen(port, () => {
  console.log(`Listening on ${port} ...`);
});
