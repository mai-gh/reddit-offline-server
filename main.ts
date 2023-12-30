// @deno-types="npm:@types/express@4"
import express, { NextFunction, Request, Response } from "npm:express@4.18.2";
import { marked } from "npm:marked@11.1.0";
import { DB } from "https://deno.land/x/sqlite/mod.ts";
const db = new DB("reddit.db", { mode: "read" });
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

const buildComments = async (subreddit, parentId, depth = 0, out = "") => {  
  depth++;
  const comments = await db.query(`SELECT * FROM ${subreddit}_comments WHERE parent_id LIKE '%${parentId}';`);
  out += "\n\n";
  for (let c of comments) {
    const [  , author, timestamp, id, body ] = c;
    out += `### **_${author} @ ${new Date(timestamp * 1000).toISOString()}:_**\n\n${body}`.replace(/^/gm, "> ".repeat(depth)) + '\n\n'
    
    out = await buildComments(subreddit, id, depth, out);
  }
  depth--;
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
    const [ [ permalink, title, url, author, timestamp, num_comments, body, id, name] ] = await db.query(`SELECT * FROM ${req.params.subreddit}_submissions WHERE id='${req.params.submissionId}';`);
    const commentsMD = await buildComments(req.params.subreddit, id);
    res.status(200).send(marked.parse(
      `# ${title}` + '\n\n' +
      `### By: ${author} at ${new Date(timestamp * 1000).toISOString()}` + '\n\n' +
      (new URL(url).pathname == permalink ? "" : url + '\n\n') + 
      `${body}` + '\n\n' +
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
