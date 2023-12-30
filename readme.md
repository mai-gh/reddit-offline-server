## Reddit-Offline-Server

this project aims to use reddit data dumps from https://libreddit.oxymagnesium.com/r/pushshift/comments/11ef9if/separate_dump_files_for_the_top_20k_subreddits/ to enable offline viewing similar to [kiwix](https://kiwix.org)'s offline stackexchange wikipedia collections.


### How To

1. download the subreddit submissions/comments zst files you are interested in.
2. clone this repo && cd into it
3. generate a sqlite db with `rm reddit.db; python create_db.py $SUBREDDITNAME`
4. start the server with `deno task dev`
5. browse to `http://127.0.0.1:3000/r/SUBREDDITNAME`
