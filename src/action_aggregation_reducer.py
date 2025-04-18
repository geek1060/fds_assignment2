#!/usr/bin/env python3

import sys

current_user = None

for line in sys.stdin:
    try:
        key, value = line.strip().split('\t')

        user_id = key.split(',')[0]

        current_user = user_id

        post_count, like_count, comment_count, share_count = map(int, value.split(','))

        output = f"{user_id}\tposts:{post_count},likes:{like_count},comments:{comment_count},shares:{share_count}"
        print(output)

    except Exception as e:
        sys.stderr.write(f"Error processing line: {line.strip()}, Error: {str(e)}\n")