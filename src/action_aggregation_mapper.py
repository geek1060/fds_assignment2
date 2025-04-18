#!/usr/bin/env python3

import sys
from collections import defaultdict


user_action_counts = defaultdict(lambda: {'post': 0, 'like': 0, 'comment': 0, 'share': 0})

for line in sys.stdin:
    try:

        fields = line.strip().split('\t')

        if len(fields) >= 3:
            user_id = fields[0]
            action_type = fields[2].lower()

            if action_type in ['post', 'like', 'comment', 'share']:
                user_action_counts[user_id][action_type] += 1

    except Exception as e:
        sys.stderr.write(f"Error processing line: {line.strip()}, Error: {str(e)}\n")

for user_id, counts in user_action_counts.items():

    sort_key = f"{10000 - counts['post']:05d}"

    key = f"{user_id},{sort_key}"

    value = f"{counts['post']},{counts['like']},{counts['comment']},{counts['share']}"

    print(f"{key}\t{value}")