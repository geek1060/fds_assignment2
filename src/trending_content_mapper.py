#!/usr/bin/env python3

import sys
from collections import defaultdict

content_engagement = defaultdict(int)

for line in sys.stdin:
    try:
        fields = line.strip().split('\t')

        if len(fields) >= 4:
            content_id = fields[3]
            action_type = fields[2].lower()

            if action_type in ['like', 'share']:
                content_engagement[content_id] += 1

    except Exception as e:

        sys.stderr.write(f"Error processing line: {line.strip()}, Error: {str(e)}\n")

for content_id, engagement in content_engagement.items():
    print(f"{content_id}\t{engagement}")