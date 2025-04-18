#!/usr/bin/env python3

import sys
from collections import defaultdict

content_engagement = defaultdict(int)

for line in sys.stdin:
    try:
        content_id, engagement = line.strip().split('\t')
        content_engagement[content_id] += int(engagement)

    except Exception as e:
        sys.stderr.write(f"Error processing line: {line.strip()}, Error: {str(e)}\n")

for content_id, engagement in content_engagement.items():
    print(f"{content_id}\t{engagement}")