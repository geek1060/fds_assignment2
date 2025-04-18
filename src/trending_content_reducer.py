#!/usr/bin/env python3

import sys
import os
import numpy as np

threshold = int(os.environ.get('TRENDING_THRESHOLD', -1))

all_engagements = []
content_data = []

for line in sys.stdin:
    try:
        content_id, engagement = line.strip().split('\t')
        engagement = int(engagement)

        all_engagements.append(engagement)
        content_data.append((content_id, engagement))

    except Exception as e:
        sys.stderr.write(f"Error processing line: {line.strip()}, Error: {str(e)}\n")

if threshold < 0:
    if all_engagements:
        threshold = np.percentile(all_engagements, 90)
    else:
        threshold = 0
sys.stderr.write(f"reporter:counter:TrendingStats,ThresholdUsed,{int(threshold)}\n")

for content_id, engagement in content_data:
    if engagement >= threshold:
        print(f"{content_id}\t{engagement}")