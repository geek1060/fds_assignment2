#!/usr/bin/env python3

import sys
import json
import re
from datetime import datetime

discarded_records = {
    'invalid_timestamp': 0,
    'malformed_json': 0,
    'missing_fields': 0,
    'total_discarded': 0
}

timestamp_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z?$')


def validate_timestamp(timestamp):
    if not timestamp_pattern.match(timestamp):
        return False

    try:
        if timestamp.endswith('Z'):
            timestamp = timestamp[:-1]
        datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S')
        return True
    except ValueError:
        return False


def validate_json(json_str):
    try:
        json.loads(json_str)
        return True
    except json.JSONDecodeError:
        return False


for line in sys.stdin:
    try:
        fields = line.strip().split('\t')

        if len(fields) < 5:
            discarded_records['missing_fields'] += 1
            discarded_records['total_discarded'] += 1
            continue

        timestamp, user_id, action_type, content_id, metadata_json = fields[:5]

        if not validate_timestamp(timestamp):
            discarded_records['invalid_timestamp'] += 1
            discarded_records['total_discarded'] += 1
            continue

        if not validate_json(metadata_json):
            discarded_records['malformed_json'] += 1
            discarded_records['total_discarded'] += 1
            continue

        print(f"{user_id}\t{timestamp}\t{action_type}\t{content_id}\t{metadata_json}")

    except Exception as e:
        discarded_records['total_discarded'] += 1
        sys.stderr.write(f"Error processing line: {line.strip()}, Error: {str(e)}\n")

for counter_name, counter_value in discarded_records.items():
    sys.stderr.write(f"reporter:counter:DataQuality,{counter_name},{counter_value}\n")