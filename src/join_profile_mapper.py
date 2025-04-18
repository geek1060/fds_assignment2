#!/usr/bin/env python3

import sys
import os

skewed_keys_str = os.environ.get('skewed.keys', '')
skewed_keys = set(skewed_keys_str.split(',')) if skewed_keys_str else set()

NUM_SALTS = 10

for line in sys.stdin:
    try:
        fields = line.strip().split('\t', 1)

        if len(fields) >= 1:
            user_id_parts = fields[0].split(',')
            user_id = user_id_parts[0]  

            profile_data = fields[0]
            if len(fields) > 1:
                profile_data += "\t" + fields[1]

            if user_id in skewed_keys:
                for i in range(NUM_SALTS):
                    salted_key = f"{user_id}_{i}"
                    print(f"{salted_key}\tP:{profile_data}")
            else:
                print(f"{user_id}\tP:{profile_data}")

    except Exception as e:
        sys.stderr.write(f"Error processing line: {line.strip()}, Error: {str(e)}\n")