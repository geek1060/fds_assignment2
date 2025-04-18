#!/usr/bin/env python3

import sys

current_user = None
profile_data = None
activity_data = None

for line in sys.stdin:
    try:
        user_id, tagged_data = line.strip().split('\t', 1)

        if '_' in user_id:
            user_id = user_id.split('_')[0]

        if user_id != current_user:
            if current_user is not None and profile_data is not None and activity_data is not None:
                print(f"{current_user}\t{profile_data}\t{activity_data}")

            current_user = user_id
            profile_data = None
            activity_data = None

        tag = tagged_data[0]
        data = tagged_data[2:]

        if tag == 'P':
            profile_data = data
        elif tag == 'A':
            activity_data = data

    except Exception as e:
        sys.stderr.write(f"Error processing line: {line.strip()}, Error: {str(e)}\n")

if current_user is not None and profile_data is not None and activity_data is not None:
    print(f"{current_user}\t{profile_data}\t{activity_data}")