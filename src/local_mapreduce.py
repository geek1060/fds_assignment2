
import os
import sys
import subprocess
import argparse
from pathlib import Path
import time
import json

def run_map_reduce_job(mapper, reducer, input_path, output_path, combiner=None, env=None):
    start_time = time.time()

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    if os.path.isdir(input_path):
        print(f"Input is a directory: {input_path}")
        input_files = [os.path.join(input_path, f) for f in os.listdir(input_path)
                      if os.path.isfile(os.path.join(input_path, f))]

        temp_input = os.path.join(os.path.dirname(output_path), "temp_input.txt")
        with open(temp_input, 'w', encoding='utf-8') as outfile:
            for input_file in input_files:
                with open(input_file, 'r', encoding='utf-8') as infile:
                    outfile.write(infile.read())
                    outfile.write('\n')

        input_path = temp_input

    print(f"Running mapper: {mapper}")
    map_env = os.environ.copy()
    if env:
        map_env.update(env)

    with open(input_path, 'r', encoding='utf-8') as infile:
        map_process = subprocess.Popen(
            [sys.executable, mapper],
            stdin=infile,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=map_env
        )
        mapper_output, mapper_stderr = map_process.communicate()

    if mapper_stderr:
        print(f"Mapper stderr: {mapper_stderr}")

    if not mapper_output.strip():
        print("Warning: Mapper produced no output")
        with open(output_path, 'w', encoding='utf-8') as outfile:
            outfile.write("")
        return True

    print("Sorting mapper output")
    sorted_lines = sorted(mapper_output.strip().split('\n'))

    if combiner:
        print(f"Running combiner: {combiner}")
        combine_process = subprocess.Popen(
            [sys.executable, combiner],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=map_env
        )
        combiner_input = '\n'.join(sorted_lines)
        mapper_output, combiner_stderr = combine_process.communicate(input=combiner_input)

        if combiner_stderr:
            print(f"Combiner stderr: {combiner_stderr}")

        sorted_lines = sorted(mapper_output.strip().split('\n'))

    print(f"Running reducer: {reducer}")
    reduce_env = os.environ.copy()
    if env:
        reduce_env.update(env)

    reduce_process = subprocess.Popen(
        [sys.executable, reducer],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=reduce_env
    )
    reducer_input = '\n'.join(sorted_lines)
    reducer_output, reducer_stderr = reduce_process.communicate(input=reducer_input)

    if reducer_stderr:
        print(f"Reducer stderr: {reducer_stderr}")

    with open(output_path, 'w', encoding='utf-8') as outfile:
        outfile.write(reducer_output)

    elapsed_time = time.time() - start_time
    print(f"Job completed in {elapsed_time:.2f} seconds")
    print(f"Output written to {output_path}")

    return True

def run_join_job(activity_mapper, profile_mapper, reducer, activity_input, profile_input, output_path, env=None):

    start_time = time.time()

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    job_env = os.environ.copy()
    if env:
        job_env.update(env)

    print(f"Running activity mapper: {activity_mapper}")
    with open(activity_input, 'r', encoding='utf-8') as infile:
        activity_map_process = subprocess.Popen(
            [sys.executable, activity_mapper],
            stdin=infile,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=job_env
        )
        activity_mapper_output, activity_mapper_stderr = activity_map_process.communicate()

    if activity_mapper_stderr:
        print(f"Activity mapper stderr: {activity_mapper_stderr}")

    print(f"Running profile mapper: {profile_mapper}")
    with open(profile_input, 'r', encoding='utf-8') as infile:
        profile_map_process = subprocess.Popen(
            [sys.executable, profile_mapper],
            stdin=infile,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=job_env
        )
        profile_mapper_output, profile_mapper_stderr = profile_map_process.communicate()

    if profile_mapper_stderr:
        print(f"Profile mapper stderr: {profile_mapper_stderr}")

    combined_output = activity_mapper_output.strip() + '\n' + profile_mapper_output.strip()

    print("Sorting combined mapper output")
    sorted_lines = sorted(combined_output.split('\n'))

    print(f"Running join reducer: {reducer}")
    reduce_process = subprocess.Popen(
        [sys.executable, reducer],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=job_env
    )
    reducer_input = '\n'.join(sorted_lines)
    reducer_output, reducer_stderr = reduce_process.communicate(input=reducer_input)

    if reducer_stderr:
        print(f"Reducer stderr: {reducer_stderr}")

    with open(output_path, 'w', encoding='utf-8') as outfile:
        outfile.write(reducer_output)

    elapsed_time = time.time() - start_time
    print(f"Join job completed in {elapsed_time:.2f} seconds")
    print(f"Output written to {output_path}")

    return True

def run_skew_detection(input_path, output_path):

    print(f"Running skew detection on {input_path}")

    with open(input_path, 'r', encoding='utf-8') as infile:
        skew_process = subprocess.Popen(
            [sys.executable, os.path.join('src', 'skew_detection.py')],
            stdin=infile,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        skew_output, skew_stderr = skew_process.communicate()

    if skew_stderr:
        print(f"Skew detection stderr: {skew_stderr}")

    with open(output_path, 'w', encoding='utf-8') as outfile:
        outfile.write(skew_output)

    try:
        skew_data = json.loads(skew_output)
        skewed_keys = skew_data.get('skewed_keys', [])
        return ','.join(skewed_keys)
    except json.JSONDecodeError:
        print("Error parsing skew detection output")
        return ""

def main():
    parser = argparse.ArgumentParser(description="Local MapReduce Simulator")
    parser.add_argument('--job', required=True,
                        choices=['cleansing', 'aggregation', 'trending', 'join', 'all'],
                        help='Job name (cleansing, aggregation, trending, join, or all)')
    parser.add_argument('--input-dir', default='data',
                        help='Input directory (default: data)')
    parser.add_argument('--output-dir', default='output',
                        help='Output directory (default: output)')
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    base_dir = os.path.dirname(os.path.abspath(__file__))  


    if os.path.basename(base_dir).lower() == 'src':
        project_root = os.path.dirname(base_dir)
        src_dir = base_dir  
    else:
        project_root = base_dir
        src_dir = os.path.join(project_root, 'src')  

    print(f"Base directory: {base_dir}")
    print(f"Project root: {project_root}")
    print(f"Source directory: {src_dir}")

    input_dir = os.path.join(project_root, args.input_dir)
    output_dir = os.path.join(project_root, args.output_dir)


    if not os.path.exists(input_dir):
        print(f"Error: Input directory '{input_dir}' does not exist")
        return 1

    os.makedirs(output_dir, exist_ok=True)

    social_media_logs = os.path.join(input_dir, 'social_media_logs.txt')
    user_profiles = os.path.join(input_dir, 'user_profiles.txt')

    cleansed_data_output = os.path.join(output_dir, 'cleansed_data.txt')
    user_activity_output = os.path.join(output_dir, 'user_activity.txt')
    trending_content_output = os.path.join(output_dir, 'trending_content.txt')
    skew_analysis_output = os.path.join(output_dir, 'skew_analysis.json')
    joined_data_output = os.path.join(output_dir, 'joined_data.txt')

    cleansingMapper = os.path.join(src_dir, 'cleansing_mapper.py')
    cleansingReducer = os.path.join(src_dir, 'cleansing_reducer.py')

    actionAggregationMapper = os.path.join(src_dir, 'action_aggregation_mapper.py')
    actionAggregationReducer = os.path.join(src_dir, 'action_aggregation_reducer.py')

    trendingContentMapper = os.path.join(src_dir, 'trending_content_mapper.py')
    trendingContentCombiner = os.path.join(src_dir, 'trending_content_combiner.py')
    trendingContentReducer = os.path.join(src_dir, 'trending_content_reducer.py')

    joinActivityMapper = os.path.join(src_dir, 'join_activity_mapper.py')
    joinProfileMapper = os.path.join(src_dir, 'join_profile_mapper.py')
    joinReducer = os.path.join(src_dir, 'join_reducer.py')

    success = True

    if args.job == 'cleansing' or args.job == 'all':
        print("\n=== Running Data Cleansing Job ===")
        success = success and run_map_reduce_job(
            cleansingMapper,
            cleansingReducer,
            social_media_logs,
            cleansed_data_output
        )

    if (args.job == 'aggregation' or args.job == 'all') and success:
        print("\n=== Running Action Aggregation Job ===")
        success = success and run_map_reduce_job(
            actionAggregationMapper,
            actionAggregationReducer,
            cleansed_data_output if args.job == 'all' else args.input_dir,
            user_activity_output
        )

    if (args.job == 'trending' or args.job == 'all') and success:
        print("\n=== Running Trending Content Job ===")
        env = {'TRENDING_THRESHOLD': '-1'} 
        success = success and run_map_reduce_job(
            trendingContentMapper,
            trendingContentReducer,
            cleansed_data_output if args.job == 'all' else args.input_dir,
            trending_content_output,
            trendingContentCombiner,
            env
        )

    if (args.job == 'join' or args.job == 'all') and success:
        print("\n=== Running Data Join Job ===")

        skewed_keys = ""
        if args.job == 'all':
            skewed_keys = run_skew_detection(user_activity_output, skew_analysis_output)

        env = {'skewed.keys': skewed_keys}

        success = success and run_join_job(
            joinActivityMapper,
            joinProfileMapper,
            joinReducer,
            user_activity_output if args.job == 'all' else os.path.join(args.input_dir, 'user_activity.txt'),
            user_profiles,
            joined_data_output,
            env
        )

    if success:
        print("\n=== Workflow Completed Successfully ===")
        if args.job == 'all':
            print(f"Outputs written to {args.output_dir}/")
            print("Files:")
            print(f"  - {cleansed_data_output}")
            print(f"  - {user_activity_output}")
            print(f"  - {trending_content_output}")
            print(f"  - {joined_data_output}")
    else:
        print("\n=== Workflow Failed ===")
        return 1

    return 0

if __name__ == "__main__":
    sys.exit(main())