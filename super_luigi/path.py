__author__ = 'Sun'

import subprocess

def get_hadoop_files(path, hadoop_bin):


    clear_output_pattern = hadoop_bin + " fs -ls {path}"
    clear_output_str = clear_output_pattern.format(path = path)

    p = subprocess.Popen(clear_output_str, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    value, err = p.communicate()

    if p.returncode != 0 or err:
        raise Exception("path not found: " + path )

    raw_file_paths = value.split('\n')

    file_paths = []

    for raw_file_path in raw_file_paths:

        if raw_file_path.startswith("Found"):
            continue

        info_parts = filter(None,[part.strip() for part in raw_file_path.split()])

        if info_parts:
            file_path = info_parts[-1]
            file_paths.append(file_path)

    return file_paths