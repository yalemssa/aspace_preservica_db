#!/usr/bin/python3
#~/anaconda3/bin/python

# thread or process?
from concurrent.futures import ThreadPoolExecutor
import csv
import json
import os
import shutil
import traceback

from rich import print
from rich.progress import track

from data_checker import get_row_counts
from _constants import console, progress

'''CLEANUP FUNCTIONS'''

def report_on_removed_files(cfg):
    data_directory = cfg.get("base_data_directory")
    dupe_file_list = f"{data_directory}/duplicate_files.csv"
    print('Getting row counts...')
    rowcount = get_row_counts(dupe_file_list)
    print(f"Row count retrieved: {rowcount}")
    deleted_file_list = f"{data_directory}/deleted_files.csv"
    print('Retrieving all files...')
    all_all_filepaths = {os.path.join(dirpath, filename) for dirpath, dirnames, filenames in os.walk(data_directory)
                            for filename in filenames}
    print('All files retrieved...')
    with open(dupe_file_list, 'r', encoding='utf8') as infile, open(deleted_file_list, 'a', encoding='utf8') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        writer.writerow(['filename', 'filesize', 'filepath'])
        for row in track(reader, total=rowcount):
            fp_to_check = row[2]
            if fp_to_check not in all_all_filepaths:
                writer.writerow(row)

def remove_dupe_files(cfg):
    data_directory = cfg.get("base_data_directory")
    dupe_file_list = f"{data_directory}/duplicate_files.csv"
    print('Getting row counts...')
    rowcount = get_row_counts(dupe_file_list)
    print(f'Row count retrieved: {rowcount}')
    master_task = progress.add_task("overall", total=rowcount, filename="Overall Progress")
    with progress, open(dupe_file_list, 'r', encoding='utf8') as infile, ThreadPoolExecutor(max_workers=4) as pool:
        reader = csv.reader(infile)
        for row in reader:
            pool.submit(delete_files, row, master_task)

def delete_files(row: list, master_task):
    progress.advance(master_task, 1)
    filepath = row[2]
    if os.path.isfile(filepath):
        os.remove(filepath)

def get_file_data(filename: str, filepath: str) -> list:
    return [filename, os.path.getsize(filepath), filepath]

def get_all_filenames(filepath):
    return {filename: os.path.join(dirpath, filename) for dirpath, dirnames, filenames in os.walk(filepath)
                        for filename in filenames}

def set_paths(cfg):
    base_directory = cfg.get('base_directory')
    data_directory = cfg.get("base_data_directory")
    most_recent = f"{base_directory}/xml_data"
    duplicate_fp = f"{data_directory}/duplicate_files.csv"
    changed_fp = f"{data_directory}/changed_files.csv"
    not_in_fp = f"{data_directory}/not_in_most_recent.csv"
    return base_directory, data_directory, most_recent, duplicate_fp, changed_fp, not_in_fp

def check_similarity(dirpath, filename, all_filenames, base_directory, dupe_writer, change_writer, not_in_writer):
    # if (filename in all_filenames) and (base_directory not in dirpath):
    #     new_file_path = all_filenames.get(filename)
    #     new_file_data = get_file_data(filename, new_file_path)
    #     old_file_path = os.path.join(dirpath, filename)
    #     old_file_data = get_file_data(filename, old_file_path)
    #     if (old_file_data[0:2] == new_file_data[0:2]) and (old_file_path != new_file_path):
    #         old_file_data.extend(new_file_data[1:3])
    #         dupe_writer.writerow(old_file_data)
    #     elif (old_file_data[0:2] != new_file_data[0:2]):
    #         old_file_data.extend(new_file_data[1:3])
    #         change_writer.writerow(old_file_data)
    if filename not in all_filenames:
        old_file_path = os.path.join(dirpath, filename)
        old_file_data = get_file_data(filename, old_file_path)
        not_in_writer.writerow(old_file_data)


def find_dupe_files(cfg: dict) -> None:
    '''Finds identical files from previous ingests in preparation for deletion'''
    master_task = progress.add_task("overall", total=11038608, filename="Overall Progress")
    base_directory, data_directory, most_recent, duplicate_fp, changed_fp, not_in_fp = set_paths(cfg)
    print(most_recent)
    all_filenames = get_all_filenames(most_recent)
    with progress, open(duplicate_fp, 'a', encoding='utf8') as dupes, open(changed_fp, 'a', encoding='utf8') as changed, open(not_in_fp, 'a', encoding='utf8') as not_in:
        dupe_writer = csv.writer(dupes)
        change_writer = csv.writer(changed)
        not_in_writer = csv.writer(not_in)
        header_row = ['filename', 'old_size', 'old_full_path', 'new_size', 'new_full_path']
        dupe_writer.writerow(header_row)
        change_writer.writerow(header_row)
        not_in_writer.writerow(header_row)
        for dirpath, dirnames, filenames in os.walk(data_directory):
            for filename in filenames:
                progress.advance(master_task, 1)
                check_similarity(dirpath, filename, all_filenames, base_directory, dupe_writer, change_writer, not_in_writer)


def create_base_directory(cfg):
    base_directory = cfg.get("base_directory")
    if not os.path.isdir(base_directory):
        os.mkdir(base_directory)
    return base_directory

def create_directories(cfg: dict):
    base_directory = create_base_directory(cfg)
    for item in ('digital_files', 'manifestations', 'collections', 'deliverable_units', 'logs'):
        new_path = f"{base_directory}/{item}"
        if not os.path.isdir(new_path):
            os.mkdir(new_path)

def create_log_files(cfg: dict):
    pass

def set_config_values(cfg: dict, previous_ingest_value: str, current_ingest_value: str, new_ingest_value: str):
    '''Updates config file with new paths'''
    for key, value in cfg.items():
        if previous_ingest_value in str(value):
            new_value = value.replace(previous_ingest_value, current_ingest_value)
            cfg[key] = new_value
        if current_ingest_value in str(value):
            new_value = value.replace(current_ingest_value, new_ingest_value)
            cfg[key] = new_value
    return cfg

def update_config(cfg: dict, current_ingest_value: int):
    current_ingest_val = str(current_ingest_value).zfill(2)
    previous_ingest_value = str(current_ingest_value - 1).zfill(2)
    new_ingest_value = str(current_ingest_value + 1).zfill(2)
    new_config = set_config_values(cfg, previous_ingest_value, current_ingest_val, new_ingest_value)
    with open('config.json', 'w', encoding='utf-8') as outfile:
        json.dump(new_config, outfile, sort_keys=True, indent=4)

def mover(iterable, source_path, dest_path):
    print('Moving files...')
    for item in track(iterable):
        full_source_path = f"{source_path}/{item}"
        full_dest_path = f"{dest_path}/{item}"
        shutil.move(full_source_path, full_dest_path)

def get_files(fp: str):
    print('Getting files...')
    return [filename for filename in track(os.listdir(fp)) if filename != '.DS_Store']

def move_files(source_path, dest_path):
    data_files = get_files(source_path)
    mover(data_files, source_path, dest_path)

def move_manifestation_files(cfg: dict):
    print('Moving manifestation files to last ingest folder...')
    source_path = cfg.get("manifestation_output_folder")
    dest_path = cfg.get("last_ingest_manifestation_metadata_folder")
    move_files(source_path, dest_path)

def move_digital_files(cfg: dict):
    print('Moving digital files to last ingest folder...')
    source_path = cfg.get("digital_file_output_folder")
    dest_path = cfg.get("last_ingest_digital_file_metadata_folder")
    move_files(source_path, dest_path)

def get_folder(base_directory: str, folder_name: str):
    data_path = f"{base_directory}/{folder_name}"
    if not os.path.isdir(data_path):
        os.mkdir(data_path)
    return data_path

def move_dus_and_collections_to_xml_data(cfg: dict):
    print('Moving deliverable units and collections to XML data folder...')
    data_types = ('deliverable_units', 'collections', 'digital_files', 'manifestations')
    xml_data_path = get_folder(cfg.get("base_directory"), 'xml_data')
    mover(data_types, cfg.get("base_directory"), xml_data_path)

def move_dig_files_and_manifestations_to_xml_data(cfg: dict):
    print('Moving digital files and manifestations to XML data folder...')
    data_types = ('digital_files', 'manifestations')
    xml_data_path = get_folder(cfg.get("base_directory"), 'xml_data')
    last_ingest_directory = get_folder(cfg.get("last_ingest_base_directory"), 'xml_data')
    mover(data_types, last_ingest_directory, xml_data_path)

def move_reports_to_ingested_folder(cfg: dict):
    print('Moving reports to ingested folder...')
    base_directory = cfg.get("base_directory")
    filenames = [fname for fname in track(os.listdir(base_directory)) if fname.endswith('csv')]
    ingest_data_path = get_folder(base_directory, 'ingested')
    mover(filenames, base_directory, ingest_data_path)

def delete_empty_folders(cfg):
    data_types = ('digital_files', 'manifestations')
    base_directory = cfg.get('base_directory')        
    for data_type in data_types:
        full_path = f"{base_directory}/{data_type}"
        if not os.listdir(full_path):
            os.rmdir(full_path)

def run_cleanup(cfg: dict, current_ingest_version: int):
    '''Desired sequence of events:
    1.) Create a folder called xml_data in the most recent ingest folder, move deliverable units and collection folders there
    2.) Move files from the "new" digital file and manifestation folders into the digital file and manifestations
        folders from the previous ingest, which should contain ALL digital file and manifestation XML files
    3.) Move the digital file and manifestation folders from the previous ingest to the xml_data folder for the current ingest
    4.) Delete any empty folders as needed'''
    try:
        move_manifestation_files(cfg)
        move_digital_files(cfg)
        move_dus_and_collections_to_xml_data(cfg)
        move_dig_files_and_manifestations_to_xml_data(cfg)
        move_reports_to_ingested_folder(cfg)
        #delete_empty_folders(cfg)
        pass
    # updates for the next ingest
    finally:
        update_config(cfg, current_ingest_version)
        create_directories(cfg)

def main(ingest_version):
    try:
        with open('config.json') as cfg_file:
            cfg = json.load(cfg_file)
            run_cleanup(cfg, ingest_version)
            #find_dupe_files(cfg)
            # remove_dupe_files(cfg)
            #report_on_removed_files(cfg)
    except Exception as exc:
        print(traceback.format_exc())

if __name__ == "__main__":
    main(15)