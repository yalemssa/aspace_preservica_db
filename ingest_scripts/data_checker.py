#!/usr/bin/python3
#~/anaconda3/bin/python

from collections import defaultdict
import csv
import json
import os
from pathlib import Path
import shutil
import traceback
import warnings

from rich import print
from rich.progress import track

import filecmp

import _db_manager as db_manager
from _constants import console, progress
from _xml_tools import get_file_set, get_id_set
import ingest_data


def datatypes() -> set:
    return {'archival_object', 'collection', 'deliverable_unit', 'digital_file',
            'digital_object', 'digital_object_component', 'event', 'file_version',
            'hierarchies', 'manifestation', 'resource', 'restriction'}

def prep_query(datatype) -> str:
    return f"SELECT COUNT(*) FROM {datatype}"

def check_db_rows(db_cursor: db_manager.mysql.connector.cursor.MySQLCursor, datatypes: set) -> dict:
    '''Gets the number of rows in each table of the aspace_preservica_db'''
    data_dict = defaultdict(list)
    db_manager.use_database(db_cursor, 'aspace_preservica_db')
    for datatype in datatypes:
        query = prep_query(datatype)
        results = db_manager.run_query(db_cursor, query)
        data_dict[datatype].append(results[0][0])
    return data_dict

def get_dir_length(cfg: dict, file_type: str) -> int:
    # Remove the .replace after testing
    dirpath = f"{cfg.get('base_directory').replace('10', '09')}/xml_data/{file_type}"
    return len({item for item in track(os.listdir(dirpath)) if item not in ('.DS_Store', 'preservica_.xml', 'child_collections.csv')})

def check_directory_counts(cfg: dict, data_dict: dict) -> dict:
    '''Checking directory counts for Preservica data'''
    for key, value in data_dict.items():
        if f"{key}s" in ('collections', 'deliverable_units', 'digital_files', 'manifestations'):
            console.log(f'Getting directory counts for {key}s...')
            dir_length = get_dir_length(cfg, f"{key}s")
            data_dict[key].append(dir_length)
    return data_dict

def get_row_counts(fp: str) -> int:
    with open(fp, 'r', encoding='utf8') as datafile:
        csv_reader = csv.reader(datafile)
        next(csv_reader)
        return len(list(csv_reader))

def check_row_counts(cfg: dict, data_dict: dict) -> dict:
    for key, value in data_dict.items():
        if key in ('archival_object', 'digital_object', 'digital_object_component', 'event', 'file_version', 'hierarchies', 'resource', 'restriction'):
            # Remove the .replace after testing
            console.log(f'Getting row counts for {key}s...')
            dirpath = f"{cfg.get('base_directory').replace('10', '09')}/ingested/{key}_prepped.csv"
            row_count = get_row_counts(dirpath)
            data_dict[key].append(row_count)
    return data_dict

def compare_counts(data_dict: dict) -> dict:
    return {key: value for key, value in data_dict.items() if value[0] != value[1]}

def check_for_missing_data(cfg, data, db_cursor):
    data_values = check_db_rows(db_cursor, data)
    data_values = check_directory_counts(cfg, data_values)
    data_values = check_row_counts(cfg, data_values)
    compared = compare_counts(data_values)
    return compared

def get_diff_count(compared: dict) -> dict:
    return {key: value[1] - value[0] for key, value in compared.items()}

def query_diffs(db_cursor: db_manager.mysql.connector.cursor.MySQLCursor, compared: dict) -> dict:
    diff_dict = defaultdict(list)
    for key in compared.keys():
        console.log(f'Getting database id set for {key}s...')
        query = f"SELECT id FROM {key}"
        results = {item[0] for item in db_manager.run_query(db_cursor, query)}
        diff_dict[key].append(results)
    return diff_dict

def folder_diffs(cfg: dict, diff_dict: dict) -> dict:
    for key in diff_dict.keys():
        console.log(f'Getting folder id set for {key}s...')
        # Remove the .replace after testing  
        dirpath = f"{cfg.get('base_directory').replace('10', '09')}/xml_data/{key}s"
        file_set = get_file_set(dirpath)
        diff_dict[key].append(file_set)
    return diff_dict

def compare_diffs(diff_dict: dict) -> dict:
    for key, value in diff_dict.items():
        console.log(f'Getting diffs for {key}s...')
        in_db_not_folder = value[0] - value[1]
        in_folder_not_db = value[1] - value[0]
        diff_dict[key] = {'db_only': in_db_not_folder, 'folder_only': in_folder_not_db}
    return diff_dict

def write_to_file(cfg: dict, dataset: set, dtype: str, result_type: str) -> None:
    dirpath = f"{cfg.get('base_directory')}/{dtype}_missing_{result_type}.csv"
    prepped = [[row] for row in dataset]
    with open(dirpath, 'a', encoding='utf8') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(['id'])
        writer.writerows(prepped)
    return dirpath

def process_results(cfg, diff_dict: dict) -> None:
    for key, value in diff_dict.items():
        # db_only = value.get('db_only')
        # this shouldn't happen, right? not sure why it would??? All this is
        # really checking for is if the directory and CSV stuff got into the DB
        # think about whether this is true, and if so remove
        # What would i even do with a DB only result?
        # if db_only:
        #     dirpath = write_to_file(cfg, db_only, key, 'db_only')
        folder_only = value.get('folder_only')
        if folder_only:
            dirpath = write_to_file(cfg, folder_only, key, 'folder_only')
            return dirpath

def remediate_results(dirpath):
    '''Once we have the missing files, what do we do with them? How do we process
    the XML and add to the DB? Need to use the xml tools and ingest data functions, somehow.
    Also need to make sure that this will work for any data type, but that we can also
    select particular datatypes

    A bit stumped.
    '''
    for filename in os.listdir(dirpath):
        full_path = f"{dirpath}/{filename}"
        pass

def compare_files(cfg):
    old_base_dir = f"{cfg.get('last_ingest_base_directory')}/xml_data"
    print(old_base_dir)
    new_base_dir = f"{cfg.get('base_directory')}/xml_data"
    print(new_base_dir)
    # for now just these two, since there's nothing else really to compare them to...don't have another full set 
    # of manifestation or digital file data
    for datatype in ('collections', 'deliverable_units', 'manifestations', 'digital_files'):
        console.log(f'Comparing {datatype} files')
        old_dirpath = f"{old_base_dir}/{datatype}"
        new_dirpath = f"{new_base_dir}/{datatype}"
        old_file_set = {item for item in os.listdir(old_dirpath)}
        new_file_set = {item for item in os.listdir(new_dirpath)}
        just_in_old_set = old_file_set - new_file_set
        just_in_new_set = new_file_set - old_file_set
        common_files = new_file_set.intersection(old_file_set)
        results = filecmp.cmpfiles(new_dirpath, old_dirpath, common_files)
        console.log(f"""{datatype.upper()}: 
                            Number of files in old set: {len(old_file_set)}
                            Number of files in new set: {len(new_file_set)}
                            Files only in old set: {len(just_in_old_set)}
                            Files only in new set: {len(just_in_new_set)}
                            Number of common files: {len(common_files)}
                            Matches: {len(results[0])}
                            Mismatches: {len(results[1])}
                            Errors: {len(results[2])}""")

def compare_directories(old_dirpath, new_dirpath, datatype):
    old_file_set = {item for item in os.listdir(old_dirpath)}
    new_file_set = {item for item in os.listdir(new_dirpath)}
    just_in_old_set = old_file_set - new_file_set
    just_in_new_set = new_file_set - old_file_set
    console.log(f"""{datatype.upper()}: 
                        Number of files in old set: {len(old_file_set)}
                        Number of files in new set: {len(new_file_set)}
                        Files only in old set: {len(just_in_old_set)}
                        Files only in new set: {len(just_in_new_set)}""")
    return (just_in_old_set, just_in_new_set)

def missing_file_report(cfg):
    old_base_dir = f"{cfg.get('last_ingest_base_directory')}/xml_data"
    new_base_dir = f"{cfg.get('base_directory')}/xml_data"
    for datatype in ('collections', 'deliverable_units', 'manifestations', 'digital_files'):
        old_dirpath = f"{old_base_dir}/{datatype}"
        new_dirpath = f"{new_base_dir}/{datatype}"    
        old_set, new_set = compare_directories(old_dirpath, new_dirpath, datatype)
        write_to_file(cfg, old_set, datatype, f"in_{old_base_dir.split('/')[7]}_only")
        write_to_file(cfg, new_set, datatype, f"in_{new_base_dir.split('/')[7]}_only")

def find_missing_files(cfg, db_cursor):
    data = datatypes()
    results = check_for_missing_data(cfg, data, db_cursor)
    diff_count = get_diff_count(results)
    console.log(diff_count)
    diff_dictionary = query_diffs(db_cursor, results)
    diff_dictionary = folder_diffs(cfg, diff_dictionary)
    results = compare_diffs(diff_dictionary)
    result_dirpath = process_results(cfg, results)   

def main():
    with warnings.catch_warnings(), open('config.json', encoding='utf8') as cfg_file:
        warnings.simplefilter("ignore")
        try:
            cfg = json.load(cfg_file)
            mysql_instance, db_cursor, db_name = db_manager.connection_manager()
            find_missing_files(cfg, db_cursor)
            compare_files(cfg)
            missing_file_report(cfg)
        except Exception as exc:
            print(traceback.format_exc())
        finally:
            db_cursor.close()
            mysql_instance.close()

if __name__ == "__main__":
    main()
