#!/usr/bin/python3
#~/anaconda3/bin/python

import csv
from concurrent.futures import ProcessPoolExecutor
from operator import itemgetter
from collections import Counter, defaultdict
from datetime import datetime
import json
from lxml import etree
import os
import pprint
import traceback
from utilities import utilities as u

from rich import print
from rich.progress import track
from rich.padding import Padding

from _constants import console, progress


def process_entries(collection_id, collection_code, collection_title, entries):
    '''Use this function in other functions - abstraction'''
    entry_data = []
    for element in entries:
        identifier = element.find("{http://www.w3.org/2005/Atom}id").text
        title = element.find("{http://www.w3.org/2005/Atom}title").text
        link = element.find("{http://www.w3.org/2005/Atom}link")
        link_type = link.attrib.get('rel')
        link_href = link.attrib.get('href')
        entry_data.append([identifier, collection_id, collection_code, collection_title, title, link_href, link_type])
    return entry_data

def get_entry_data(file_list, entry_type, dirpath):
    '''Recursively parses entry metadata and extracts identifiers and links'''
    entry_data = defaultdict(set)
    for item in track(file_list):
        try:
            tree = etree.parse(item)
            entries = tree.iter("{http://www.w3.org/2005/Atom}entry")
            for element in entries:
                identifier = element.find("{http://www.w3.org/2005/Atom}id").text
                link = element.find("{http://www.w3.org/2005/Atom}link")
                if entry_type in link.attrib.get('rel'):
                    entry_data[identifier].add(tuple([identifier, link.attrib.get('href'), item.replace(f"{dirpath}/", '').replace('preservica_', '').replace('.xml', '')]))
        except Exception:
            print(traceback.format_exc())
            continue
    print(len(entry_data))
    return entry_data
    #return [tuple(value)[0] for key, value in entry_data.items()]

def count_entries(file_list, entry_type, dirpath):
    entry_data = set()
    for item in track(file_list):
        tree = etree.parse(item)
        entries = tree.iter("{http://www.w3.org/2005/Atom}entry")
        for element in entries:
            identifier = element.find("{http://www.w3.org/2005/Atom}id").text
            link = element.find("{http://www.w3.org/2005/Atom}link")
            if entry_type in link.attrib.get('rel'):
                entry_data.add(identifier)
    print(len(entry_data))
    return entry_data

def get_path_set(input_path):
    return {f"{input_path}/{item}" for item in os.listdir(input_path) if item not in ('.DS_Store', 'child_collections.csv', 'preservica_.xml')}

def get_file_set(input_path):
    return {item.replace('preservica_', '').replace('.xml', '') for item in os.listdir(input_path) if item not in ('.DS_Store', 'child_collections.csv', 'preservica_.xml')}

def get_id_set(input_path):
    return {item[0] for item in u.opencsv(input_path)[1]}

def combine_csv_data(input_path_1, input_path_2):
    csv_one = get_id_set(input_path_1)
    csv_two = get_id_set(input_path_2)
    combined = csv_one | csv_two
    return [[item] for item in combined]

def compare_folders(input_path, entry_list):
    csv_set = {item[0] for item in entry_list}
    file_set = {item.replace('preservica_', '').replace('.xml', '') for item in os.listdir(input_path) if item != '.DS_Store'}
    diffs = csv_set - file_set
    return [[item] for item in diffs]

def get_entries(input_path, entry_type):
    folder_path_ready = {f"{input_path}/{item}" for item in os.listdir(input_path) if item not in ('.DS_Store', 'child_collections.csv', 'preservica_.xml')}
    entry_dataset = get_entry_data(folder_path_ready, entry_type, input_path)
    processed_data = [tuple(value)[0] for key, value in entry_dataset.items()]
    return processed_data

def get_digital_files_from_dus(cfg):
    return get_entries(cfg.get('deliverable_unit_output_folder'), 'digital-file')

def get_manifestations_from_dus(cfg):
    return get_entries(cfg.get('deliverable_unit_output_folder'), 'manifestation')

def get_dus_from_collections(cfg):
    return get_entries(cfg.get('collection_output_folder'), 'deliverable-unit')

def get_new_manifestations(cfg):
    manifestation_data = get_manifestations_from_dus(cfg)
    compared_folders = compare_folders(cfg.get('last_ingest_manifestation_metadata_folder'), manifestation_data)
    print(len(compared_folders))
    return compared_folders

def get_new_digital_files(cfg):
    digital_file_data = get_digital_files_from_dus(cfg)
    compared_folders = compare_folders(cfg.get('last_ingest_digital_file_metadata_folder'), digital_file_data)
    return compared_folders

'''
Functions to prepare database tables

'''

def get_file_list(dirpath):
    return {f"{dirpath}/{item}" for item in os.listdir(dirpath) if item not in ('.DS_Store', 'preservica_.xml', 'child_collections.csv')}

def get_digital_file_list_from_manifestations(tree, csvoutfile):
    '''
    This goes through manifestation files and pulls digital file and type data
    '''
    man_identifier = tree.findall(".//{http://www.tessella.com/XIP/v4}DeliverableUnits/{http://www.tessella.com/XIP/v4}Manifestation/{http://www.tessella.com/XIP/v4}ManifestationRef")[0].text
    man_type = tree.findall(".//{http://www.tessella.com/XIP/v4}DeliverableUnits/{http://www.tessella.com/XIP/v4}Manifestation/{http://www.tessella.com/XIP/v4}TypeRef")[0].text
    entries = tuple(tree.iter("{http://www.w3.org/2005/Atom}entry"))
    for element in entries:
        links = element.findall("{http://www.w3.org/2005/Atom}link")
        if links:
            for link in links:
                if link.attrib.get('rel') == 'digital-file':
                    dig_identifier = element.find("{http://www.w3.org/2005/Atom}id").text
                    csvoutfile.writerow([man_identifier, man_type, dig_identifier])

def convert_to_defaultdict(csvfile):
    manifestation_dict = defaultdict(list)
    for row in csvfile:
        manifestation_dict[row[2]].append([row[0], row[1]])
    return manifestation_dict

def get_row_length(fp):
    with open(fp) as csvin:
        return len(list(csv.reader(csvin)))

def update_dig_files_with_manifestations(fp1, fp2, fp3):
    '''Adds manifestation IDs to digital file table CSV'''
    # the digital file table CSV - to be uploaded to database
    try:
        #digital file csv
        header_row1, dig_file_csv = u.opencsv(fp1)
        # the CSV file with manifestation IDs matched with digital files; created by get_digital_file_list_from_manifestations func
        header_row2, manifestation_csv = u.opencsv(fp2)
        fileobject, csvoutfile = u.opencsvout(fp3)
        row_len = get_row_length(fp1)
        csvoutfile.writerow(header_row1 + ['manifestation', 'type'])
        manifestation_d = convert_to_defaultdict(manifestation_csv)
        for row in track(dig_file_csv, total=row_len):
            if row[0] in manifestation_d:
                #some digital files have multiple manifestations - just pick the first one,
                #for simplicity's sake; these are almost always metadata and images
                #of containers, etc.
                first_item_only = sorted(manifestation_d[row[0]], key=itemgetter(1))[0]
                row.extend(first_item_only)
            # outside of the conditional so that the files will be written to CSV even if
            # there is no manifestation (only 2 that I know of)
            #print(row)
            csvoutfile.writerow(row)
    except:
        print(traceback.format_exc())
    finally: 
        fileobject.close()

def get_root_deliverable_unit_parent(tree, parent_id):
    entries = tuple(tree.iter("{http://www.w3.org/2005/Atom}entry"))
    parents = []
    for element in entries:
        #there should only be one link per entry - that I care about, at least
        links = element.findall("{http://www.w3.org/2005/Atom}link")
        if links:
            for link in links:
                if 'parent-deliverable-unit' in link.attrib.get('rel'):
                    identifier = element.find("{http://www.w3.org/2005/Atom}id").text
                    relator = link.attrib.get('rel').replace('parent-deliverable-unit-level-', '')
                    parents.append((identifier, relator))
    #print(parents)
    if parents:
    #   #sorts the list by the highest number then returns the first value
        return sorted(parents, key=itemgetter(1), reverse=True)[0]
    else:
        return [parent_id, '0']

def collection_metadata(root, csvoutfile):
    identifier = root.findall(".//{http://www.tessella.com/XIP/v4}Collections/{http://www.tessella.com/XIP/v4}Collection/{http://www.tessella.com/XIP/v4}CollectionRef")[0].text
    parent_id = root.find(".//{http://www.tessella.com/XIP/v4}Collections/{http://www.tessella.com/XIP/v4}Collection/{http://www.tessella.com/XIP/v4}ParentRef")
    if parent_id != None:
        parent_id = parent_id.text
    else:
        parent_id = ''
    collection_code = root.findall(".//{http://www.tessella.com/XIP/v4}Collections/{http://www.tessella.com/XIP/v4}Collection/{http://www.tessella.com/XIP/v4}CollectionCode")[0].text
    security_tag = root.findall(".//{http://www.tessella.com/XIP/v4}Collections/{http://www.tessella.com/XIP/v4}Collection/{http://www.tessella.com/XIP/v4}SecurityTag")[0].text
    title = root.findall(".//{http://www.tessella.com/XIP/v4}Collections/{http://www.tessella.com/XIP/v4}Collection/{http://www.tessella.com/XIP/v4}Title")[0].text
    create_time = str(datetime.now())
    m_time = str(datetime.now())
    csvoutfile.writerow([identifier, parent_id, collection_code, security_tag, title, create_time, m_time])

def deliverable_unit_metadata(root, csvoutfile):
    identifier = root.findall(".//{http://www.tessella.com/XIP/v4}DeliverableUnits/{http://www.tessella.com/XIP/v4}DeliverableUnit/{http://www.tessella.com/XIP/v4}DeliverableUnitRef")[0].text
    collection_id = root.findall(".//{http://www.tessella.com/XIP/v4}DeliverableUnits/{http://www.tessella.com/XIP/v4}DeliverableUnit/{http://www.tessella.com/XIP/v4}CollectionRef")[0].text
    parent_id = root.find(".//{http://www.tessella.com/XIP/v4}DeliverableUnits/{http://www.tessella.com/XIP/v4}DeliverableUnit/{http://www.tessella.com/XIP/v4}ParentRef")
    if parent_id != None:
        parent_id = parent_id.text
    else:
        parent_id = ''
    root_parent, level = get_root_deliverable_unit_parent(root, parent_id)
    digital_surrogate = root.findall(".//{http://www.tessella.com/XIP/v4}DeliverableUnits/{http://www.tessella.com/XIP/v4}DeliverableUnit/{http://www.tessella.com/XIP/v4}DigitalSurrogate")[0].text
    coverage_from = root.findall(".//{http://www.tessella.com/XIP/v4}DeliverableUnits/{http://www.tessella.com/XIP/v4}DeliverableUnit/{http://www.tessella.com/XIP/v4}CoverageFrom")[0].text
    coverage_to = root.findall(".//{http://www.tessella.com/XIP/v4}DeliverableUnits/{http://www.tessella.com/XIP/v4}DeliverableUnit/{http://www.tessella.com/XIP/v4}CoverageTo")[0].text
    security_tag = root.findall(".//{http://www.tessella.com/XIP/v4}DeliverableUnits/{http://www.tessella.com/XIP/v4}DeliverableUnit/{http://www.tessella.com/XIP/v4}SecurityTag")[0].text
    create_time = str(datetime.now())
    m_time = str(datetime.now())
    csvoutfile.writerow([identifier, collection_id, parent_id, root_parent, level, digital_surrogate, coverage_from, coverage_to, security_tag, create_time, m_time])

def manifestation_metadata(root, csvoutfile):
    identifier = root.findall(".//{http://www.tessella.com/XIP/v4}DeliverableUnits/{http://www.tessella.com/XIP/v4}Manifestation/{http://www.tessella.com/XIP/v4}ManifestationRef")[0].text
    deliverable_unit_id = root.find(".//{http://www.tessella.com/XIP/v4}DeliverableUnits/{http://www.tessella.com/XIP/v4}Manifestation/{http://www.tessella.com/XIP/v4}DeliverableUnitRef").text
    typeref = root.findall(".//{http://www.tessella.com/XIP/v4}DeliverableUnits/{http://www.tessella.com/XIP/v4}Manifestation/{http://www.tessella.com/XIP/v4}TypeRef")[0].text
    create_time = str(datetime.now())
    m_time = str(datetime.now())
    csvoutfile.writerow([identifier, deliverable_unit_id, typeref, create_time, m_time])

def digital_file_metadata(root, csvoutfile):
    #these all only have one entry
    try:
        file_size = root.findall(".//{http://www.tessella.com/XIP/v4}Files/{http://www.tessella.com/XIP/v4}File/{http://www.tessella.com/XIP/v4}FileSize")[0].text
        file_name = root.findall(".//{http://www.tessella.com/XIP/v4}Files/{http://www.tessella.com/XIP/v4}File/{http://www.tessella.com/XIP/v4}FileName")[0].text
        file_set = root.findall(".//{http://www.tessella.com/XIP/v4}Files/{http://www.tessella.com/XIP/v4}File/{http://www.tessella.com/XIP/v4}IngestedFileSetRef")[0].text
        file_ref = root.findall(".//{http://www.tessella.com/XIP/v4}Files/{http://www.tessella.com/XIP/v4}File/{http://www.tessella.com/XIP/v4}FileRef")[0].text
        last_mod = root.find(".//{http://www.tessella.com/XIP/v4}Files/{http://www.tessella.com/XIP/v4}File/{http://www.tessella.com/XIP/v4}LastModifiedDate")
        if last_mod is not None:
            last_mod = last_mod.text
        else:
            last_mod = '1999-01-01T01:01:00.000-04:00'
        working_path = root.findall(".//{http://www.tessella.com/XIP/v4}Files/{http://www.tessella.com/XIP/v4}File/{http://www.tessella.com/XIP/v4}WorkingPath")[0].text
        newrow = [file_ref, file_set, file_size, file_name, last_mod, working_path]
        #these could have more
        format_puids = root.findall(".//{http://www.tessella.com/XIP/v4}Files/{http://www.tessella.com/XIP/v4}File/{http://www.tessella.com/XIP/v4}FormatInfo/{http://www.tessella.com/XIP/v4}FormatPUID")
        puids = [puid.text for puid in format_puids]
        newrow.append(puids)
        format_names = root.findall(".//{http://www.tessella.com/XIP/v4}Files/{http://www.tessella.com/XIP/v4}File/{http://www.tessella.com/XIP/v4}FormatInfo/{http://www.tessella.com/XIP/v4}FormatName")
        form_names = [form_name.text for form_name in format_names]
        newrow.append(form_names)
        csvoutfile.writerow(newrow)
    except Exception:
        print(file_name)
        print(file_ref)
        print(traceback.format_exc())

def main():
    try:
        cfg = json.load(open('config.json'))
        csv_1 = cfg.get('input_csv')
        csv_2 = cfg.get('input_csv_2')
        update_dig_files_with_manifestations(csv_1, csv_2, cfg.get('output_csv'))
    except Exception as exc:
        print(item)
        print(traceback.format_exc())
    finally:
        pass
        #fileobject.close()

if __name__ == "__main__":
    main()