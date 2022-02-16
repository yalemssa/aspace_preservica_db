#/usr/bin/python3

import json
import os
from lxml import etree
import warnings
from utilities import utilities as u
import _xml_tools as xml_tools
import _db_manager as db_manager
import _run_query as run_query
from rich.progress import track
import traceback
from utilities import db

def ingest_into_db(db_cursor, db_name, base_directory, datatypes):
    try:
        for datatype, values in datatypes.items():
            fpath = f"{base_directory}/{datatype}_prepped.csv"
            header_row, csvfile = u.opencsv(fpath)
            if datatype != 'digital_files_from_manifestations':
                if datatype in ('manifestation', 'digital_file'):
                    db_manager.update_db_table(db_cursor, db_name, csvfile, datatype, values[2])
                else:
                    db_manager.drop_and_update_db_table(db_cursor, db_name, csvfile, datatype, values[2])
    except Exception:
        print(traceback.format_exc())

def fix_fields(db_cursor, db_name, fix_field_query_path):
    db_manager.use_database(db_cursor, db_name)
    for filename in os.listdir(fix_field_query_path):
        if filename != '.DS_Store':
            full_path = f"{fix_field_query_path}/{filename}"
            query_string = run_query.open_query(full_path)
            db_manager.run_query(db_cursor, query_string)

def additional_processing_events(base_directory):
    event_path = f"{base_directory}/events_need_ids.csv"
    outfile_path = f"{base_directory}/event_prepped.csv"
    fileobject, csvoutfile = u.opencsvout(outfile_path)
    header_row, csvfile = u.opencsv(event_path)
    csvoutfile.writerow(header_row)
    for i, row in enumerate(csvfile, 1):
        row.insert(0, str(i))
        csvoutfile.writerow(row)

def additional_processing_digital_files(base_directory):
    digital_file_csv = f"{base_directory}/digital_file_needs_manifestation_ids.csv"
    digital_file_list = f"{base_directory}/digital_files_from_manifestations_prepped.csv"
    output_path = f"{base_directory}/digital_file_prepped.csv"
    xml_tools.update_dig_files_with_manifestations(digital_file_csv, digital_file_list, output_path)

def set_group_concat_len(query_path, dbconn):
    full_path = f"{query_path}/global_access.sql"
    print(full_path)
    query_string = run_query.open_query(full_path)
    dbconn.run_query_no_return(query_string)

def process_aspace_data(query_path, dbconn, base_directory, archivesspace_datatypes):
    set_group_concat_len(query_path, dbconn)
    for filename in os.listdir(query_path):
        datatype = filename.replace('.sql', '')
        if datatype in archivesspace_datatypes.keys():
            full_path = f"{query_path}/{filename}"
            query_string = run_query.open_query(full_path)
            results, header_row = dbconn.run_query_list(query_string)
            if datatype == 'event':
                outfile_path = f"{base_directory}/events_need_ids.csv"
            else:
                outfile_path = f"{base_directory}/{filename.replace('.sql', '')}_prepped.csv"
            run_query.write_outfile(outfile_path, header_row, results)

def process_preservica_data(base_directory, preservica_datatypes):
    for datatype, values in preservica_datatypes.items():
        if datatype != 'digital_files_from_manifestations':
            dirpath = f"{base_directory}/{datatype}s"
        # what's going on here?? why do I need this? what happens if it's not here?
        else:
            dirpath = f"{base_directory}/manifestations"
        if datatype != 'digital_file':
            output_dir = f"{base_directory}/{datatype}_prepped.csv"
        else:
            output_dir = f"{base_directory}/{datatype}_needs_manifestation_ids.csv"
        fileobject, csvoutfile = u.opencsvout(output_dir)
        csvoutfile.writerow(values[0])
        fileset = xml_tools.get_file_list(dirpath)
        prep_data(fileset, csvoutfile, values[1])
        fileobject.close()

def prep_data(fileset, csvoutfile, func):
    print(f"Running: {func}...")
    for item in track(fileset):
        tree = etree.parse(str(item))
        func(tree, csvoutfile)

def aspace_datatypes():
    return {'archival_object': [[], None, db_manager.prep_archival_objects], 'digital_object': [[], None, db_manager.prep_digital_objects], 'digital_object_component': [[], None, db_manager.prep_digital_object_components], 'event': [[], None, db_manager.prep_events], 'hierarchies': [[], None, db_manager.prep_hierarchies], 'restriction': [[], None, db_manager.prep_restrictions], 'resource': [[], None, db_manager.prep_resources], 'file_version': [[], None, db_manager.prep_file_versions]}

def pres_datatypes():
    return {
        'collection': [['identifier', 'parent_id', 'collection_code', 'security_tag', 'title', 'create_time', 'm_time'], xml_tools.collection_metadata, db_manager.prep_collections], 
        'deliverable_unit': [['identifier', 'collection_id', 'parent_id', 'root_parent', 'level', 'digital_surrogate', 'coverage_from', 'coverage_to', 'security_tag', 'create_time', 'm_time'], xml_tools.deliverable_unit_metadata, db_manager.prep_deliverable_units], 
        'digital_file': [['file_ref', 'file_set', 'file_size', 'file_name', 'last_mod', 'working_path'], xml_tools.digital_file_metadata, db_manager.prep_digital_files], 
        'manifestation': [['identifier', 'deliverable_unit_id', 'typeref', 'create_time', 'm_time'], xml_tools.manifestation_metadata, db_manager.prep_manifestations],
        'digital_files_from_manifestations': [['manifestation_id', 'type', 'digital_file_id'], xml_tools.get_digital_file_list_from_manifestations]
        }

def main():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        try:
            dbconn = db.DBConn(config_file='config.json', typejson=True)
            mysql_instance, db_cursor, db_name = db_manager.connection_manager()
            cfg = json.load(open('config.json'))
            base_directory = cfg.get('base_directory')
            aspace_query_path = cfg.get('aspace_query_path')
            fix_field_query_path = cfg.get('fix_field_query_path')
            #db_manager.initialize(db_cursor, db_name)
            preservica_datatypes = pres_datatypes()
            archivesspace_datatypes = aspace_datatypes()
            process_preservica_data(base_directory, preservica_datatypes)
            additional_processing_digital_files(base_directory)
            process_aspace_data(aspace_query_path, dbconn, base_directory, archivesspace_datatypes)
            additional_processing_events(base_directory)
            ingest_into_db(db_cursor, db_name, base_directory, preservica_datatypes)
            ingest_into_db(db_cursor, db_name, base_directory, archivesspace_datatypes)
            fix_fields(db_cursor, db_name, fix_field_query_path)
        finally:
            dbconn.close_conn()
            mysql_instance.commit()
            db_cursor.close()
            mysql_instance.close()


if __name__ == "__main__":
    main()