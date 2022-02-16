#/usr/bin/python3

from datetime import datetime
import traceback
import mysql.connector
from mysql.connector import errorcode
from utilities import utilities as u

def override_foreign_key_contstraint(cursor):
    try:
        cursor.execute(
            f"SET FOREIGN_KEY_CHECKS=0")
        print(f'Overriding foreign key constraints')
    except mysql.connector.Error as err:
        print(f"Failed overriding foreign key constraint: {err}")
        exit(1) 

def use_database(cursor, db_name):
    try:
        cursor.execute(
            f"USE {db_name}")
        print(f'using {db_name}')
    except mysql.connector.Error as err:
        print(f"Failed switching to database: {err}")
        exit(1) 

def drop_table(cursor, table_name):
    try:
        cursor.execute(
            f"DROP TABLE IF EXISTS {table_name}")
        print(f'dropped table {table_name}')
    except mysql.connector.Error as err:
        print(f"Failed dropping database: {err}")
        exit(1)

def drop_database(cursor, db_name):
    try:
        cursor.execute(
            f"DROP DATABASE IF EXISTS {db_name}")
        print(f'dropped database {db_name}')
    except mysql.connector.Error as err:
        print(f"Failed dropping table: {err}")
        exit(1)

def create_database(cursor, db_name):
    try:
        cursor.execute(
            f"CREATE DATABASE {db_name} DEFAULT CHARACTER SET 'utf8mb4'")
        print(f'created database {db_name}')
    except mysql.connector.Error as err:
        print(f"Failed creating database: {err}")
        exit(1)

def create_tables(cursor, db_name, table_dict):
    try:
        for table_name, table_values in table_dict.items():
            print(f'Creating table {table_name}')
            cursor.execute(table_values)
            print(f'Table created {table_name}')
    except mysql.connector.Error as err:
        print(f"Failed creating table: {err}")
        exit(1)

def create_table(cursor, table_name):
    try:
        print(f'Creating table {table_name}')
        query = database_tables().get(table_name)
        cursor.execute(query)
        print(f'Table created {table_name}')
    except mysql.connector.Error as err:
        print(f"Failed creating table: {err}")
        exit(1)       

def alter_table(cursor, db_name, updated_data):
    pass

def insert_data(cursor, table_name, row, column_names=''):
    try:
        print(f"INSERT INTO {table_name} {column_names} VALUES {row}")
        cursor.execute(
            f"INSERT INTO {table_name} {column_names} VALUES {row}")
    except mysql.connector.Error as err:
        print(f"Failed inserting data: {err}")
        exit(1)

def run_query(cursor, query_string):
    try:
        #print(f"Executing query")
        cursor.execute(query_string)
        return cursor.fetchall()
    except mysql.connector.Error as err:
        print(f"Query failed: {err}")
        exit(1)    

def database_tables():
    '''Not sure if this should continue to be hardcoded, or should pull a spreadsheet schema??'''
    tables = {}
    tables['resource'] = ("""
        CREATE TABLE `resource` (
            `id` int(11) NOT NULL,
            `call_number` varchar(255) NOT NULL,
            `title` varchar(8704) NOT NULL,
            `publish` int(11) NOT NULL,
            `suppressed` int(11) NOT NULL,
            `create_time` timestamp NOT NULL,
            `m_time` timestamp NOT NULL,
        PRIMARY KEY (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
        """)
    tables['collection'] = ("""
        CREATE TABLE `collection` (
            `id` varchar(255) NOT NULL,
            `parent_collection_id` varchar(255) DEFAULT NULL,
            `collection_code` varchar(255) DEFAULT NULL,
            `security_tag` varchar(255) DEFAULT NULL,
            `title` varchar(8704) NOT NULL,
            `create_time` timestamp NOT NULL,
            `m_time` timestamp NOT NULL,
        PRIMARY KEY (`id`),
        KEY `parent_collection_id` (`parent_collection_id`),
        CONSTRAINT `collection_ibfk_1` FOREIGN KEY (`parent_collection_id`) REFERENCES `collection` (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
        """)
    tables['archival_object'] = ("""
        CREATE TABLE `archival_object` (
            `id` int(11) NOT NULL,
            `repo_id` int(11) NOT NULL,
            `root_record_id` int(11) NOT NULL,
            `parent_id` int(11) DEFAULT NULL,
            `ref_id` varchar(255) NOT NULL,
            `component_id` varchar(255) DEFAULT NULL,
            `title` varchar(8704) NOT NULL,
            `publish` int NOT NULL,
            `level` varchar(255) NOT NULL,
            `preservica_collection_id` varchar(255) DEFAULT NULL,
            `extent` varchar(255) DEFAULT NULL,
            `physical_containers` mediumtext DEFAULT NULL,
            `create_time` timestamp NOT NULL,
            `m_time` timestamp NOT NULL,
        PRIMARY KEY (`id`),
        KEY `preservica_collection_id` (`preservica_collection_id`),
        KEY `parent_id` (`parent_id`),
        KEY `root_record_id` (`root_record_id`),
        CONSTRAINT `archival_object_ibfk_1` FOREIGN KEY (`preservica_collection_id`) REFERENCES `collection` (`id`),
        CONSTRAINT `archival_object_ibfk_2` FOREIGN KEY (`parent_id`) REFERENCES `archival_object` (`id`),
        CONSTRAINT `archival_object_ibfk_3` FOREIGN KEY (`root_record_id`) REFERENCES `resource` (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
        """)
    tables['restriction'] = ("""
        CREATE TABLE `restriction` (
            `id` int(11) NOT NULL,
            `parent_id` int(11) DEFAULT NULL,
            `root_record_id` int(11) NOT NULL,
            `repo_id` int(11) NOT NULL,
            `ao_level` varchar(255) NOT NULL,
            `lvl` bigint DEFAULT NULL,
            `resource_note_text` longtext DEFAULT NULL,
            `resource_mar` longtext DEFAULT NULL,
            `resource_end` longtext DEFAULT NULL,
            `text_path` longtext DEFAULT NULL,
            `type_path` longtext DEFAULT NULL,
            `end_path` longtext DEFAULT NULL,
            `create_time` timestamp NOT NULL,
            `m_time` timestamp NOT NULL,
        PRIMARY KEY (`id`),
        KEY `id` (`id`),
        CONSTRAINT `restriction_ibfk_1` FOREIGN KEY (`id`) REFERENCES `archival_object` (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
        """)
    tables['hierarchies'] = ("""
        CREATE TABLE `hierarchies` (
            `id` int(11) NOT NULL,
            `resource_id` int(11) NOT NULL,
            `repo_id` int(11) NOT NULL,
            `full_path` longtext NOT NULL,
            `lvl` bigint NOT NULL,
            `create_time` timestamp NOT NULL,
            `m_time` timestamp NOT NULL,
        PRIMARY KEY (`id`),
        KEY `id` (`id`),
        KEY `resource_id` (`resource_id`),
        CONSTRAINT `hierarchies_ibfk_1` FOREIGN KEY (`id`) REFERENCES `archival_object` (`id`),
        CONSTRAINT `hierarchies_ibfk_2` FOREIGN KEY (`resource_id`) REFERENCES `resource` (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
        """)
    tables['deliverable_unit'] = ("""
        CREATE TABLE `deliverable_unit` (
            `id` varchar(255) NOT NULL,
            `collection_id` varchar(255) NOT NULL,
            `parent_deliverable_unit` varchar(255) DEFAULT NULL,
            `root_parent_deliverable_unit` varchar(255) DEFAULT NULL,
            `deliverable_unit_level` int(11) DEFAULT NULL,
            `digital_surrogate` varchar(255) DEFAULT NULL,
            `coverage_from` timestamp DEFAULT NULL,
            `coverage_to` timestamp DEFAULT NULL,
            `security_tag` varchar(255) DEFAULT NULL,
            `create_time` timestamp NOT NULL,
            `m_time` timestamp NOT NULL,
        PRIMARY KEY (`id`),
        KEY `collection_id` (`collection_id`),
        KEY `parent_deliverable_unit` (`parent_deliverable_unit`),
        KEY `root_parent_deliverable_unit` (`root_parent_deliverable_unit`),
        CONSTRAINT `deliverable_unit_ibfk_1` FOREIGN KEY (`collection_id`) REFERENCES `collection` (`id`),
        CONSTRAINT `deliverable_unit_ibfk_2` FOREIGN KEY (`parent_deliverable_unit`) REFERENCES `deliverable_unit` (`id`),
        CONSTRAINT `deliverable_unit_ibfk_3` FOREIGN KEY (`root_parent_deliverable_unit`) REFERENCES `deliverable_unit` (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
        """)
    tables['digital_object'] = ("""
        CREATE TABLE `digital_object` (
            `id` int(11) NOT NULL,
            `digital_object_id` varchar(255) NOT NULL,
            `archival_object_id` int(11) DEFAULT NULL,
            `title` varchar(255) NOT NULL,
            `publish` int NOT NULL,
            `has_content_link` varchar(255) NOT NULL,
            `create_time` timestamp NOT NULL,
            `m_time` timestamp NOT NULL,
        PRIMARY KEY (`id`),
        KEY `archival_object_id` (`archival_object_id`),
        KEY `digital_object_id` (`digital_object_id`),
        CONSTRAINT `digital_object_ibfk_1` FOREIGN KEY (`archival_object_id`) REFERENCES `archival_object` (`id`),
        CONSTRAINT `digital_object_ibfk_2` FOREIGN KEY (`digital_object_id`) REFERENCES `deliverable_unit` (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
        """)
    tables['digital_object_component'] = ("""
        CREATE TABLE `digital_object_component` (
            `id` int(11) NOT NULL,
            `repo_id` int(11) NOT NULL,
            `root_record_id` int(11) NOT NULL,
            `parent_id` int(11) DEFAULT NULL,
            `component_id` varchar(255) DEFAULT NULL,
            `title` varchar(8704) NOT NULL,
            `publish` int NOT NULL,
            `create_time` timestamp NOT NULL,
            `m_time` timestamp NOT NULL,
        PRIMARY KEY (`id`),
        KEY `root_record_id` (`root_record_id`),
        KEY `parent_id` (`parent_id`),
        KEY `component_id` (`component_id`),
        CONSTRAINT `digital_object_component_ibfk_1` FOREIGN KEY (`root_record_id`) REFERENCES `digital_object` (`id`),
        CONSTRAINT `digital_object_component_ibfk_2` FOREIGN KEY (`parent_id`) REFERENCES `digital_object_component` (`id`),
        CONSTRAINT `digital_object_component_ibfk_3` FOREIGN KEY (`component_id`) REFERENCES `deliverable_unit` (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
        """)
    tables['event'] = ("""
        CREATE TABLE `event` (
            `id` int(11) NOT NULL,
            `event_id` int(11) NOT NULL,
            `repo_id` int(11) NOT NULL,
            `archival_object_id` int(11) NOT NULL,
            `event_link_role` varchar(255) NOT NULL,
            `event_type` varchar(255) DEFAULT NULL,
            `event_outcome` varchar(255) DEFAULT NULL,
            `outcome_note` mediumtext DEFAULT NULL,
            `timestamp` timestamp DEFAULT NULL,
            `refid` varchar(255) DEFAULT NULL,
            `agent_auth_name` varchar(255) DEFAULT NULL,
            `agent_auth_id` varchar(255) DEFAULT NULL,
            `agent_auth_role` varchar(255) DEFAULT NULL,
            `create_time` timestamp NOT NULL,
            `m_time` timestamp NOT NULL,
        PRIMARY KEY (`id`),
        KEY `archival_object_id` (`archival_object_id`),
        CONSTRAINT `event_ibfk_1` FOREIGN KEY (`archival_object_id`) REFERENCES `archival_object` (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
        """)
    tables['manifestation'] = ("""
        CREATE TABLE `manifestation` (
            `id` varchar(255) NOT NULL,
            `deliverable_unit_id` varchar(255) NOT NULL,
            `type` varchar(255) DEFAULT NULL,
            `create_time` timestamp NOT NULL,
            `m_time` timestamp NOT NULL,
        PRIMARY KEY (`id`),
        KEY `deliverable_unit_id` (`deliverable_unit_id`),
        CONSTRAINT `manifestation_ibfk_1` FOREIGN KEY (`deliverable_unit_id`) REFERENCES `deliverable_unit` (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
        """)
    tables['digital_file'] = ("""
        CREATE TABLE `digital_file` (
            `id` varchar(255) NOT NULL,
            `manifestation_id` varchar(255) DEFAULT NULL,
            `file_set_id` varchar(255) DEFAULT NULL,
            `filesize` varchar(255) DEFAULT NULL,
            `filemoddate` timestamp DEFAULT NULL,
            `filename` varchar(255) NOT NULL,
            `working_path` longtext DEFAULT NULL,
            `format_puid` varchar(255) DEFAULT NULL,
            `format_name` mediumtext DEFAULT NULL,
 #           `mimetype` varchar(255) DEFAULT NULL,
            `create_time` timestamp NOT NULL,
            `m_time` timestamp NOT NULL,
        PRIMARY KEY (`id`),
        KEY `manifestation_id` (`manifestation_id`),
        CONSTRAINT `digital_file_ibfk_1` FOREIGN KEY (`manifestation_id`) REFERENCES `manifestation` (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
        """)
    tables['file_version'] = ("""
        CREATE TABLE `file_version` (
            `id` int(11) NOT NULL,
            `digital_object_id` int(11) NOT NULL,
            `repo_id` int(11) NOT NULL,
            `digital_file_id` varchar(255) NOT NULL,
            `file_uri` mediumtext NOT NULL,
            `file_size_bytes` bigint DEFAULT NULL,
            `file_format` varchar(255) DEFAULT NULL,
            `create_time` timestamp NOT NULL,
            `m_time` timestamp NOT NULL,
        PRIMARY KEY (`id`),
        KEY `digital_object_id` (`digital_object_id`),
        KEY `digital_file_id` (`digital_file_id`),
        CONSTRAINT `file_version_ibfk_1` FOREIGN KEY (`digital_object_id`) REFERENCES `digital_object` (`id`),
        CONSTRAINT `file_version_ibfk_2` FOREIGN KEY (`digital_file_id`) REFERENCES `digital_file` (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
        """)
    return tables

def initialize_database(db_cursor, db_name, db_tables):
    '''Function that drops any existing database, creates a new database, seelects the database,
        creates DB tables, and tests the new connection. Combined several initialization functions
        so only one function needs to be called from main'''
    drop_database(db_cursor, db_name)
    create_database(db_cursor, db_name)
    use_database(db_cursor, db_name)
    create_tables(db_cursor, db_name, db_tables)
    test_connection = mysql.connector.connect(user='root', password='benji8798', host='127.0.0.1', database='aspace_preservica_db', use_pure=True, autocommit=True)
    print(test_connection)

def prep_file_versions(row):
    ident = int(row[0])
    dobject_id = int(row[1])
    repo_id = int(row[2])
    digital_file_id = row[3]
    file_uri = row[4]
    if row[5] == '':
        file_size_bytes = 9999999999
    else:
        file_size_bytes = int(row[5])
    file_format = row[6]
    create_time = row[7]
    mtime = row[8]
    return (ident, dobject_id, repo_id, digital_file_id, file_uri, file_size_bytes, file_format, str(create_time), str(mtime))

def prep_digital_objects(row):
    ident = int(row[0])
    dobject_id = row[1]
    if (row[2] != '' and row[2] != 'NULL' and row[2] is not None):
        ao_id = int(row[2])
    else:
        ao_id = 0
    dig_object_title = row[3]
    publish = int(row[4])
    has_content_link = row[5]
    create_time = row[6]
    m_time = row[7]
    return (ident, dobject_id, ao_id, dig_object_title, publish, has_content_link, str(create_time), str(m_time))

def prep_archival_objects(row):
    ident = int(row[0])
    repo_id = int(row[1])
    root_record_id = int(row[2])
    if (row[3] != '' and row[3] != 'NULL' and row[3] is not None):
        parent_id = int(row[3])
    else:
        parent_id = 0
    ref_id = row[4]
    component_id = row[5]
    title = row[6]
    publish = int(row[7])
    level = row[8]
    preservica_collection_id = row[9]
    extent = row[10]
    physical_containers = row[11]
    create_time = row[12]
    m_time = row[13]
    return (ident, repo_id, root_record_id, parent_id, ref_id, component_id, title, publish, level, preservica_collection_id, extent, physical_containers, str(create_time), str(m_time))

def prep_events(row):
    try:
        if (row[8] == '' or row[8] == 'NULL' or row[8] is not None):
            row[8] = '1999-01-01 00:00:00'
        return (int(row[0]), int(row[1]), int(row[2]), int(row[3]), row[4], row[5], row[6], row[7], str(row[8]).replace('Z', ''), row[9], row[10], row[11], row[12], str(row[13]).replace('Z', ''), str(row[14]).replace('Z', ''))
    except Exception:
        print(row)
        print(traceback.format_exc())

def prep_digital_object_components(row):
    if (row[3] != '' and row[3] != 'NULL' and row[3] is not None):
        parent_id = int(row[3])
    else:
        parent_id = 0
    return (int(row[0]), int(row[1]), int(row[2]), parent_id, row[4], row[5], int(row[6]), str(row[7]).replace('Z', ''), str(row[8]).replace('Z', ''))

def prep_restrictions(row):
    if (row[1] != '' and row[1] != 'NULL' and row[1] is not None):
        parent_id = int(row[1])
    else:
        parent_id = 0
    return (int(row[0]), parent_id, int(row[2]), int(row[3]), row[4], int(row[5]), row[6], row[7], row[8], row[9], row[10], row[11], str(datetime.now()).replace('Z', ''), str(datetime.now()).replace('Z', ''))

def prep_resources(row):
    return (int(row[0]), row[1], row[2], int(row[3]), int(row[4]), str(row[5]), str(row[6]))

def prep_hierarchies(row):
    return (int(row[0]), int(row[1]), int(row[2]), row[3], int(row[4]), str(datetime.now()).replace('Z', ''), str(datetime.now()).replace('Z', ''))

def prep_collections(row):
    return (row[0], row[1], row[2], row[3], row[4], row[5], row[6])

def prep_deliverable_units(row):
    if row[6].replace('Z', '') == "0205-11-22T00:00:00.000-05:00":
        row[6] = "2005-11-22T00:00:00.000-05:00"
    if (row[2] == '' or row[2] == 'NULL'):
        row[2] = row[0]
    if (row[3] == '' or row[3] == 'NULL'):
        row[3] = row[0]
    return (row[0], row[1], row[2], row[3], int(row[4]), row[5], row[6].replace('Z', ''), row[7].replace('Z', ''), row[8], row[9], row[10])

def prep_digital_files(row):
    return (row[0], row[8], row[1], int(row[2]), row[4], row[3], row[5], row[6], row[7], str(datetime.now()).replace('Z', ''), str(datetime.now()).replace('Z', ''))

def prep_manifestations(row):
    return (row[0], row[1], row[2], str(datetime.now()).replace('Z', ''), str(datetime.now()).replace('Z', ''))

def update_database(db_cursor, table_name, db_input, func):
    for num, item in enumerate(db_input, 1):
        try:
            row = func(item)
            #print(row)
            insert_data(db_cursor, table_name, row)
            print(f"Row {num} created")
        except Exception:
            print(f"Error on row {num}")
            print(row)
            print(len(row))
            print(traceback.format_exc())

def connection_manager():
    mysql_instance = mysql.connector.connect(user='root', password='benji8798', host='127.0.0.1', use_pure=True)
    db_cursor = mysql_instance.cursor()
    db_name = 'aspace_preservica_db'
    return mysql_instance, db_cursor, db_name

def update_db_table(db_cursor, db_name, csvfile, datatype, func):
    override_foreign_key_contstraint(db_cursor)
    use_database(db_cursor, db_name)
    update_database(db_cursor, datatype, csvfile, func)

def drop_and_update_db_table(db_cursor, db_name, csvfile, datatype, func):
    override_foreign_key_contstraint(db_cursor)      
    use_database(db_cursor, db_name)
    drop_table(db_cursor, datatype)
    create_table(db_cursor, datatype)
    update_database(db_cursor, datatype, csvfile, func)

def initialize(db_cursor, db_name):
    db_tables = database_tables()
    initialize_database(db_cursor, db_name, db_tables)

def main():
    mysql_instance, db_cursor, db_name = connection_manager()
    header_row, csvfile = u.opencsv('/Users/aliciadetelich/Dropbox/git/aspace_preservica_db/db_data/fifth_ingest/digital_files_prepped.csv')
    try:
        override_foreign_key_contstraint(db_cursor)      
        use_database(db_cursor, db_name)
        #drop_table(db_cursor, 'manifestation')
        #create_table(db_cursor, 'manifestation')
        update_database(db_cursor, 'digital_file', csvfile, prep_digital_files)
    except Exception:
        print(traceback.format_exc())
    finally:
        mysql_instance.commit()
        db_cursor.close()
        mysql_instance.close()


if __name__ == "__main__":
    main()

#NOTE - NULL values in the digital object table archival objecet id column are being populated with 0s for some reason
#Did an update to change this
#Also the NULLS are getting put in as NULL strings, gotta change that too

