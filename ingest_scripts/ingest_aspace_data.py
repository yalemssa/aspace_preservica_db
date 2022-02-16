#/usr/bin/python3

import json
import warnings
import ingest_data
import traceback
from utilities import db


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
            archivesspace_datatypes = ingest_data.aspace_datatypes()
            ingest_data.process_aspace_data(aspace_query_path, dbconn, base_directory, archivesspace_datatypes)
            ingest_data.additional_processing_events(base_directory)
            ingest_data.ingest_into_db(db_cursor, db_name, base_directory, archivesspace_datatypes)
            ingest_data.fix_fields(db_cursor, db_name, fix_field_query_path)
        except Exception:
            print(traceback.format_exc())
        finally:
            dbconn.close_conn()
            mysql_instance.commit()
            db_cursor.close()
            mysql_instance.close()


if __name__ == "__main__":
    main()