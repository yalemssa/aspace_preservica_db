# aspace_preservica_db
A reporting database for collating and doing stuff with ArchivesSpace and Preservica metadata

## Tables

The tables in the database and how they are generated

#### `archival_object`

This table is populated by running the `archival_object_table.sql` query against the ArchivesSpace database.


| Column                   | Description       				| Type         |
| ------------------------ | ------------------------------ | ------------ |
| id                       | The archival object id 		| int		   |
| repo_id                  | The repository id 				| int 		   |
| root_record_id           | The parent resource id 		| int 		   |
| parent_id                | The parent archival object 	| int          |
| ref_id                   | The ref id 					| varchar(255) |
| component_id             | The component unique id 		| varchar(255) |
| title                    | The archival object title 		| varchar(8704)|
| publish                  | 1 published, 0 unpublished 	| int 		   |
| level                    | Hierarchical level (i.e. file) | varchar(255) |
| preservica_collection_id | Found in the note field 		| varchar(255) |
| extent                   | Concatenated extent value		| varchar(255) |
| physical_containers      | Concatenated container data    | mediumtext   |
| create_time              | ArchivesSpace creation time    | timestamp    |
| m_time                   | ArchivesSpace last modified    | timestamp    |

#### `digital_object`
This table is populated by running the `digital_object_table.sql` query against the ArchivesSpace database.

| Column                   | Description       				| Type         |
| ------------------------ | ------------------------------ | ------------ |
| id					   | The digital object id			| int          |
| digital_object_id		   | The Preservica DelUnit id		| varchar(255) | 
| archival_object_id	   | The linked record id		 	| int          |
| title					   | The digital object title	    | varchar(255) |
| publish				   | 1 published, 0 unpublished	    | int          |
| has_content_link		   | 1 has link. 0 no link			| int          |
| create_time			   | ArchivesSpace creation time	| timestamp    |
| m_time				   | ArchivesSpace creation time	| timestamp    |

#### `digital_object_component`
This table is populated by running the `digital_object_component_table.sql` query against the ArchivesSpacee database.

| Column                   | Description       				| Type         |
| ------------------------ | ------------------------------ | ------------ |
| id					   | The digital object component id| int          |
| repo_id                  | The repository id 				| int 		   |
| root_record_id           | The root digital object id 	| int 		   |
| parent_id                | The parent digital object id 	| int          |
| component_id             | The component unique id 		| varchar(255) |
| title                    | The component title     		| varchar(8704)|
| publish                  | 1 published, 0 unpublished 	| int 		   |
| create_time			   | ArchivesSpace creation time	| timestamp    |
| m_time				   | ArchivesSpace creation time	| timestamp    |

#### `restriction`
This table is populated by running the `restrictions_table.sql` query against the ArchivesSpace database. This query can only be run on a local/test version of YUL ArchivesSpace, as it requires SQL common table expressions (CTEs) that only exist in MySQL 8+. The production version of YUL ArchivesSpace should be upgraded to MySQL 8 in early 2021.

| Column                   | Description       				| Type         |
| ------------------------ | ------------------------------ | ------------ |
| id					   | The archival object id			| int          |
| resource_note_text	   | The resource-level note		| longtext     |
| lvl					   | The note level					| int          | 
| path					   | The restriction text hierarchy	| longtext     |
| type_path				   | The restriction type hierarchy	| longtext     |
| end_path				   | The restriction date hierarchy	| longtext     |

#### `hierarchy`
This table is populated by running the `hierarchies.sql` query against the ArchivesSpace database. The `hierarchies` table is a view that only exists in a local/test version of YUL ArchivesSpace, as it requires SQL common table expressions (CTEs) that only exist in MySQL 8+. The production version of YUL ArchivesSpace should be upgraded to MySQL 8 in early 2021.

| Column                   | Description       				| Type         |
| ------------------------ | ------------------------------ | ------------ |
| id					   | The archival object id			| int          |
| root_record_id		   | The resource id				| int          |
| repo_id				   | The repository id				| int          |
| full_path				   | The full hierarchy				| longtext     |
| lvl					   | The hierarchical level			| int          |

#### `collection`
This table is populated by extracting data from Preservica collection XML files. The collection IDs are stored in notes within ArchivesSpace archival object records, and are extracted by running the `get_collection_ids.sql` query against the ArchivesSpace database. 

| Column                   | Description       				| Type         |
| ------------------------ | ------------------------------ | ------------ |
| id					   | Preservica collection id		| varchar(255) |
| parent_collection_id	   | Preservica parent collection	| varchar(255) |
| collection_code		   | Preservica collection code	    | varchar(255) |	
| security_tag			   | Preservica security tag		| varchar(255) |
| title					   | Preservica collection title	| varchar(8704)|
| create_time			   | Local database create time		| timestamp    |
| m_time				   | Local database last modified	| timestamp    |

#### `deliverable_unit`
This table is populated by extracting data from Preservica deliverable unit XML files. The deliverable unit IDs are stored in digital object records in ArchivesSpace, and are extracted by running the `get_deliverable_unit_ids.sql` query against the  ArchivesSpace database.

| Column                    | Description       			 | Type         |
| ------------------------- | ------------------------------ | ------------ |
| id						| The digital object id			 | int          |
| collection_id				| The Preservica collection id	 | varchar(255) |
| parent_deliverable_unit	| The parent deliverable unit id | varchar(255) |
| root_parent_del_unit		| The root deliverable unit id	 | varchar(255) |
| deliverable_unit_level	| The deliverable unit level	 | int          |
| digital_surrogate			| yes or no						 | varchar(255) |
| coverage_from				| Start date					 | timestamp    |
| coverage_to				| End date						 | timestamp    |
| security_tag				| Security tag					 | varchar(255) |
| create_time				| Local database create time	 | timestamp    |
| m_time					| Local database last modified	 | timestamp    |

#### `manifestation`
This table is populated by extracting data from Preservica deliverable unit XML files.

| Column                   | Description       				| Type         |
| ------------------------ | ------------------------------ | ------------ |
| id					   | Preservica  manifestation id	| varchar(255) |
| deliverable_unit_id	   | Deliverable unit id			| varchar(255) |	
| typeref				   | The manifestation type			| varchar(255) |
| summary				   | Preservica summary				| varchar(8704)|
| create_time			   | Local database create time		| timestamp    |
| m_time				   | Local database last modified	| timestamp    |

#### `digital_file`
This table is populated by extracting data from Preservica digital file XML files. The list of digital files is derived from Preservica deliverable unit XML files.

| Column                   | Description       				| Type         |
| ------------------------ | ------------------------------ | ------------ |
| id					   | The digital file id		    | int          |
| manifestation_id		   | The manifestation id			| int          |
| file_set_id			   | The file set id			 	| int          |
| filesize				   | The file size in bytes			| int          |
| filemoddate			   | The file modification date		| timestamp    |
| filename				   | The name of the file			| varchar(255) | 
| working_path			   | The relative path to the file	| varchar(255) |
| format_puid			   | The format unique id			| varchar(255) |
| format_name			   | The format name				| mediumtext   |
| mimetype				   | The format mimetype			| varchar(255) |
| create_time			   | Local database creation time	| timestamp    |
| m_time				   | Local database last modified	| timestamp    |

## Extracting Data from Source Databases

How to extract data from ArchivesSpace and Preservica

### ArchivesSpace

Data is extracted from ArchivesSpace via several SQL queries

### Preservica

Data is extracted from Preservica by calling the Preservica API, as access to the Preservica database is restricted.

## Initializing the `aspace_preservica_db` Database

How to create and populate the database using extracted data from ArchivesSpace and Preservica

### Run `process_xml_for_db.py`

The first step in initializing the `aspace_preservica_db` database is running the `process_xml_for_db.py` script on extracted Preservica XML files. 

### Run `db_manager.py`

Once the CSV spreadsheets are prepared for ingest, it is time to run the `db_manager.py` script to create and populate the database.

### Run SQL updates to fix field formatting

After the data is ingested, a little bit of clean-up is necessary.

### Derive parent deliverable unit data

## Updating the `aspace_preservica_db` Database

### Checking for new data

### Adding new data to the database

### Design Notes and To-Dos


#### Reconciling missing data

Some data was missed during the initial load:

* Collection data
* Child deliverable unit data
* Digital object component data
* Restriction data

Consider re-designing manifestation table, since there are duplicate manifestation IDs; or at least sort out those issues.















