# Prepping database data

A description of the process for retrieving and collating data from ArchivesSpace and Preservica into aspace_preservica_db

## Retrieving ArchivesSpace data

ArchivesSpace data is retrieved directly from the ArchivesSpace MySQL database using a series of SQL queries. Two of the queries (restrictions and hierarchies) rely upon special views that are only available in MySQL 8, and thus only on my local instance of ArchivesSpace (which I regularly populate with dumps from PROD)

## Retrieving Preservica data

Preservica data is retrieved from the Preservica API. Preservica deliverable unit and collection identifiers are pulled from the ArchivesSpace database. Deliverable unit IDs are stored in digital object and digital object component records, and collection IDs are stored in notes within archival object records.

Preservica metadata for deliverable units and collections is stored in XML files which can be downloaded from the Preservica API. The API accepts a number of parameters which allow the user to retrieve links to other collection and deliverable units within the same hierarchy, which may or may not be included in ArchivesSpace. Using the data in ArchivesSpace as a starting point for querying Preservica, it is possible to retrieve a relatively complete set of Preservica metadata from the API, even without full database access. (NOTE: it is possible to retrieve all collection IDs from the Preservica API by omitting an identifier when calling the collection endpoint. This does not work for deliverable units or digital files).

The exact process for retrieving this data is still somewhat in flux, as there are some discrepancies between the data in ArchivesSpace and what is in the Preservica API (for instance, only 50k deliverable units in AS, over 70k retrieved from Preservica; over 5000 Preservica IDs found in AS that were not found in Preservica, etc.). However I am relatively confident after signficant experimentation that I have a complete set of data that I can build upon as new materials are added to Preservica. 

<!-- Add a note about how the nested links and such are returned -->

## Processing Preservica data

After the data is downloaded from Preservica and additional links are extracted from the metadata, the `process_xml_for_db.py` script can be run against each record type (manifestation, deliverable unit, collection, digital file) to pull data from each XML file and store it in a CSV file to be ingested into the database


### Digital files

Digital file XML does not contain any reference to the manifestation, unfortunately. This means that after the digital file data is processed into a CSV, it needs additional processing to link back to its manifestation and/or deliverable unit records

Be aware of a possible foreign key constraint - I can't remember the issue I ran into last time...

## Ingesting the data into aspace_preservica_db

To ingest the data into the aspace_preservica_db database, run the `db_manager.py` script. This will create the database and populate it with the various CSV files of data returned from ArchivesSpace and Preservica.

## Running SQL update scripts after ingest

I am currently struggling to get the `db_manager.py` script to appropriately create NULL values upon table ingest, so it is necessary to run a few update SQL queries on the ingested data before it is ready to use. This must be done after every update

## Updating the aspace_preservica_db

<!-- TBA -->