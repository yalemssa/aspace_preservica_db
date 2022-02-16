#/usr/bin/python3

import _get_collection_data as get_collection_data
import _get_deliverable_unit_data as get_deliverable_unit_data
import _get_manifestation_data as get_manifestation_data
import _get_digital_file_data as get_digital_file_data


def main():
	''' 1: Run the get_collection_data.py script to retrieve all collection XML records. This should be done during every update. Be sure to include the includeChildHierarchy and includeParentHierarchy parameters in the params flag within the config file.
	'''
	get_collection_data.main()
	''' 2. Run extract_new_data.get_all_deliverable_units_from_collections function against the downloaded collection XML to retrieve a list of deliverable units.
		3. Download all deliverable units using the preservica_metadata_file_picker.py function. Be sure to include the includeNestedLinks parameter in the config file's params flag in order to retrieve all manifestations and digital file links.
	'''
	input("Press enter to download deliverable units... ")
	get_deliverable_unit_data.main()
	''' 4. Retrieve manifestation data by running the extract_new_data.get_manifestations_from_dus function against the deliverable unit XML folder
		5. Download manifestation records using the preservica_metadata_file_picker.py script
	'''
	input("Press enter to download manifestations... ")
	get_manifestation_data.main(full=False)
	''' 6. Retrieve digital file data by running the extract_new_data.get_digital_files_from_dus function against the deliverable unit XML folder
		7. Downloading over 4 million digital file records is time consuming, and the files themselves rarely change. To retrieve a list of only NEW digital files (i.e. those which are not among the previous ingests' files), run the extract_new_data.get_new_digital_files function using the list of digital files from step 7 and the folder of digital files from the previous ingests. Will either need to combine the digital files for each ingest (possibly with a function), or write a function which gets the contents of the digital file folders which are present within each ingest folder.
	'''
	input("Press enter to download digital files... ")
	get_digital_file_data.main(full=False)
	''' 8. It can also be time consuming to download all the manifestation files, so could do a similar process as in step 7 by running the extract_new_data.get_new_manifestations function. This is not required but may save some time.
	'''


if __name__ == "__main__":
	main()