#!/usr/bin/python3
#~/anaconda3/bin/python

from collections import Counter
from datetime import datetime
from lxml import etree
import os
import pprint
import traceback
from utilities import utilities as u


def collection_metadata(root, filename):
	identifier = root.findall(".//{http://www.tessella.com/XIP/v4}Collections/{http://www.tessella.com/XIP/v4}Collection/{http://www.tessella.com/XIP/v4}CollectionRef")[0].text
	parent_collection_id = root.findall(".//{http://www.tessella.com/XIP/v4}Collections/{http://www.tessella.com/XIP/v4}Collection/{http://www.tessella.com/XIP/v4}ParentRef")[0].text
	collection_code = root.findall(".//{http://www.tessella.com/XIP/v4}Collections/{http://www.tessella.com/XIP/v4}Collection/{http://www.tessella.com/XIP/v4}CollectionCode")[0].text
	security_tag = root.findall(".//{http://www.tessella.com/XIP/v4}Collections/{http://www.tessella.com/XIP/v4}Collection/{http://www.tessella.com/XIP/v4}SecurityTag")[0].text
	title = root.findall(".//{http://www.tessella.com/XIP/v4}Collections/{http://www.tessella.com/XIP/v4}Collection/{http://www.tessella.com/XIP/v4}Title")[0].text
	create_time = str(datetime.now())
	m_time = str(datetime.now())
	return (identifier, parent_collection_id, collection_code, security_tag, title, create_time, m_time)

def deliverable_unit_metadata(root, filename):
	identifier = root.findall(".//{http://www.tessella.com/XIP/v4}DeliverableUnits/{http://www.tessella.com/XIP/v4}DeliverableUnit/{http://www.tessella.com/XIP/v4}DeliverableUnitRef")[0].text
	collection_id = root.findall(".//{http://www.tessella.com/XIP/v4}DeliverableUnits/{http://www.tessella.com/XIP/v4}DeliverableUnit/{http://www.tessella.com/XIP/v4}CollectionRef")[0].text
	parent_id = root.find(".//{http://www.tessella.com/XIP/v4}DeliverableUnits/{http://www.tessella.com/XIP/v4}DeliverableUnit/{http://www.tessella.com/XIP/v4}ParentRef")
	if parent_id != None:
		parent_id = parent_id.text
	else:
		parent_id = ''
	digital_surrogate = root.findall(".//{http://www.tessella.com/XIP/v4}DeliverableUnits/{http://www.tessella.com/XIP/v4}DeliverableUnit/{http://www.tessella.com/XIP/v4}DigitalSurrogate")[0].text
	coverage_from = root.findall(".//{http://www.tessella.com/XIP/v4}DeliverableUnits/{http://www.tessella.com/XIP/v4}DeliverableUnit/{http://www.tessella.com/XIP/v4}CoverageFrom")[0].text
	coverage_to = root.findall(".//{http://www.tessella.com/XIP/v4}DeliverableUnits/{http://www.tessella.com/XIP/v4}DeliverableUnit/{http://www.tessella.com/XIP/v4}CoverageTo")[0].text
	security_tag = root.findall(".//{http://www.tessella.com/XIP/v4}DeliverableUnits/{http://www.tessella.com/XIP/v4}DeliverableUnit/{http://www.tessella.com/XIP/v4}SecurityTag")[0].text
	create_time = str(datetime.now())
	m_time = str(datetime.now())
	return (identifier, collection_id, parent_id, digital_surrogate, coverage_from, coverage_to, security_tag, create_time, m_time)

# def manifestation_metadata(root, filename):
# 	identifier = root.findall("")
# 	deliverable_unit_id = root.findall("")
# 	typeref = root.findall("")
# 	create_time = str(datetime.now())
# 	m_time = str(datetime.now())
# 	return ()

def digital_file_metadata(root, filename):
	#these all only have one entry
	file_size = root.findall(".//{http://www.tessella.com/XIP/v4}Files/{http://www.tessella.com/XIP/v4}File/{http://www.tessella.com/XIP/v4}FileSize")[0].text
	file_name = root.findall(".//{http://www.tessella.com/XIP/v4}Files/{http://www.tessella.com/XIP/v4}File/{http://www.tessella.com/XIP/v4}FileName")[0].text
	file_set = root.findall(".//{http://www.tessella.com/XIP/v4}Files/{http://www.tessella.com/XIP/v4}File/{http://www.tessella.com/XIP/v4}IngestedFileSetRef")[0].text
	file_ref = root.findall(".//{http://www.tessella.com/XIP/v4}Files/{http://www.tessella.com/XIP/v4}File/{http://www.tessella.com/XIP/v4}FileRef")[0].text
	last_mod = root.findall(".//{http://www.tessella.com/XIP/v4}Files/{http://www.tessella.com/XIP/v4}File/{http://www.tessella.com/XIP/v4}LastModifiedDate")[0].text
	working_path = root.findall(".//{http://www.tessella.com/XIP/v4}Files/{http://www.tessella.com/XIP/v4}File/{http://www.tessella.com/XIP/v4}WorkingPath")[0].text
	newrow = [file_ref, file_set, file_size, file_name, last_mod, working_path]
	#these could have more
	format_puids = root.findall(".//{http://www.tessella.com/XIP/v4}Files/{http://www.tessella.com/XIP/v4}File/{http://www.tessella.com/XIP/v4}FormatInfo/{http://www.tessella.com/XIP/v4}FormatPUID")
	puids = [puid.text for puid in format_puids]
	newrow.append(puids)
	format_names = root.findall(".//{http://www.tessella.com/XIP/v4}Files/{http://www.tessella.com/XIP/v4}File/{http://www.tessella.com/XIP/v4}FormatInfo/{http://www.tessella.com/XIP/v4}FormatName")
	form_names = [form_name.text for form_name in format_names]
	newrow.append(form_names)
	return newrow


def main():
	try:
		fileobject, csvoutfile = u.opencsvout('/Users/aliciadetelich/Dropbox/git/mssa_digital_data_projects/database/deliverable_unit_table.csv')
		#csvoutfile.writerow(['id', 'collection_id', 'digital_surrogate', 'coverage_from', 'coverage_to', 'security_tag', 'create_time', 'm_time'])
		csvoutfile.writerow(['identifier', 'collection_id', 'parent_id', 'digital_surrogate', 'coverage_from', 'coverage_to', 'security_tag'])
		#csvoutfile.writerow(['id', 'parent_id', 'collection_code', 'security_tag', 'title', 'create_time', 'mtime'])
		#fileobject2, csvoutfile2 = u.opencsvout('/Users/aliciadetelich/Dropbox/git/mssa_digital_data_projects/adding_content_links_to_aspace/error_files.csv')
		#csvoutfile2.writerow(['filename'])
		metadata_directory = '/Users/aliciadetelich/Dropbox/git/mssa_digital_data_projects/preservica_api_exploration/04/deliverable_units'
		filelist = os.listdir(metadata_directory)
		print(len(filelist))
		combined_list = []
		for i, filename in enumerate(filelist, 1):
			if 'DS_Store' not in filename:
				try:
					tree = etree.parse(f"{metadata_directory}/{filename}")
					new_row = deliverable_unit_metadata(tree, filename)
					csvoutfile.writerow(new_row)
					#print(new_row)
					#seems like the element count within each of the metadata files is always 1
					# element_count = list(tree.iter())
					# for item in element_count:
					# 	combined_list.append(str(item).split(' at ')[0])
				except Exception as exc:
					#csvoutfile2.writerow([i, filename])
					print(traceback.format_exc())
					continue
		# print('Working...')
		# counted_items = dict(Counter(combined_list))
		# for k, v in counted_items.items():
		# 	row = [k, v]
		# 	csvoutfile.writerow(row)
		#pprint.pprint(counted_items)
	finally:
		fileobject.close()
		#fileobject2.close()

if __name__ == "__main__":
	main()