#!/usr/bin/python3
#~/anaconda3/bin/python

import json
from lxml import etree
import traceback
import preservica_metadata_file_picker as pmfp
import _xml_tools as xml_tools
from utilities import utilities as u

from rich import print
from rich.progress import track
from rich.padding import Padding

from _constants import console, progress


def download_child_collection_files(client, cfg, endpoint, params):
    #downloads all the child collections from get_nested_links
    console.log('Downloading child collection files...')
    input_fp = f"{cfg.get('collection_output_folder')}/child_collections.csv"
    csvfile, rowcount, input_csv = pmfp.opencsvdict(input_fp)
    pmfp.run_thread_pool_executor(client, csvfile, rowcount, endpoint, params)
    console.log('Child collection files downloaded')

def get_child_collections(cfg):
    '''This gets the nested links from downloaded XML files'''
    console.log('Getting child collection identifiers...')
    file_list = xml_tools.get_path_set(cfg.get('collection_output_folder'))
    fileobject, csvoutfile = u.opencsvout(f"{cfg.get('collection_output_folder')}/child_collections.csv")
    csvoutfile.writerow(['direct_download_link'])
    entry_dataset = xml_tools.get_entry_data(file_list, 'collection', cfg.get('collection_output_folder'))
    entry_dataset = [tuple(value)[0] for key, value in entry_dataset.items()]
    csvoutfile.writerows(entry_dataset)
    fileobject.close()
    console.log('Child collection identifiers retrieved.')

def download_parent_collection_files(client, cfg):
    # opens the file that was created via the get_mssa_parent_collection_ids
    # function; downloads all the files
    console.log('Downloading collection files...')
    input_fp = cfg.get('collection_output_csv')
    csvfile, rowcount, input_csv = pmfp.opencsvdict(input_fp)
    print(rowcount)
    endpoint = cfg.get('collection_endpoint')
    params = cfg.get('collection_params')
    pmfp.run_thread_pool_executor(client, csvfile, rowcount, endpoint, params)
    console.log('Collection files downloaded.')

def get_mssa_parent_collection_ids(cfg):
    '''This function parses the results of the get_main_collection_xml file
    and puts the collection identifiers into a CSV file'''
    console.log('Getting MSSA collection identifiers...')
    fileobject, csvoutfile = u.opencsvout(cfg.get('collection_output_csv'))
    csvoutfile.writerow(['direct_download_link', 'parent_collection_id', 'collection_code', 'collection_title', 'title', 'link', 'link_type'])
    collection_path = f"{cfg.get('collection_output_folder')}/preservica_.xml"
    tree = etree.parse(collection_path)
    collections = tree.findall(".//{http://www.tessella.com/XIP/v4}Collections/{http://www.tessella.com/XIP/v4}Collection")
    for collection in collections:
        collection_id = collection.find(".//{http://www.tessella.com/XIP/v4}CollectionRef")
        collection_code = collection.find(".//{http://www.tessella.com/XIP/v4}CollectionCode")
        collection_title = collection.find(".//{http://www.tessella.com/XIP/v4}Title")
        entries = collection.iter("{http://www.w3.org/2005/Atom}entry")
        if collection_title.text in ('MSSA', 'pendingSynchronization'):
            processed_entries = xml_tools.process_entries(collection_id.text, collection_code.text, collection_title.text, entries)
            csvoutfile.writerows(processed_entries)
    fileobject.close()
    console.log('MSSA collection identifiers retrieved.')

def get_main_collection_xml(client, cfg):
    '''This will pull ALL collections in Preservica. The only endpoint where you do not
    need to specify an identifier'''
    console.log('Getting main collection XML...')
    # passing empty string because there is no identifier and no parameters
    # passing dict with empty string because the usual param for the send_request is a CSV dict
    # The resulting file is going to be titled f"cfg.get('output_folder')/preservica_.xml"
    client.send_request({'direct_download_link': ""}, cfg.get('collection_endpoint'), "")
    console.log('Collection XML retrieved.')

def get_client(cfg):
    return pmfp.PreservicaDownloader(pmfp.configure(cfg, 'collection_output_folder'), pmfp.configure(cfg, 'preservica_username'), pmfp.configure_password(cfg), pmfp.configure(cfg, 'tenant'), pmfp.configure(cfg, 'preservica_api_url'))

def main():
    try:
        cfg = json.load(open('config.json'))
        endpoint = cfg.get('collection_endpoint')
        params = cfg.get('collection_params')
        client = get_client(cfg)
        # Gets the main collection XML
        get_main_collection_xml(client, cfg)
        # Parses it and pulls out all the MSSA and Pending Sync collection IDs
        get_mssa_parent_collection_ids(cfg)
        # Downloads all of those identifiers
        download_parent_collection_files(client, cfg)
        # This gets all the collection data from the parents
        input("Press enter to download child collections... ")
        get_child_collections(cfg)
        # This downloads the child collections; presumably there are no dupes here? I'm not sure though...
        # also, what happens when I do or do not include the includeNestedLinks param in the params for collections?
        # I am pretty sure it just applies to digital file links - but I have to check the docs
        download_child_collection_files(client, cfg, endpoint, params)
    except Exception:
        print(traceback.format_exc())
    finally:
        pass
        #fileobject.close()


if __name__ == "__main__":
    main()





# def compare_files(dirpath, other_dirpath):
#     new_identifiers = get_nested_links(other_dirpath)
#     return pxfd.get_new_data(dirpath, new_identifiers)
#pxfd.get_all_collections(cfg, csvoutfile)
# xml_toolspoint = cfg.get('xml_toolspoint')
# params = cfg.get('params')
# Possible to-do: run the metadata file picker and download all these files
# add a function to extract the rest of the child entries
# Also pull that metadata from Preservica
# client = PreservicaDownloader(configure(cfg, 'output_folder'), configure(cfg, 'preservica_username'), configure_password(cfg), configure(cfg, 'tenant'), configure(cfg, 'preservica_api_url'))
#       run_thread_pool_executor(client, csvfile, rowcount, xml_toolspoint, params)
        # dirpath = cfg.get('collection_metadata_folder')
        # other_dirpath = cfg.get('metadata_folder_1')
        # fileobject, csvoutfile = u.opencsvout(cfg.get('output_csv'))
        # new_data = compare_files(dirpath, other_dirpath)
        # print(len(new_data))
        # csvoutfile.writerows(new_data)