#!/usr/bin/python3
#~/anaconda3/bin/python

import json
import preservica_metadata_file_picker as pmfp
import _xml_tools as xml_tools
from utilities import utilities as u

def get_digital_files(cfg, full):
    if not full:
        entries = xml_tools.get_new_digital_files(cfg)
    else:
        entries = xml_tools.get_digital_files_from_dus(cfg)
    fileobject, csvoutfile = u.opencsvout(cfg.get('digital_file_output_csv'))
    csvoutfile.writerow(['direct_download_link'])
    csvoutfile.writerows(entries)
    fileobject.close()

def download_digital_files(cfg):
    pmfp.start_process(cfg, 'digital_file_output_csv', 'digital_file_endpoint', 'digital_file_params', 'digital_file_output_folder')

def main(full=False):
    cfg = json.load(open('config.json'))
    print('Getting digital file IDs...')
    get_digital_files(cfg, full)
    print('Downloading new digital files...')
    download_digital_files(cfg)
