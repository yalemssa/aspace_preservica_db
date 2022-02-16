#!/usr/bin/python3
#~/anaconda3/bin/python

import json
import preservica_metadata_file_picker as pmfp
import _xml_tools as xml_tools
from utilities import utilities as u

def get_manifestations(cfg, full):
    if not full:
        print('Getting new manifestations...')
        entries = xml_tools.get_new_manifestations(cfg)
        print(len(entries))
    else:
        entries = xml_tools.get_manifestations_from_dus(cfg)
    fileobject, csvoutfile = u.opencsvout(cfg.get('manifestation_output_csv'))
    csvoutfile.writerow(['direct_download_link'])
    csvoutfile.writerows(entries)
    fileobject.close()

def download_manifestations(cfg):
    pmfp.start_process(cfg, 'manifestation_output_csv', 'manifestation_endpoint', 'manifestation_params', 'manifestation_output_folder')

def main(full=False):
    cfg = json.load(open('config.json'))
    print('Getting manifestation IDs')
    get_manifestations(cfg, full)
    print('Downloading new manifestations...')
    download_manifestations(cfg)


if __name__ == "__main__":
    main()