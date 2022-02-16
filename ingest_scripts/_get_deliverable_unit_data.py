#!/usr/bin/python3
#~/anaconda3/bin/python

import json
import preservica_metadata_file_picker as pmfp
import _xml_tools as xml_tools
from utilities import utilities as u


def get_deliverable_units(cfg, full):
    if not full:
        entries = xml_tools.get_new_deliverable_units(cfg)
    else:
        entries = xml_tools.get_dus_from_collections(cfg)
    fileobject, csvoutfile = u.opencsvout(cfg.get('deliverable_unit_output_csv'))
    csvoutfile.writerow(['direct_download_link'])
    csvoutfile.writerows(entries)
    fileobject.close()

def download_deliverable_units(cfg):
    pmfp.start_process(cfg, 'deliverable_unit_output_csv', 'deliverable_unit_endpoint', 'deliverable_unit_params', 'deliverable_unit_output_folder')

def main():
    cfg = json.load(open('config.json'))
    print('Getting deliverable unit IDs...')
    get_deliverable_units(cfg)
    print('Downloading deliverable units...')
    download_deliverable_units(cfg)


if __name__ == "__main__":
    main()