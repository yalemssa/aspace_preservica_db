#/usr/bin/python3

import csv
from datetime import datetime
import json
import os
import sys
import traceback
from concurrent.futures import ThreadPoolExecutor

import requests
from rich import print
from rich.progress import track
from rich.padding import Padding

from data_checker import get_row_counts
from _constants import console, progress

'''Script to retrieve metadata from Preservica content API'''


class PreservicaDownloader():
    def __init__(self, dirpath, username, pw, tenant, api_url):
        self.dirpath = dirpath
        self.username = username
        self.pw = pw
        self.tenant = tenant
        self.api_url = api_url
        self.session = requests.Session()
        self.token = self.__token__()
        
    def __token__(self):
        response = requests.post(f'https://{self.api_url}accesstoken/login?username={self.username}&password={self.pw}&tenant={self.tenant}')
        if response.status_code == 200:
            console.log('[bold green]Connected.[/bold green]')
            return response.json()['token']
        else:
            console.log(f"[bold red]Get new token failed with error code: {response.status_code}[/bold red]")
            console.log(response.request.url)
            raise SystemExit

    def toggle_params(self, params):
        if 'includeParentHierarchy=true&includeChildHierarchy=true' in params:
            params = '?includeChildHierarchy=true'
        elif '?includeChildHierarchy=true' in params:
            params = '?includeParentHierarchy=true'
        elif '?includeParentHierarchy=true' in params:
            params = ""
        return params

    def send_request(self, csv_row, endpoint, params, master_task=None, try_number=0):
        try:
            headers = {'Preservica-Access-Token': self.token}
            url = csv_row['direct_download_link']
            full_url = f"{endpoint}/{url}{params}"
            with self.session.get(full_url, headers=headers) as req:
                if req.status_code == 200:
                    try:
                        with open(f"{self.dirpath}/preservica_{url}.xml", 'w') as outfile:
                            outfile.write(req.text)
                        if master_task is not None:
                            progress.advance(master_task, 1)
                    except Exception:
                        console.print_exception()
                elif req.status_code == 401:
                    self.token = self.__token__()
                    return self.send_request(csv_row, endpoint, params, master_task)
                elif req.status_code == 502:
                    console.print(f"502 error on {full_url}, trying again...")
                    if try_number != 0:
                        params = self.toggle_params(params)
                    console.print(f"Try number {try_number}: {params}")
                    return self.send_request(csv_row, endpoint, params, master_task, try_number=try_number + 1)
                elif req.status_code != 200:
                    console.print(f"{req.status_code} error on: {full_url}")
                    return f'SOME ERROR: {req.status_code}'
        # except (ConnectionError, HTTPError, urllib3.exceptions.ProtocolError, http.client.RemoteDisconnected):
        #     console.print(f"Connection error on: {full_url}, trying again")
        #     return self.send_request(csv_row, endpoint, params, master_task)
        # Catching all exceptions, since the above did not work for some reason??
        except Exception:
            console.print_exception()
            console.print(f'Trying {full_url} again...')
            return self.send_request(csv_row, endpoint, params, master_task)

    def process_results(self, datafile, filename):
        with open(f"{self.dirpath}/{filename}", 'wb') as outfile:
            outfile.write(datafile)


def configure(config_file, field):
    if config_file.get(field) != "":
        return config_file.get(field)
    else:
        return console.input(f"[#6c99bb]Please enter the {field}: [/#6c99bb]")

def configure_password(config_file):
    pres_password = config_file.get('preservica_password')
    if pres_password != "":
        return pres_password
    else:
        return console.input(f"[#6c99bb]Please enter your preservica_password: [/#6c99bb]", password=True)

def get_row_counts(fp):
    with open(fp, 'r', encoding='utf8') as datafile:
        csv_reader = csv.reader(datafile)
        #skips the header row
        next(csv_reader)
        return len(list(csv_reader))

#Open a CSV in dictreader mode
def opencsvdict(input_csv=None):
    """Opens a CSV in DictReader mode."""
    try:
        if input_csv is None:
            input_csv = console.input('[#6c99bb]Please enter path to input CSV file: [/#6c99bb]')
        if input_csv in ('quit', 'Q', 'Quit'):
            raise SystemExit
        infile = open(input_csv, 'r', encoding='utf-8')
        rowcount = get_row_counts(input_csv)
        return csv.DictReader(infile), rowcount, input_csv
    except FileNotFoundError:
        return opencsvdict()

def welcome():
    console.log("[#b05279]Starting...[/#b05279]")
    console.rule()
    console.rule("[color(11)]Hello![/color(11)]")
    console.rule("[color(11)]This is the Preservica File Picker[/color(11)]")
    console.rule()
    console.log("[#b05279]Checking your credentials...[/#b05279]")    

def wrap_up():
    console.rule("[color(11)]Goodbye![/color(11)]")
    console.log("[#b05279]Exiting...[#b05279]")
    #console.save_text('console_output.txt')

def run_thread_pool_executor(client, csvfile, rowcount, endpoint, params):
    master_task = progress.add_task("overall", total=rowcount, filename="Overall Progress")
    with progress:
        with ThreadPoolExecutor(max_workers=8) as pool:
            try:
                for row in csvfile:
                    pool.submit(client.send_request, row, endpoint, params, master_task)
            except Exception:
                console.print_exception()

def start_process(cfg: dict, cfg_csv: str, cfg_endpoint: str, cfg_params: str, cfg_output: str):
    welcome()
    csvfile, rowcount, input_csv = opencsvdict(cfg.get(cfg_csv))
    endpoint = cfg.get(cfg_endpoint)
    params = cfg.get(cfg_params)
    try:
        client = PreservicaDownloader(configure(cfg, cfg_output), configure(cfg, 'preservica_username'), configure_password(cfg), configure(cfg, 'tenant'), configure(cfg, 'preservica_api_url'))
        run_thread_pool_executor(client, csvfile, rowcount, endpoint, params)
    except (KeyboardInterrupt, SystemExit):
        console.print('[bold red]Aborted![/bold red]')
    except Exception:
        console.print_exception()
        console.log(traceback.format_exc())
    finally:
        wrap_up()

def main():
    cfg = json.load(open('config.json'))
    start_process(cfg, 'test_output_csv', 'test_endpoint', 'test_params', 'test_output_folder')


if __name__ == "__main__":
    main()
