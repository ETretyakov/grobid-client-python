import os
import io
import json
import argparse
import time
import concurrent.futures
from client import ApiClient
import ntpath
import requests

'''
This version uses the standard ProcessPoolExecutor for parallelizing the concurrent calls to the GROBID services. 
Given the limits of ThreadPoolExecutor (input stored in memory, blocking Executor.map until the whole input
is acquired), it works with batches of PDF of a size indicated in the config.json file (default is 1000 entries). 
We are moving from first batch to the second one only when the first is entirely processed - which means it is
slightly sub-optimal, but should scale better. However acquiring a list of million of files in directories would
require something scalable too, which is not implemented for the moment.   
'''


class GrobidClient(ApiClient):

    def __init__(self, config_path='./config.json', **kwargs):

        self.config = None

        self._load_config(config_path)

        self.api_url = f"http://{self.config['grobid_server']}"
        if self.config['grobid_port']:
            self.api_url = f"{self.api_url}:{self.config['grobid_port']}"

        if not self._is_alive():
            raise ConnectionError("Grobid server is down.")

        base_url = self.config["grobid_server"]

        self.number_of_processes = kwargs.get("number_of_processes", self.config["number_of_processes"])
        self.generate_ids = kwargs.get("generate_ids", True)
        self.consolidate_header = kwargs.get("consolidate_header", True)
        self.consolidate_citations = kwargs.get("consolidate_citations", True)
        self.force = kwargs.get("force", True)
        self.tei_coordinates = kwargs.get("tei_coordinates", True)

        self.service = None
        self.output_directory = None

        super().__init__(base_url)

    def _load_config(self, path='./config.json'):
        with open(path, "r", encoding="utf-8") as file:
            self.config = json.loads(file.read())

        print(f"Configuration loaded: {path}")

    def _is_alive(self):
        status_url = f"{self.api_url}/api/isalive"

        response = requests.get(status_url)
        status_code = response.status_code

        if status_code != 200:
            print(f"GROBID server does not appear up and running, status code: {status_code}")
            return False
        else:
            print("GROBID server is up and running")
            return True

    def process(self, service, input_directory, output_directory):
        self.service = service
        self.output_directory = output_directory

        batch_size_pdf = self.config["batch_size"]
        pdf_files_paths = []

        for (directory_path, directory_names, filenames) in os.walk(input_directory):
            for filename in filenames:
                if filename.endswith('.pdf') or filename.endswith('.PDF'):
                    pdf_files_paths.append(os.sep.join([directory_path, filename]))

                    if len(pdf_files_paths) == batch_size_pdf:
                        self.process_batch(pdf_files_paths)
                        pdf_files_paths = []

        if len(pdf_files_paths) > 0:
            self.process_batch(pdf_files_paths)

    def process_batch(self, pdf_files_paths):

        print(f"PDF files to process: {len(pdf_files_paths)}")

        with concurrent.futures.ProcessPoolExecutor(max_workers=n) as executor:
            for pdf_files_path in pdf_files_paths:
                executor.submit(self.process_pdf, pdf_files_path)

    def process_pdf(self, pdf_file_path):
        pdf_file_name = ntpath.basename(pdf_file_path)
        if self.output_directory is not None:
            filename = os.path.join(self.output_directory, os.path.splitext(pdf_file_name)[0] + '.tei.xml')
        else:
            filename = os.path.join(ntpath.dirname(pdf_file_path), os.path.splitext(pdf_file_name)[0] + '.tei.xml')

        if not self.force and os.path.isfile(filename):
            print(f"{filename} is already exist, skipping... (use --force to reprocess pdf input files)")
            return

        print(f"Processing -> {pdf_file_path}")

        with open(pdf_file_path, "rb") as file:
            files = {
                "input": (pdf_file_path, file, 'application/pdf', {'Expires': '0'})
            }
            self.api_url = f"{self.api_url}/api/{self.service}"
            data = self._prepare_post_data()

            response, status_code = self.post(url=self.api_url, files=files, data=data,
                                              headers={'Accept': 'text/plain'})

        if status_code == 503:
            time.sleep(self.config['sleep_time'])
            return self.process_pdf(pdf_file_path)
        elif status_code != 200:
            print(f"Processing failed, response status code: {status_code}")
        else:

            try:
                with io.open(filename, "w", encoding='utf8') as tei_file:
                    tei_file.write(response.text)
            except OSError as error:
                print(f"Processing failed for {filename} [ERROR]: {error}")

    def _prepare_post_data(self):
        data = {}
        if self.generate_ids:
            data['generate_ids'] = '1'
        if self.consolidate_header:
            data['consolidate_header'] = '1'
        if self.consolidate_citations:
            data['consolidate_citations'] = '1'
        if self.tei_coordinates:
            data['tei_coordinates'] = self.config['coordinates']

        return data


if __name__ == "__main__":
    instructions = {
        "service": "one of [processFulltextDocument, processHeaderDocument, processReferences]",
        "input": "path to the directory containing PDF to process",
        "output": "path to the directory where to put the results (optional)",
        "config": "path to the config file, default is ./config.json",
        "n": "concurrency for service usage",
        "generateIDs": "generate random xml:id to textual XML elements of the result files",
        "consolidate_header": "call GROBID with consolidation of the metadata extracted from the header",
        "consolidate_citations": "call GROBID with consolidation of the extracted bibliographical references",
        "force": "force re-processing pdf input files when tei output files already exist",
        "teiCoordinates": "add the original PDF coordinates (bounding boxes) to the extracted elements"
    }

    parser = argparse.ArgumentParser(description="Client for GROBID services")
    parser.add_argument("service", help=instructions["service"])
    parser.add_argument("--input", default=None, help=instructions["input"])
    parser.add_argument("--output", default=None, help=instructions["output"])
    parser.add_argument("--config", default="./config.json", help=instructions["config"])
    parser.add_argument("--n", default=10, help=instructions["n"])
    parser.add_argument("--generateIDs", action='store_true', help=instructions["generateIDs"])
    parser.add_argument("--consolidate_header", action='store_true', help=instructions["consolidate_header"])
    parser.add_argument("--consolidate_citations", action='store_true', help=instructions["consolidate_citations"])
    parser.add_argument("--force", action='store_true', help=instructions["force"])
    parser.add_argument("--teiCoordinates", action='store_true', help=instructions["teiCoordinates"])

    args = parser.parse_args()

    n = 10
    if args.n is not None:
        try:
            n = int(args.n)
        except ValueError:
            print(f"Invalid concurrency for parameter n: {n}, n = 10 will be used by default")

    if args.output is not None and not os.path.isdir(args.output):
        try:
            print(f"Output directory does not exist but will be created: {args.output}")
            os.makedirs(args.output, exist_ok=True)
        except OSError:
            print(f"Creation of the directory {args.output} failed")
        else:
            print(f"Successfully created the directory {args.output}")

    options = {
        "number_of_processes": n,
        "generate_ids": args.generateIDs,
        "consolidate_header": args.consolidate_header,
        "consolidate_citations": args.consolidate_citations,
        "force": args.force,
        "tei_coordinates": args.teiCoordinates,
    }

    client = GrobidClient(config_path=args.config, **options)

    start_time = time.time()

    client.process(args.service, args.input, args.output)

    runtime = round(time.time() - start_time, 3)
    print(f"Runtime: {runtime} seconds")
