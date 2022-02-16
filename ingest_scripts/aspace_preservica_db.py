#/usr/bin/python3

import download_preservica_data
import ingest_data
import data_checker
import file_tools


def main():
	download_preservica_data.main()
	input('Press enter to run ingest scripts... ')
	# add config options here
	ingest_data.main()
	input('Press enter to clean up directories... ')
	file_tools.main(ingest_version=14)
	input('Press enter to run data checker scripts... ')
	data_checker.main()

if __name__ == "__main__":
	main()