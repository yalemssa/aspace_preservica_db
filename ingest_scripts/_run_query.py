#!/usr/bin/python3

from datetime import datetime
import pprint
import timeit
import warnings

#I don't know if I should use this

from utilities import db
from utilities import utilities as u

'''Simple interface for running local MySQL queries. To use, just run on the command line
and follow on-screen instructions.

What about using CLICK???
Fix table display for long records
'''

def get_columns(list_input):
	#how to get this all at the same time?
	for row in list_input:
	     print(" | ".join(word.ljust(15) for word in row if word is not None))

def open_query(query_fp):
	'''Asks for user to input path to query, opens query and returns it as a string'''
	qstring = open(query_fp, 'r', encoding='utf8').read()
	return qstring

def generate_outfile_path(query_fp, results):
	'''Takes the query file path and the length of the results and returns a filepath string for the outfile'''
	ofp = datetime.isoformat(datetime.now())[:-7].replace(':', '.')
	outfile_path = f'{query_fp[:-4]}_results_{len(results)}_{ofp}.csv'
	return outfile_path	

def write_outfile(outfile_path, metadata, results):
	'''Opens an outfile, writes the headers and results, closes the outfile, opens the file
	in the default spreadsheet program'''
	fileobject, csvoutfile = u.opencsvout(outfile_path)
	csvoutfile.writerow(metadata)
	csvoutfile.writerows(results)
	u.open_outfile(outfile_path)
	fileobject.close()

def print_data(execution_time, results):
	print(f'Finishing query at {datetime.isoformat(datetime.now())[:-7]}')
	print(f'Query executed in {execution_time} seconds')
	print(f'Number of results: {len(results)}')

def main():
	with warnings.catch_warnings():
		warnings.simplefilter("ignore")
		dbconn = db.DBConn()
			#start the timer
		start = timeit.default_timer()
		query_fp = input(f'Enter path to query: ')
		#get the path to the SQL file and open the SQL file
		query_string = open_query(query_fp)
		print(f'Starting query at {datetime.isoformat(datetime.now())[:-7]}')
		#Run the query. Would this be faster in a generator?
		results, header_row = dbconn.run_query_list(query_string)
		outfile_path = generate_outfile_path(query_fp, results)
		write_outfile(outfile_path, header_row, results)
		#stop the timer and print execution time
		#I think I have this in a decorator
		stop = timeit.default_timer()
		execution_time = stop - start
		print_data(execution_time, results)
		if input('Would you like to print your results to the console? Enter Y if yes, any key if no: ') == 'Y':
			get_columns(results)
			#print(tabulate((results), headers=metadata, tablefmt='orgtbl'))
		#figure out a way to pass in the filename here...so don't hafe to press a key and THEN enter name
		while input('Press any key to run another query. Enter Q to quit: ') != 'Q':
			main(dbconn)
		else:
			dbconn.close_conn()
			print('All Done!')


if __name__ == "__main__":
	main()
