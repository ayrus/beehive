import json
import numpy as np
from pprint import pprint
def process_flle(f):
	json_data=open(f)

	data = json.load(json_data)

	gets = []
	puts = []

	for d in data:
		if d['method'] == 'GET':
			gets.append(d['dur'])
		elif d['method'] == "PUT":
			puts.append(d['dur'])
	print "\t\tPUTS -  " + str(np.mean(puts))
	print "\t\tGETS -  " + str(np.mean(gets))
	json_data.close()

process_file("bench.out")
