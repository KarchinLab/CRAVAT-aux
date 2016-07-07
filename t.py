import sys
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('test_cases', help='list of test cases')
parser.add_argument('-ex','--exclude', help='list of cases to exclude')

args = parser.parse_args()

print args.test_cases
if args.exclude:
	print args.exclude