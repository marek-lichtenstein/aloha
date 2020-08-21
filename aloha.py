import sys
import argparse
import re
from files.setup import Setup


if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	group_src = parser.add_mutually_exclusive_group()
	group_src.add_argument("-ff", "--from-file", help="option to populate db with data from file", default="persons.json", type=str)
	group_src.add_argument("-dl", "--download", help="option to populate db with data from randomuser.me API", type=int)
	group_task = parser.add_mutually_exclusive_group()
	group_task.add_argument("-avg", "--average-age", help="display average age of a requested gender", type=str, choices=['male', 'female'])
	group_task.add_argument("-gpct", "--gender-pct", help="display percentage of selected gender", type=str, choices=['male', 'female'])
	group_task.add_argument("-pwds", "--most-freq-pwds", help="display most frequent passwords", type=int)
	group_task.add_argument("-cts", "--most-freq-cities", help="display most frequent cities")
	group_task.add_argument("-safe", "--safest-pwd", help="display safest password(s)", action="store_true")
	group_task.add_argument("-rng", "--range-dob", help="display usernames of users born in a selected period of time", nargs=2, type=str)
	args=parser.parse_args()
	
	if args.download:
		print("Initialization...")
		api = Setup().insert_downloaded_data(args.download)
	else:
		print("Initialization...")
		api = Setup().insert_data_from_file()
		print("Done")

	if args.average_age:
		print(api.average_age(args.average_age))
		sys.exit(1)	

	if args.gender_pct:
		print(api.gender_pct(args.gender_pct))
		sys.exit(1)

	if args.most_freq_pwds:	
		print(api.most_freq_pwds(args.most_freq_pwds))
		sys.exit(1)	

	if args.most_freq_cities:
		print(api.most_freq_cities(args.most_freq_cities))
		sys.exit(1)	

	if args.safest_pwd:
		print(api.safest_pwd())
		sys.exit(1)
	
	if args.range_dob:
		import re
		rec = re.compile('\d{4}-\d{2}-\d{2}', re.ASCII)
		for arg in args.range_dob:
			if not rec.match(arg):
				print("Unsupported date format: not YYYY-MM-DD")
				sys.exit(0)	
		print(api.range_dob(*args.range_dob))
		sys.exit(1)	
