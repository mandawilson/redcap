# TODO check 'grep P-0017103 test.log'
# How did I download 3 "old rows" and end up with 2 old row keys, only one looks correct?  
# And why didn't the correct keys match?  THey look the same, was it a utf8 difference?
import redcap
import redcap_config
import sys
from collections import defaultdict
import csv

# TODO these should be parameters
# TODO documentation at top and usage, validate parameters
pid = '77'
verbose = True
csv_filename = "modified_darwin_eight_batches.csv"
primary_key = "patient_id"

instruments_to_record_id_to_key_to_new_dict = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))

# TODO need the field mapping, and it needs to define the record_id
# read in CSV and store instrument -> record id -> rows
with open(csv_filename, 'rb') as csv_file:
  csvreader = csv.DictReader(csv_file, delimiter=',', quotechar='"')
  for row in csvreader:
    instrument_name = row["redcap_repeat_instrument"] if row["redcap_repeat_instrument"] else primary_key 
    # TODO this should not be included in the CSV file
    del row["redcap_repeat_instance"] # this might be different, so remove it from the record
    # sort values based on sorted keys, do not just sort values -- fields could be in different order but result in the same
    # (even though it is unlikely) -- do the same below too
    instruments_to_record_id_to_key_to_new_dict[instrument_name][row[primary_key]]["".join([v for k,v in sorted(row.items())])] = row

# download all data for project, one instrument at a time (to save some memory)
instruments = redcap.get_instruments(pid, verbose)
for instrument in instruments:
  record_ids_to_key_to_old_dict = defaultdict(lambda: defaultdict(dict))
  max_repeat_instance = None
  if verbose:
    print "LOG: downloading rows for %s" % (instrument["instrument_name"])
  old_rows = redcap.get_records(pid, instrument["instrument_name"], primary_key, verbose)
  for row in old_rows:
    print "LOG: downloaded row", row
    if "redcap_repeat_instance" in row:
      max_repeat_instance = max(max_repeat_instance, row["redcap_repeat_instance"])
      del row["redcap_repeat_instance"]
    record_ids_to_key_to_old_dict[row[primary_key]]["".join([v for k,v in sorted(row.items())])] = row

  # TODO make sure headers are the same
  # now loop through each new record id and figure out if anything changed
  print "instruments_to_record_id_to_key_to_new_dict[instrument['instrument_name']] size:", len(instruments_to_record_id_to_key_to_new_dict[instrument["instrument_name"]])
  for record_id, key_to_new_dict in instruments_to_record_id_to_key_to_new_dict[instrument["instrument_name"]].iteritems():
    for key in key_to_new_dict:
      if verbose:
        print "LOG: checking record '%s' with key '%s'" % (record_id, key)
	for old_key in record_ids_to_key_to_old_dict[record_id]:
	  print "LOG: old_key = ", old_key
      if key in record_ids_to_key_to_old_dict[record_id]:
        print "Found '%s' in old records" % (key)
      else:
        print "Did NOT find '%s' in old records" % (key)
	print "Example:", record_ids_to_key_to_old_dict[record_id].iterkeys().next() 
    sys.exit()

