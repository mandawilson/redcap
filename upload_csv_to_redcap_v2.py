# TODO trim fields from input file
# TODO we found modified value, but not new/deleted fields
#   make sure we get everything from `diff modified_darwin_eight_batches.csv /data/redcap_scratch/darwin_eight_batches.csv`
# TODO then delete any patient with modifed/deleted data, and add all their data back
# TODO then add any new fields (do not add if already added to deleted patient)
# WARNING this is ignoring *_complete fields TODO add back
# TODO check headers are the same
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
    # TODO Redcap is trimming the fields (I think), we should too
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
    #print "LOG: downloaded row", row
    if "redcap_repeat_instance" in row:
      if row["redcap_repeat_instance"] == "":
	if verbose:
          print "LOG: skipping row '%s' because it is a redcap_repeat_instance row with no redcap_repeat_instance, it is just an empty row with only patient id set" % (row)
        continue
      max_repeat_instance = max(max_repeat_instance, row["redcap_repeat_instance"])
      del row["redcap_repeat_instance"]
    # TODO remove this, we should probably include this column in the input file
    #   or somehow set it to be an optional column
    if "redcap_repeat_instrument" in row:
      del row["%s_complete" % (row["redcap_repeat_instrument"])]

    key = "".join([str(v) for k,v in sorted(row.items())])
    #print "LOG: saving key '%s', row '%s'" % (key, row)
    record_ids_to_key_to_old_dict[row[primary_key]][key] = row

  # TODO make sure headers are the same
  # now loop through each new record id and figure out if anything changed
  print "instruments_to_record_id_to_key_to_new_dict[instrument['instrument_name']] size:", len(instruments_to_record_id_to_key_to_new_dict[instrument["instrument_name"]])
  for record_id, key_to_new_dict in instruments_to_record_id_to_key_to_new_dict[instrument["instrument_name"]].iteritems():
    for key in key_to_new_dict:
      if verbose:
        print "LOG: checking record '%s' with key '%s'" % (record_id, key)
	#for old_key in record_ids_to_key_to_old_dict[record_id].iterkeys():
	#  print "LOG: old_key = '%s', key is type '%s'" % (old_key, type(old_key))
      if key not in record_ids_to_key_to_old_dict[record_id]:
        print "Did NOT find '%s' in old records" % (key)
	print "Example:", record_ids_to_key_to_old_dict[record_id].iterkeys().next() 
      #else:
      #  print "Found '%s' in old records" % (key)
    #sys.exit()
