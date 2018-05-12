# TODO test that if we delete an entire record we find that
# TODO actually update redcap
# TODO make sure we get everything from `diff modified_darwin_eight_batches.csv /data/redcap_scratch/darwin_eight_batches.csv`
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

record_id_to_key_to_new_dict = defaultdict(lambda: defaultdict(dict))
record_id_to_key_to_old_dict = defaultdict(lambda: defaultdict(dict))
# TODO need the field mapping, and it needs to define the record_id
# read in CSV and store instrument -> record id -> rows
# these are the 'new' records
with open(csv_filename, 'rb') as csv_file:
  csvreader = csv.DictReader(csv_file, delimiter=',', quotechar='"')
  for row in csvreader:
    # Redcap is trimming the fields (I think), we should too
    row = { k:v.strip() for k, v in row.iteritems()}
    instrument_name = row["redcap_repeat_instrument"] if row["redcap_repeat_instrument"] else primary_key 
    # TODO this should not be included in the CSV file
    del row["redcap_repeat_instance"] # this might be different, so remove it from the record
    # sort values based on sorted keys, do not just sort values -- fields could be in different order but result in the same
    # (even though it is unlikely) -- do the same below too
    #print "LOG:", row
    record_id_to_key_to_new_dict[row[primary_key]]["".join([v for k,v in sorted(row.items())])] = row

# download all data for project, one instrument at a time
# these are the 'old' records
instruments = redcap.get_instruments(pid, verbose)
for instrument in instruments:
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
      del row["redcap_repeat_instance"]
    # TODO remove this, we should probably include this column in the input file
    #   or somehow set it to be an optional column
    complete_field_name = "%s_complete" % (instrument["instrument_name"])
    if complete_field_name in row:
      #print "LOG: deleting '%s' from row" % (complete_field_name)
      del row[complete_field_name]

    key = "".join([str(v) for k,v in sorted(row.items())])
    #print "LOG: saving key '%s', row '%s'" % (key, row)
    exit
    record_id_to_key_to_old_dict[row[primary_key]][key] = row

# what records are new?
# what records have been deleted?
new_record_id_set = set(record_id_to_key_to_new_dict.keys())
old_record_id_set = set(record_id_to_key_to_old_dict.keys())
deleted_record_ids = old_record_id_set - new_record_id_set # in old but not in new
added_record_ids = new_record_id_set - old_record_id_set # in new but not in old
records_in_both_sets = old_record_id_set & new_record_id_set # in new and in old
changed_record_ids = set([])

# for the records that were in redcap before and we still want, has any data changed?
# TODO make sure headers are the same
for record_id in records_in_both_sets:
  old_data_set = set(record_id_to_key_to_new_dict[record_id].keys())
  new_data_set = set(record_id_to_key_to_old_dict[record_id].keys())
  # get data in old set or new set, but NOT IN BOTH, if there is at least one record difference, 
  # this record needs to be deleted and re-added
  different_data = old_data_set ^ new_data_set
  if len(different_data) > 0:
    if verbose:
      for data in different_data:
        print "LOG: '%s' is different between the old and new records" % (data)
        if data in old_data_set:
          print "LOG: the above was found in the old data set dict is", record_id_to_key_to_old_dict[record_id][data]
        if data in new_data_set:
          print "LOG: the above was found in the new data set dict is", record_id_to_key_to_new_dict[record_id][data]

    changed_record_ids.add(record_id)

print "LOG: %d records in old set" % (len(old_record_id_set))
print "LOG: %d records in new set" % (len(new_record_id_set))
print "LOG: %d added records" % (len(added_record_ids))
print "LOG: %d deleted records" % (len(deleted_record_ids))
print "LOG: %d records changed" % (len(changed_record_ids))

# TODO delete data for modified records and deleted records
record_ids_to_delete = changed_record_ids | deleted_record_ids
record_ids_to_add = changed_record_ids | added_record_ids
for record_id in record_ids_to_delete:
  if verbose:
    print "LOG: delete record '%s'" % (record_id)
for record_id in record_ids_to_add:
  if verbose:
    print "LOG: add record '%s'" % (record_id)
