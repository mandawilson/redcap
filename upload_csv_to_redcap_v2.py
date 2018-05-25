# Read CSV file made to be loaded into redcap via the import tool,
# compare to values currently stored in redcap,
# and update project in redcap with changes only, using the API
#
# Usage:
#   upload_csv_to_redcap.py [options] pid input_filename
#     Options:
#       -k, --key key_name Primary key for RedCap study, default is record_id
#       -c, --chunk chunk_size Number of records to upload in one POST request, default is 100 (CURRENTLY IGNORED)
#       -d, --delete if a record in RedCap is missing from this file (entire record, not just one row of data)
#           then delete it from RedCap.  By default we delete rows but not entire records)
#       -f, --force update RedCap, by default this will be a dry run with no changes made to RedCap
#       -v, --verbose Turn on verbose logging
#   e.g. upload_csv_to_redcap.py -k project_id -c 10 -v 99 redcap.csv
#
# Author: Manda Wilson

# TODO actually update redcap -- I backed up database already just run with -f option - do twice, second time with -d option
#   then use original file to get things back to the way they were
# TODO make sure we get everything from `diff modified_darwin_eight_batches.csv /data/redcap_scratch/darwin_eight_batches.csv`
# WARNING this is ignoring *_complete fields TODO add back?
import redcap
import redcap_config
import sys
from collections import defaultdict
import csv
import os.path
import getopt

DEFAULT_CHUNK = 100
DEFAULT_PRIMARY_KEY = "record_id"

def update_redcap(expected_ids, pid, current_rows, overwrite="normal", return_content="ids", verbose=False):
  updated = redcap.update_records(pid, current_rows, overwrite="normal", return_content="ids", verbose=verbose)
  if verbose:
    print "LOG: updated:", updated
  if "error" in updated:
    print >> sys.stderr, "ERROR: failed to update record(s).  Message is '%s'.  JSON is '%s'" % (updated["error"], updated)

  failed_to_update = expected_ids - set(updated)
  if failed_to_update: 
    print >> sys.stderr, "ERROR: failed to update record(s): '%s'" % (",".join(failed_to_update))
    sys.exit(1)

def get_data_ready_for_redcap(full_record_data, verbose=False):
  updated_record = []
  instrument_to_next_redcap_repeat_instance = defaultdict(int)
  for row in full_record_data.values():
    # if this is a redcap_repeat_instrument, set the redcap_repeat_instance
    if "redcap_repeat_instrument" in row and row["redcap_repeat_instrument"]:
      instrument_to_next_redcap_repeat_instance[row["redcap_repeat_instrument"]] += 1
      row["redcap_repeat_instance"] = instrument_to_next_redcap_repeat_instance[row["redcap_repeat_instrument"]] 
    if verbose:
      print "LOG: updated/added Redcap row is '%s'" % (",".join([ "%s:%s" % (k, v) for k,v in row.iteritems()]))
    updated_record.append(row)
  return updated_record  

def run(pid, input_stream, primary_key, chunk, delete=False, force=False, verbose=False):
  record_id_to_key_to_new_dict = defaultdict(lambda: defaultdict(dict))
  record_id_to_key_to_old_dict = defaultdict(lambda: defaultdict(dict))
  new_header = set([])
  old_header = set([])
  # TODO need the field mapping, and it needs to define the record_id
  # read in CSV and store instrument -> record id -> rows
  # these are the 'new' records
  csvreader = csv.DictReader(input_stream, delimiter=',', quotechar='"')
  for row in csvreader:
    # Redcap is trimming the fields (I think), we should too
    row = { k:v.strip() for k, v in row.iteritems()}
    instrument_name = row["redcap_repeat_instrument"] if row["redcap_repeat_instrument"] else primary_key 
    # TODO this should not be included in the CSV file
    del row["redcap_repeat_instance"] # this might be different, so remove it from the record
    # sort values based on sorted keys, do not just sort values -- fields could be in different order but result in the same
    # (even though it is unlikely) -- do the same below too
    #print "LOG:", row
    new_header = set(row.keys())
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
          #if verbose:
          #  print "LOG: skipping row '%s' because it is a redcap_repeat_instance row with no redcap_repeat_instance, it is just an empty row with only patient id set" % (row)
          continue
        del row["redcap_repeat_instance"]
            
      # TODO remove this, we should probably include this column in the input file
      #   or somehow set it to be an optional column
      complete_field_name = "%s_complete" % (instrument["instrument_name"])
      if complete_field_name in row:
        #print "LOG: deleting '%s' from row" % (complete_field_name)
        del row[complete_field_name]
      old_header = set(row.keys()) | old_header
  
      key = "".join([str(v) for k,v in sorted(row.items())])
      #print "LOG: saving key '%s', row '%s'" % (key, row)
      record_id_to_key_to_old_dict[row[primary_key]][key] = row

  header_diff = new_header ^ old_header
  if len(header_diff) > 0:
    print >> sys.stderr, "ERROR: Redcap header and input header differ."
    in_redcap_not_input = old_header - new_header
    in_input_not_redcap = new_header - old_header
    if len(in_redcap_not_input) > 1:
      print >> sys.stderr, "ERROR: Redcap header contains '%s' which are missing from input" % (", ".join(in_redcap_not_input))
    if len(in_input_not_redcap) > 1:
      print >> sys.stderr, "ERROR: Input header contains '%s' which are missing from Redcap" % (", ".join(in_input_not_redcap))
    sys.exit(1)
  
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
 
  if verbose: 
    print "LOG: %d records in old set" % (len(old_record_id_set))
    print "LOG: %d records in new set" % (len(new_record_id_set))
    print "LOG: %d added records: '%s'" % (len(added_record_ids), ",".join(added_record_ids))
    print "LOG: %d deleted records: '%s'" % (len(deleted_record_ids), ",".join(deleted_record_ids))
    print "LOG: Deleting the above records? %s" % (delete)
    print "LOG: %d records changed: '%s'" % (len(changed_record_ids), ",".join(changed_record_ids))
  
  # TODO delete data for modified records and deleted records
  record_ids_to_delete = changed_record_ids
  # we might not want to remove deleted records
  if delete:
    record_ids_to_delete = record_ids_to_delete | deleted_record_ids
  record_ids_to_add = changed_record_ids | added_record_ids
  if verbose:
    print "LOG: we will actually delete %d records from Redcap: '%s'" % (len(record_ids_to_delete), ",".join(record_ids_to_delete))
    print "LOG: we will then add/add back %d records to Redcap '%s'" % (len(record_ids_to_add), ",".join(record_ids_to_add))
  for record_id in record_ids_to_delete:
    if force:
      if verbose:
        print "LOG: deleting record '%s'" % (record_id)
      redcap_deleted_record_count = redcap.delete_record(pid, record_id, verbose)
      if redcap_deleted_record_count != 1:
        print >> sys.stderr, "ERROR: failed to delete record '%s'" % (record_id)
      else:
        if verbose:
          print "LOG: successfully deleted record '%s' from RedCap" % (record_id)
    else:
      if verbose:
        print "LOG: [TEST NOT] deleting record '%s'" % (record_id)

  for record_id in record_ids_to_add:
    if force:
      if verbose:
        print "LOG: adding record '%s'" % (record_id)
      # TODO get rows for record_id
      # TODO fill in instrument index number
      redcap_data = get_data_ready_for_redcap(record_id_to_key_to_new_dict[record_id], verbose)
      update_redcap(set([record_id]), pid, redcap_data, overwrite="normal", return_content="ids", verbose=verbose)
    else:
      if verbose:
        print "LOG: [TEST NOT] adding record '%s'" % (record_id)

def usage():
  print "Usage:"
  print "  upload_csv_to_redcap.py [options] pid input_filename"
  print "    Options:"
  print "      -k, --key key_name Primary key for RedCap study, default is record_id"
  print "      -c, --chunk chunk_size Number of records to upload in one POST request, default is", DEFAULT_CHUNK
  print "      -d, --delete if a record in RedCap is missing from this file (entire record, not just one row of data)"
  print "          then delete it from RedCap.  By default we delete rows but not entire records)"
  print "      -f, --force update RedCap, by default this will be a dry run with no changes made to RedCap"
  print "      -v, --verbose Turn on verbose logging"
  print "  e.g. upload_csv_to_redcap.py -k project_id -c 10 -v 99 redcap.csv"

try:
  opts, args = getopt.getopt(sys.argv[1:], "k:c:dfv", ["key=", "chunk=", "delete", "force", "verbose"])
except getopt.GetoptError as err:
  # print help information and exit:
  print str(err) # will print something like "option -a not recognized"
  usage()
  sys.exit(1)

if len(args) < 1:
  print >> sys.stderr, "ERROR: project id is required"
  usage()
  sys.exit(1)

pid = args[0]
input_filename = args[1] if len(args) == 2 else None

if pid not in redcap_config.pids_to_tokens:
  print >> sys.stderr, "ERROR: project id '%s' not a known project id.  Known project ids are: %s" % (pid, ", ".join(redcap_config.pids_to_tokens.keys()))
  sys.exit(1)

if input_filename and not os.path.isfile(input_filename):
  print >> sys.stderr, "ERROR: '%s' is not a file" % (input_filename)
  sys.exit(1)

primary_key = DEFAULT_PRIMARY_KEY
chunk = DEFAULT_CHUNK
delete = False
force = False
verbose = False

for o, a in opts:
  if o in ("-v", "--verbose"):
    verbose = True
  elif o in ("-d", "--delete"):
    delete = True
  elif o in ("-f", "--force"):
    force = True
  elif o in ("-k", "--key"):
    primary_key = a
  elif o in ("-c", "--chunk"):
    try:
      chunk = int(a)
    except ValueError:
      print >> sys.stderr, "ERROR:", a, "is not a valid integer for chunk size"
      usage()
      sys.exit(1) 

if verbose:
  print "LOG: pid =", pid
  print "LOG: input_filename =", input_filename if input_filename else "reading piped input"
  print "LOG: primary_key =", primary_key 
  print "LOG: chunk =", chunk
  print "LOG: delete =", delete
  print "LOG: force =", force 
  print "LOG: verbose =", verbose 

if input_filename:
  with open(input_filename, 'rb') as input_file:
    run(pid, input_file, primary_key, chunk, delete, force, verbose)
else:
  run(pid, sys.stdin, primary_key, chunk, delete, force, verbose)
