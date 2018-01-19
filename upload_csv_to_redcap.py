# Read CSV file made to be loaded into redcap via the import tool and update project using API
#
# Usage:
#   upload_csv_to_redcap.py [options] pid input_filename
#     Options:
#       -k, --key key_name Primary key for RedCap study, default is record_id
#       -c, --chunk chunk_size Number of records to upload in one POST request, default is 100
#       -v, --verbose Turn on verbose logging
#   e.g. upload_csv_to_redcap.py -k project_id -c 10 -v 99 redcap.csv
#
# Author: Manda Wilson

import redcap
import redcap_config
import csv
import sys
import os.path
import getopt

DEFAULT_CHUNK = 100
DEFAULT_PRIMARY_KEY = "record_id"

def update_redcap(expected_ids, pid, current_rows, overwrite="normal", return_content="ids", verbose=False):
  updated = redcap.update_records(pid, current_rows, overwrite="normal", return_content="ids", verbose=verbose)
  if verbose:
    print "LOG: updated:", updated
  if "error" in updated:
    print >> sys.stderr, "ERROR: failed to update record(s).  Message is '%s'.  JSON is '%s'" % (updated["error"], row)
    print >> sys.stderr, [row]
    sys.exit()

  failed_to_update = expected_ids - set(updated)
  if failed_to_update: 
    print >> sys.stderr, "ERROR: failed to update record(s): '%s'" % (",".join(failed_to_update))

def parse_csv(pid, input_stream, primary_key, chunk, verbose=False):
  csvreader = csv.DictReader(input_stream, delimiter=',', quotechar='"')
  current_rows = []
  expected_ids = set([])
  for row in csvreader:
    expected_ids.add(row[primary_key])
    current_rows.append(row)
    if len(current_rows) == chunk:
      update_redcap(expected_ids, pid, current_rows, overwrite="normal", return_content="ids", verbose=verbose)
      current_rows = [] # reset for next batch  
      expected_ids = set([])

  if len(current_rows) > 0:
    update_redcap(expected_ids, pid, current_rows, overwrite="normal", return_content="ids", verbose=verbose)


def usage():
  print "Usage:"
  print "  upload_csv_to_redcap.py [options] pid input_filename"
  print "    Options:"
  print "      -k, --key key_name Primary key for RedCap study, default is record_id"
  print "      -c, --chunk chunk_size Number of records to upload in one POST request, default is", DEFAULT_CHUNK
  print "      -v, --verbose Turn on verbose logging"
  print "  e.g. upload_csv_to_redcap.py -k project_id -c 10 -v 99 redcap.csv"
  

try:
  opts, args = getopt.getopt(sys.argv[1:], "k:c:v", ["key=", "chunk=", "verbose"])
except getopt.GetoptError as err:
  # print help information and exit:
  print str(err) # will print something like "option -a not recognized"
  usage()
  sys.exit(1)

if args < 1:
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
verbose = False

for o, a in opts:
  if o in ("-v", "--verbose"):
    verbose = True
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
  print "LOG: verbose =", verbose 

if input_filename:
  with open(input_filename, 'rb') as input_file:
    parse_csv(pid, input_file, primary_key, chunk, verbose)
else:
  parse_csv(pid, sys.stdin, primary_key, chunk, verbose)
