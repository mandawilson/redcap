# Backup a RedCap project
#
# Usage:
#   backup_redcap_project.py [options] pid
#     Options:
#       -v, --verbose Turn on verbose logging
#   e.g. backup_redcap_project.py -v 99 > redcap_project.xml
#
# Author: Manda Wilson

import redcap
import redcap_config
import sys
import getopt

DEFAULT_PRIMARY_KEY = "record_id"

def download_large_project(pid, primary_key, verbose):
  # download all data for project, one instrument at a time
  # these are the 'old' records
  instruments = redcap.get_instruments(pid, verbose)
  header = set([])
  all_data = []
  for instrument in instruments:
    if verbose:
      print "LOG: downloading rows for %s" % (instrument["instrument_name"])
    rows = redcap.get_records(pid, instrument["instrument_name"], primary_key, verbose)
    for row in rows:
      header = header | set(row.keys())
      all_data.append(row) 

  sorted_header = sorted(list(header))
  print ",".join(sorted_header)
  for row in all_data:
    for key in sorted_header:
      if key not in row:
        row[key] = ""
    print ",".join(["\"%s\"" % (v) for k,v in sorted(row.items())])

def usage():
  print "Usage:"
  print "  backup_redcap_project.py [options] pid"
  print "    Options:"
  print "      -l, --large This is a large project, download one instrument at a time, output will be CSV, default is XML"
  print "      -k, --key key_name Primary key for RedCap study, default is record_id, only needed for large projects"
  print "      -v, --verbose Turn on verbose logging"
  print "  e.g. backup_redcap_project.py -v 99 > redcap_project.xml"
  print "  e.g. backup_redcap_project.py -l -k patient_id -v 98 > large_redcap_project.txt"

try:
  opts, args = getopt.getopt(sys.argv[1:], "vlk:", ["verbose", "large", "key="])
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

if pid not in redcap_config.pids_to_tokens:
  print >> sys.stderr, "ERROR: project id '%s' not a known project id.  Known project ids are: %s" % (pid, ", ".join(redcap_config.pids_to_tokens.keys()))
  sys.exit(1)

large = False
primary_key = None
verbose = False

for o, a in opts:
  if o in ("-v", "--verbose"):
    verbose = True
  elif o in ("-l", "--large"):
    large = True
  elif o in ("-k", "--key"):
    primary_key = a

if large and not primary_key:
  print >> sys.stderr, "ERROR: key is required for a large project download.  The default primary key in RedCap is '%s'." % (DEFAULT_PRIMARY_KEY)
  usage()
  sys.exit(1)

if verbose:
  print "LOG: pid =", pid
  print "LOG: verbose =", verbose 
  print "LOG: key =", key

if large:
  download_large_project(pid, primary_key, verbose=verbose)
else:
  print redcap.download_backup(pid, verbose=verbose)
