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

def usage():
  print "Usage:"
  print "  backup_redcap_project.py [options] pid"
  print "    Options:"
  print "      -v, --verbose Turn on verbose logging"
  print "  e.g. backup_redcap_project.py -v 99 > redcap_project.xml"

try:
  opts, args = getopt.getopt(sys.argv[1:], "v", ["verbose"])
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

verbose = False

for o, a in opts:
  if o in ("-v", "--verbose"):
    verbose = True

if verbose:
  print "LOG: pid =", pid
  print "LOG: verbose =", verbose 

print redcap.download_backup(pid, verbose=verbose)
