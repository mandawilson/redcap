# API
import pycurl, cStringIO
import redcap_config
import json
from StringIO import StringIO
import sys

def update_records(pid, list_of_fields_to_values, overwrite="overwrite", return_content="count", verbose=False):
  """Updates any fields for a record included in the fields_to_values dictionary.  
    list_of_fields_to_values is a list of dictionaries, each dictionary
    containing the fields and their values for that item in the list.
    Overwrite is turned ON, so blank values will overwrite any value stored in RedCap.  
    The record id MUST be included.  redcap_repeat_instrument and redcap_repeat_instance
    are required for repeat instruments.  All dates must be in Y-M-D format regardless
    of the format definition for the field."""
  buf = cStringIO.StringIO()
  json_data = '[' + ",".join([ '{' + ",".join(["\"%s\":\"%s\"" % (field, str(value).replace("\"", "\\\"")) for field, value in fields_to_values.iteritems()]) + '}' for fields_to_values in list_of_fields_to_values ]) + ']'
  print "LOG:", json_data
  data = {
    'token': redcap_config.pids_to_tokens[pid],
    'content': 'record',
    'format': 'json',
    'type': 'flat',
    'overwriteBehavior': 'overwrite',
    'data': json_data,
    'returnContent': return_content,
    'returnFormat': 'json',
  }
  curl = pycurl.Curl()
  curl.setopt(curl.URL, redcap_config.api_url)
  curl.setopt(curl.HTTPPOST, data.items())
  curl.setopt(curl.WRITEFUNCTION, buf.write)
  #curl.setopt(pycurl.SSL_VERIFYPEER, 0)
  #curl.setopt(pycurl.SSL_VERIFYHOST, 0)
  curl.perform()
  curl.close()
  body = buf.getvalue()
  buf.close()
  return json.load(StringIO(body)) # returns count of updated records e.g. {"count": 1} or list of IDs that were updated
