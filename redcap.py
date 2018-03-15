# API
import pycurl, cStringIO
import redcap_config
import json
from StringIO import StringIO
import sys


def escape_str(field_for_json):
  return field_for_json.replace("\\", "\\\\").replace("\b", "\\b").replace("\f", "\\f").replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t").replace("\"", "\\\"")

def update_records(pid, list_of_fields_to_values, overwrite="overwrite", return_content="count", verbose=False):
  """Updates any fields for a record included in the fields_to_values dictionary.  
    list_of_fields_to_values is a list of dictionaries, each dictionary
    containing the fields and their values for that item in the list.
    Overwrite is turned ON, so blank values will overwrite any value stored in RedCap.  
    The record id MUST be included.  redcap_repeat_instrument and redcap_repeat_instance
    are required for repeat instruments.  All dates must be in Y-M-D format regardless
    of the format definition for the field."""
  buf = cStringIO.StringIO()
  json_data = '[' + ",".join([ '{' + ",".join(["\"%s\":\"%s\"" % (field, escape_str(str(value))) for field, value in fields_to_values.iteritems()]) + '}' for fields_to_values in list_of_fields_to_values ]) + ']'
  if verbose:
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
  curl.setopt(pycurl.SSL_VERIFYPEER, 0)
  curl.setopt(pycurl.SSL_VERIFYHOST, 0)
  curl.perform()
  curl.close()
  body = buf.getvalue()
  buf.close()
  if verbose:
    print "LOG:", body
  return json.load(StringIO(body)) # returns count of updated records e.g. {"count": 1} or list of IDs that were updated

def download_backup(pid, verbose=False):
  """Export Entire Project as REDCap XML File (containing metadata & data).  
    Raises RuntimeError if RESPONSE_CODE is not 200."""
  buf = cStringIO.StringIO()
  # exportFiles = true can make the export very large and may prevent it from completing if the project contains many files or very large files.
  # tested exportFiles by uplading a file called 'test_upload.txt' containing 'testing' and this was included in the downloaded XML
  # <ItemDataBase64Binary ItemOID="test" redcap:DocName="test_upload.txt" redcap:MimeType="text/plain"><![CDATA[dGVzdGluZwo=]]></ItemDataBase64Binary>
  # using a base 63 decoder, 'dGVzdGluZwo=' became 'testing'
  # returnMetadataOnly = 'false' doesn't seem to work, but false is the default, so I just removed it
  data = {
    'token': redcap_config.pids_to_tokens[pid],
    'content': 'project_xml',
    'returnFormat': 'jsoXn',
    'exportSurveyFields': 'true',
    'exportDataAccessGroups': 'true',
    'exportFiles': 'true',
  }
  curl = pycurl.Curl()
  curl.setopt(curl.URL, redcap_config.api_url)
  curl.setopt(curl.HTTPPOST, data.items())
  curl.setopt(curl.WRITEFUNCTION, buf.write)
  curl.setopt(pycurl.SSL_VERIFYPEER, 0)
  curl.setopt(pycurl.SSL_VERIFYHOST, 0)
  curl.perform()
  response_code = curl.getinfo(curl.RESPONSE_CODE)
  curl.close()
  body = buf.getvalue()
  buf.close()
  if response_code != 200:
    raise RuntimeError("Response code '%d' returned expecting '200'.  Error is '%s'" % (response_code, body))
  return body
