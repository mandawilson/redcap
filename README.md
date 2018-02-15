# redcap
Scripts for interacting with RedCap via API

## backup_redcap_project.py [options] pid > output.xml

Exports the entire REDCap project as an XML file (metadata and data).

If REDCap returns anything but a 200 RESPONSE_CODE an error is written to the standard error stream, and the standard out stream will be empty.

From the REDCap documentation, "The entire project (all records, events, arms, instruments, fields, and project attributes) can be downloaded as a single
XML file, which is in CDISC ODM format (ODM version 1.3.1). This XML file can be used to create a clone of the project (including its data, optionally) on this REDCap server
or on another REDCap server (it can be uploaded on the Create New Project page)."

## upload_csv_to_redcap.py [options] pid input_filename

Read CSV file or stdin made to be loaded into redcap via the import tool and update project using API.

    Options:
        -k, --key key_name Primary key for RedCap study, default is record_id
        -c, --chunk chunk_size Number of records to upload in one POST request, default is 100
        -v, --verbose Turn on verbose logging
    e.g. 
        python upload_csv_to_redcap.py -k project_id -c 10 -v 99 redcap.csv
        python make_an_impact.py | python upload_csv_to_redcap.py -c 100 99

## redcap.py

    import redcap

    redcap.update_records(pid, list_of_fields_to_values, overwrite, return_content, verbose)

Updates any fields for a record included in the list_of_fields_to_values dictionary.  list_of_fields_to_values is a list of dictionaries, each dictionary containing the fields and their values for that item in the list.  Overwrite is turned ON by default, so blank values will overwrite any value stored in RedCap.  The record id MUST be included.  redcap_repeat_instrument and redcap_repeat_instance are required for repeat instruments.  All dates must be in Y-M-D format regardless of the format definition for the field.
