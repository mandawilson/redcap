# redcap
Scripts for interacting with RedCap via API


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
