import json

def json_traverser(doc, level=''):
    """Function to help traverse a dict with a JSON BigQuery schema definition (more info at: https://cloud.google.com/bigquery/docs/schemas).
    It is a generator function that return one column at a time and, if a record is detected,
    will traverse it just like the root records.
    The return is a tuple with the column definition and a level, indicating where in the nest this column is.
    :param doc: the table definition (a list of column definitions)
    :type doc: list|dict
    :param level: indicates the level that the column is at (e.g. "" for root, "RECORD' for 1st level, "RECORD.SUB" for 2nd level, ...)
    :type level: str
    """
    for item in doc:
        if 'fields' in item:
            for subitem in json_traverser(item['fields'], level='{}.{}'.format(level, item['name'])):
                yield subitem
        else:
            # exclude the first dot. There should be no problem even if the it's the root level
            yield (item, level[1:])

if __name__ == "__main__":
    data = json.loads(
    """[{"type":"STRING","name":"ngram","mode":"NULLABLE"},{"type":"STRING","name":"first","mode":"NULLABLE"},
    {"type":"STRING","name":"second","mode":"NULLABLE"},{"type":"STRING","name":"third","mode":"NULLABLE"},
    {"type":"STRING","name":"fourth","mode":"NULLABLE"},{"type":"STRING","name":"fifth","mode":"NULLABLE"},
    {"fields":[{"type":"STRING","name":"value","mode":"REPEATED"},{"type":"INTEGER","name":"volume_count","mode":"NULLABLE"},
    {"type":"FLOAT","name":"volume_fraction","mode":"NULLABLE"},{"type":"INTEGER","name":"page_count","mode":"NULLABLE"},
    {"type":"INTEGER","name":"match_count","mode":"NULLABLE"},{"fields":[{"type":"STRING","name":"id","mode":"NULLABLE"},
    {"type":"STRING","name":"text","mode":"NULLABLE"},{"type":"STRING","name":"title","mode":"NULLABLE"},
    {"type":"STRING","name":"subtitle","mode":"NULLABLE"},{"type":"STRING","name":"authors","mode":"NULLABLE"},
    {"type":"STRING","name":"url","mode":"NULLABLE"}],"type":"RECORD","name":"sample","mode":"REPEATED"}],
    "type":"RECORD","name":"cell","mode":"REPEATED"},{"fields": [{"type":"TIMESTAMP","name":"DataAtualizacao","mode":"REQUIRED"},
    {"type":"BYTES","name":"RowHash","mode":"REQUIRED"},{"type":"BYTES","name":"LastRowHash","mode":"NULLABLE"},
    {"type":"BOOLEAN","name":"FlagDelete","mode":"REQUIRED"},{"type":"BYTES","name":"OriginRowHash","mode":"NULLABLE"}],
    "type":"RECORD","name":"METADATA","mode":"NULLABLE"}]"""
    )

    ignore = ['ngram', 'METADATA', 'cell.sample.url']

    for f, l in json_traverser(data):
        if(l in ignore or (f['name'] if l == '' else '{}.{}'.format(l, f['name'])) in ignore):
            print('pass')
        else:
            print('level={}, f={}'.format(l, f))