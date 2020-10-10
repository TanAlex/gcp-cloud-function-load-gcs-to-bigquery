import re
gs_file_names = ["gs://warm-actor-291222_cloudbuild/test-data/date=2020-11-02/hour=12/min=20/data-20201101-003011.json.gz",
"gs://warm-actor-291222_cloudbuild/test-data/date=2021-11-02/hour=13/min=21/data-20201101-003011.json.gz",
]


def get_partition_sql_from_file_names(file_names):
    pattern = re.compile(r'\/(\w+)=([0-9\-]+)')
    kv = {}
    for file_name in file_names:
      for match in pattern.finditer(file_name):
          if match:
              k = match.group(1)
              v = match.group(2)
              if k in kv:
                  kv[k].append(v)
              else:
                  kv[k] = [v]
    statements = []
    for k in kv:
      # add single quote if v is not a digital number
      v = map(lambda v: f"'{v}'"  if not re.match('^\d+$', v) else v, kv[k])
      statements.append(f"{k} in ( "+ ",".join(v) + " )")
    return " and ".join(statements)

res = get_partition_sql_from_file_names(gs_file_names)
print(res)