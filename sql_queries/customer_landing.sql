CREATE EXTERNAL TABLE IF NOT EXISTS customer_landing (
  customername STRING,
  email STRING,
  phone STRING,
  birthday STRING,
  serialnumber STRING,
  registrationdate BIGINT,
  lastupdatedate BIGINT,
  sharewithresearchasofdate BIGINT,
  shareWithpublicasofdate BIGINT,
  sharewithfriendsasofdate BIGINT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://stedi-lakehouse-ps-123/customer_landing/customer-1691348231425.json'
TBLPROPERTIES ('classification'='json');
