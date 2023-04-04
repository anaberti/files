CREATE EXTERNAL TABLE IF NOT EXISTS desafiofinal.divisao ( 
division string,
division_name string
)
COMMENT 'Tabela de Divisao'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
location '/datalake/raw/divisao/'
TBLPROPERTIES ("skip.header.line.count"="1");