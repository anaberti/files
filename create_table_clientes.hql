CREATE EXTERNAL TABLE IF NOT EXISTS DESAFIO_CURSO.clientes ( 
address_number string,
business_family string,
business_unit string,
customer string,
customer_key string,
customer_type string,
division string,
line_of_business string,
phone string,
region_code string,
regional_sales_mgr string,
search_type string
)
COMMENT 'Tabela de Clientes'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
location '/datalake/raw/clientes/'
TBLPROPERTIES ("skip.header.line.count"="1");
