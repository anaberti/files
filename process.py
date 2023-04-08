
# coding: utf-8

# In[ ]:


from pyspark.sql import SparkSession, dataframe, Row
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql import HiveContext
from pyspark.sql.functions import *

from pyspark.sql import functions as f
import os
import re

from pyspark.sql.window import Window

from pyspark import SparkContext, SparkConf


# In[ ]:


spark = SparkSession.builder.master("local[*]")    .enableHiveSupport()    .getOrCreate()


# In[ ]:


conf = SparkConf().setAppName("projectName").setMaster("local[*]")
sc = SparkContext.getOrCreate(conf)


# In[ ]:


def salvar_df(df, file):
    output = "/input/projeto_hive/gold/" + file
    erase = "hdfs dfs -rm " + output + "/*"
    rename = "hdfs dfs -get /datalake/gold/"+file+"/part-* /input/desafio_curso/gold/"+file+".csv"
    print(rename)

    df.coalesce(1).write.format("csv").option("header", True).option("delimiter", ";").mode("overwrite").save("/desafio_curso/gold/"+file+"/")

    os.system(erase)
    os.system(rename)


# In[ ]:


# Criando dataframes diretamente do Hive
df_clientes = spark.sql("SELECT * FROM desafio_curso.tbl_clientes")
df_divisao = spark.sql("SELECT * FROM desafio_curso.tbl_divisao")
df_regiao = spark.sql("SELECT * FROM desafio_curso.tbl_regiao")
df_vendas = spark.sql("SELECT * FROM desafio_curso.tbl_vendas")
df_endereco = spark.sql("SELECT * FROM desafio_curso.tbl_endereco")


# In[ ]:


# Criando a stage


# In[ ]:


df1 = df_clientes.join(df_vendas, df_clientes.customer_key == df_vendas.customer_key, "inner").drop(df_vendas.customer_key)


# In[ ]:


df2 = df1.join(df_divisao,df1.division == df_divisao.division,"left").drop(df1.division)


# In[ ]:


df3 = df2.join(df_regiao,df2.region_code == df_regiao.region_code,"left").drop(df2.region_code)


# In[ ]:


df_stage = df3.join(df_endereco, df3.address_number == df_endereco.address_number, "left").drop(df3.address_number)


# In[ ]:


#Tratar dados


# In[ ]:


#Passos utilizados: 
#1. Uso da função "trim()" para limpar espaços vazios antes e após as informações das células;
#2. Preencher as células vazias e substituir campos preenchidos com "null" com a função 
# "when(coluna + condição, resultado).otherwise(coluna)"
#3. Substituir ponto por vírgula nos campos com decimais

# COLUNA BUSINESS_FAMILY
df_stage = df_stage.withColumn("business_family", trim(col("business_family")))
df_stage = df_stage.withColumn("business_family", when(col("business_family")=="", "Não informado").when(col("business_family")=="null", "Não informado").otherwise(df_stage.business_family))

#COLUNA BUSINESS_UNIT
df_stage = df_stage.withColumn("business_unit", trim(col("business_unit")))
df_stage = df_stage.withColumn("business_unit", when(col("business_unit")=="", "0").when(col("business_unit")=="null", "0").otherwise(df_stage.business_unit))
df_stage = df_stage.withColumn("business_unit", translate("business_unit", ".", ","))

#COLUNA CUSTOMER
df_stage = df_stage.withColumn("customer", trim(col("customer")))
df_stage = df_stage.withColumn("customer", when(col("customer")=="", "Não informado").when(col("customer")=="null", "Não informado").otherwise(df_stage.customer))

#COLUNA CUSTOMER_KEY
df_stage = df_stage.withColumn("customer_key", trim(col("customer_key")))
df_stage = df_stage.withColumn("customer_key", when(col("customer_key")=="", "0").when(col("customer_key")=="null", "0").otherwise(df_stage.customer_key))
df_stage = df_stage.withColumn("customer_key", translate("customer_key", ".", ","))

#COLUNA CUSTOMER_TYPE
df_stage = df_stage.withColumn("customer_type", trim(col("customer_type")))
df_stage = df_stage.withColumn("customer_type", when(col("customer_type")=="", "Não informado").when(col("customer_type")=="null", "Não informado").otherwise(df_stage.customer_type))

#COLUNA LINE_OF_BUSINESS
df_stage = df_stage.withColumn("line_of_business", trim(col("line_of_business")))
df_stage = df_stage.withColumn("line_of_business", when(col("line_of_business")=="", "Não informado").when(col("line_of_business")=="null", "Não informado").otherwise(df_stage.line_of_business))

#COLUNA PHONE
df_stage = df_stage.withColumn("phone", trim(col("phone")))
df_stage = df_stage.withColumn("phone", when(col("phone")=="", "Não informado").when(col("phone")=="null", "Não informado").otherwise(df_stage.phone))

#COLUNA REGIONAL_SALES_MGR
df_stage = df_stage.withColumn("regional_sales_mgr", trim(col("regional_sales_mgr")))
df_stage = df_stage.withColumn("regional_sales_mgr", when(col("regional_sales_mgr")=="", "Não informado").when(col("regional_sales_mgr")=="null", "Não informado").otherwise(df_stage.regional_sales_mgr))

#COLUNA SEARCH_TYPE
df_stage = df_stage.withColumn("search_type", trim(col("search_type")))
df_stage = df_stage.withColumn("search_type", when(col("search_type")=="", "Não informado").when(col("search_type")=="null", "Não informado").otherwise(df_stage.search_type))

#COLUNA ACTUAL_DELIVERY_DATE
df_stage = df_stage.withColumn("actual_delivery_date", trim(col("actual_delivery_date")))
df_stage = df_stage.withColumn("actual_delivery_date", when(col("actual_delivery_date")=="", "Não informado").when(col("actual_delivery_date")=="null", "Não informado").otherwise(df_stage.actual_delivery_date))

#COLUNA DATE_KEY
df_stage = df_stage.withColumn("date_key", trim(col("date_key")))
df_stage = df_stage.withColumn("date_key", when(col("date_key")=="", "Não informado").when(col("date_key")=="null", "Não informado").otherwise(df_stage.date_key))

#COLUNA DISCOUNT_AMOUNT
df_stage = df_stage.withColumn("discount_amount", trim(col("discount_amount")))
df_stage = df_stage.withColumn("discount_amount", when(col("discount_amount")=="", "0").when(col("discount_amount")=="null", "0").otherwise(df_stage.discount_amount))
df_stage = df_stage.withColumn("discount_amount", translate("discount_amount", ".", ","))

#COLUNA INVOICE_DATE
df_stage = df_stage.withColumn("invoice_date", trim(col("invoice_date")))
df_stage = df_stage.withColumn("invoice_date", when(col("invoice_date")=="", "Não informado").when(col("invoice_date")=="null", "Não informado").otherwise(df_stage.invoice_date))

#COLUNA INVOICE_NUMBER
df_stage = df_stage.withColumn("invoice_number", trim(col("invoice_number")))
df_stage = df_stage.withColumn("invoice_number", when(col("invoice_number")=="", "0").when(col("invoice_number")=="null", "0").otherwise(df_stage.invoice_number))
df_stage = df_stage.withColumn("invoice_number", translate("invoice_number", ".", ","))

#COLUNA ITEM_CLASS
df_stage = df_stage.withColumn("item_class", trim(col("item_class")))
df_stage = df_stage.withColumn("item_class", when(col("item_class")=="", "Não informado").when(col("item_class")=="null", "Não informado").otherwise(df_stage.item_class))

#COLUNA ITEM_NUMBER
df_stage = df_stage.withColumn("item_number", trim(col("item_number")))
df_stage = df_stage.withColumn("item_number", when(col("item_number")=="", "0").when(col("item_number")=="null", "0").otherwise(df_stage.item_number))
df_stage = df_stage.withColumn("item_number", translate("item_number", ".", ","))

#COLUNA ITEM
df_stage = df_stage.withColumn("item", trim(col("item")))
df_stage = df_stage.withColumn("item", when(col("item")=="", "Não informado").when(col("item")=="null", "Não informado").otherwise(df_stage.item))

#COLUNA LINE_NUMBER
df_stage = df_stage.withColumn("line_number", trim(col("line_number")))
df_stage = df_stage.withColumn("line_number", when(col("line_number")=="", "0").when(col("line_number")=="null", "0").otherwise(df_stage.line_number))
df_stage = df_stage.withColumn("line_number", translate("line_number", ".", ","))

#COLUNA LIST_PRICE
df_stage = df_stage.withColumn("list_price", trim(col("list_price")))
df_stage = df_stage.withColumn("list_price", when(col("list_price")=="", "0").when(col("list_price")=="null", "0").otherwise(df_stage.list_price))
df_stage = df_stage.withColumn("list_price", translate("list_price", ".", ","))

#COLUNA ORDER_NUMBER
df_stage = df_stage.withColumn("order_number", trim(col("order_number")))
df_stage = df_stage.withColumn("order_number", when(col("order_number")=="", "0").when(col("order_number")=="null", "0").otherwise(df_stage.order_number))
df_stage = df_stage.withColumn("order_number", translate("order_number", ".", ","))

#COLUNA PROMISED_DELIVERY_DATE
df_stage = df_stage.withColumn("promised_delivery_date", trim(col("promised_delivery_date")))
df_stage = df_stage.withColumn("promised_delivery_date", when(col("promised_delivery_date")=="", "Não informado").when(col("promised_delivery_date")=="null", "Não informado").otherwise(df_stage.promised_delivery_date))

#COLUNA SALES_AMOUNT
df_stage = df_stage.withColumn("sales_amount", trim(col("sales_amount")))
df_stage = df_stage.withColumn("sales_amount", when(col("sales_amount")=="", "0").when(col("sales_amount")=="null", "0").otherwise(df_stage.sales_amount))
df_stage = df_stage.withColumn("sales_amount", translate("sales_amount", ".", ","))

#COLUNA SALES_AMOUNT_BASED_ON_LIST_PRICE
df_stage = df_stage.withColumn("sales_amount_based_on_list_price", trim(col("sales_amount_based_on_list_price")))
df_stage = df_stage.withColumn("sales_amount_based_on_list_price", when(col("sales_amount_based_on_list_price")=="", "0").when(col("sales_amount_based_on_list_price")=="null", "0").otherwise(df_stage.sales_amount_based_on_list_price))
df_stage = df_stage.withColumn("sales_amount_based_on_list_price", translate("sales_amount_based_on_list_price", ".", ","))

#COLUNA SALES_COST_AMOUNT
df_stage = df_stage.withColumn("sales_cost_amount", trim(col("sales_cost_amount")))
df_stage = df_stage.withColumn("sales_cost_amount", when(col("sales_cost_amount")=="", "0").when(col("sales_cost_amount")=="null", "0").otherwise(df_stage.sales_cost_amount))
df_stage = df_stage.withColumn("sales_cost_amount", translate("sales_cost_amount", ".", ","))

#COLUNA SALES_MARGIN_AMOUNT
df_stage = df_stage.withColumn("sales_margin_amount", trim(col("sales_margin_amount")))
df_stage = df_stage.withColumn("sales_margin_amount", when(col("sales_margin_amount")=="", "0").when(col("sales_margin_amount")=="null", "0").otherwise(df_stage.sales_margin_amount))
df_stage = df_stage.withColumn("sales_margin_amount", translate("sales_margin_amount", ".", ","))

#COLUNA SALES_PRICE
df_stage = df_stage.withColumn("sales_price", trim(col("sales_price")))
df_stage = df_stage.withColumn("sales_price", when(col("sales_price")=="", "0").when(col("sales_price")=="null", "0").otherwise(df_stage.sales_price))
df_stage = df_stage.withColumn("sales_price", translate("sales_price", ".", ","))

#COLUNA SALES_QUANTITY
df_stage = df_stage.withColumn("sales_quantity", trim(col("sales_quantity")))
df_stage = df_stage.withColumn("sales_quantity", when(col("sales_quantity")=="", "0").when(col("sales_quantity")=="null", "0").otherwise(df_stage.sales_quantity))
df_stage = df_stage.withColumn("sales_quantity", translate("sales_quantity", ".", ","))

#COLUNA SALES_REP
df_stage = df_stage.withColumn("sales_rep", trim(col("sales_rep")))
df_stage = df_stage.withColumn("sales_rep", when(col("sales_rep")=="", "Não informado").when(col("sales_rep")=="null", "Não informado").otherwise(df_stage.sales_rep))

#COLUNA U_M
df_stage = df_stage.withColumn("u_m", trim(col("u_m")))
df_stage = df_stage.withColumn("u_m", when(col("u_m")=="", "0").when(col("u_m")=="null", "0").otherwise(df_stage.u_m))
df_stage = df_stage.withColumn("u_m", translate("u_m", ".", ","))

#COLUNA DIVISION
df_stage = df_stage.withColumn("division", trim(col("division")))
df_stage = df_stage.withColumn("division", when(col("division")=="", "0").when(col("division")=="null", "0").otherwise(df_stage.division))
df_stage = df_stage.withColumn("division", translate("division", ".", ","))

#COLUNA DIVISION_NAME
df_stage = df_stage.withColumn("division_name", trim(col("division_name")))
df_stage = df_stage.withColumn("division_name", when(col("division_name")=="", "Não informado").when(col("division_name")=="null", "Não informado").otherwise(df_stage.division_name))

#COLUNA REGION_CODE
df_stage = df_stage.withColumn("region_code", trim(col("region_code")))
df_stage = df_stage.withColumn("region_code", when(col("region_code")=="", "0").when(col("region_code")=="null", "0").otherwise(df_stage.region_code))
df_stage = df_stage.withColumn("region_code", translate("region_code", ".", ","))

#COLUNA REGION_NAME
df_stage = df_stage.withColumn("region_name", trim(col("region_name")))
df_stage = df_stage.withColumn("region_name", when(col("region_name")=="", "Não informado").when(col("region_name")=="null", "Não informado").otherwise(df_stage.region_name))

#COLUNA ADDRESS_NUMBER
df_stage = df_stage.withColumn("address_number", trim(col("address_number")))
df_stage = df_stage.withColumn("address_number", when(col("address_number")=="", "0").when(col("address_number")=="null", "0").otherwise(df_stage.address_number))
df_stage = df_stage.withColumn("address_number", translate("address_number", ".", ","))

#COLUNA CITY
df_stage = df_stage.withColumn("city", trim(col("city")))
df_stage = df_stage.withColumn("city", when(col("city")=="", "Não informado").when(col("city")=="null", "Não informado").otherwise(df_stage.city))

#COLUNA COUNTRY
df_stage = df_stage.withColumn("country", trim(col("country")))
df_stage = df_stage.withColumn("country", when(col("country")=="", "Não informado").when(col("country")=="null", "Não informado").otherwise(df_stage.country))

#COLUNA CUSTOMER_ADDRESS_1
df_stage = df_stage.withColumn("customer_address_1", trim(col("customer_address_1")))
df_stage = df_stage.withColumn("customer_address_1", when(col("customer_address_1")=="", "Não informado").when(col("customer_address_1")=="null", "Não informado").otherwise(df_stage.customer_address_1))

#COLUNA CUSTOMER_ADDRESS_2
df_stage = df_stage.withColumn("customer_address_2", trim(col("customer_address_2")))
df_stage = df_stage.withColumn("customer_address_2", when(col("customer_address_2")=="", "Não informado").when(col("customer_address_2")=="null", "Não informado").otherwise(df_stage.customer_address_2))

#COLUNA CUSTOMER_ADDRESS_3
df_stage = df_stage.withColumn("customer_address_3", trim(col("customer_address_3")))
df_stage = df_stage.withColumn("customer_address_3", when(col("customer_address_3")=="", "Não informado").when(col("customer_address_3")=="null", "Não informado").otherwise(df_stage.customer_address_3))

#COLUNA CUSTOMER_ADDRESS_4
df_stage = df_stage.withColumn("customer_address_4", trim(col("customer_address_4")))
df_stage = df_stage.withColumn("customer_address_4", when(col("customer_address_4")=="", "Não informado").when(col("customer_address_4")=="null", "Não informado").otherwise(df_stage.customer_address_4))

#COLUNA STATE
df_stage = df_stage.withColumn("state", trim(col("state")))
df_stage = df_stage.withColumn("state", when(col("state")=="", "Não informado").when(col("state")=="null", "Não informado").otherwise(df_stage.state))

#COLUNA ZIP_CODE
df_stage = df_stage.withColumn("zip_code", trim(col("zip_code")))
df_stage = df_stage.withColumn("zip_code", when(col("zip_code")=="", "Não informado").when(col("zip_code")=="null", "Não informado").otherwise(df_stage.zip_code))


# In[ ]:


#Incluir colunas com ano, mês, dia e trimestre na stage


# In[ ]:


df_stage = df_stage.withColumn('invoice_year', split(df_stage.invoice_date, '/').getItem(2))


# In[ ]:


df_stage = df_stage.withColumn('invoice_month', split(df_stage.invoice_date, '/').getItem(1))


# In[ ]:


df_stage = df_stage.withColumn('invoice_day', split(df_stage.invoice_date, '/').getItem(0))


# In[ ]:


df_stage = df_stage.withColumn("quarter", expr("case when invoice_month == '01' then '1' " + "when invoice_month == '02' then '1' " + "when invoice_month == '03' then '1' " + "when invoice_month == '04' then '2' " + "when invoice_month == '05' then '2' " + "when invoice_month == '06' then '2' " + "when invoice_month == '07' then '3' " + "when invoice_month == '08' then '3' " + "when invoice_month == '09' then '3' " + "when invoice_month == '10' then '4' " + "when invoice_month == '11' then '4' " + "when invoice_month == '12' then '4' " + "else 'Unknown' end"))


# In[ ]:


df_stage.show(10)


# In[ ]:


salvar_df(df_stage, 'stage')


# In[ ]:


#Criar fato e dimensões


# In[ ]:


dim_local = df_stage.select("city", "country", "state", "region_code", "region_name").distinct()


# In[ ]:


dim_local.show(5)


# In[ ]:


fato_vendas = df_stage.select("order_number", "invoice_number", "customer_key", "sales_rep", "item_number", "item", "item_class", "line_number", "list_price", "date_key", "promised_delivery_date", "actual_delivery_date", "region_code", "sales_quantity", "sales_price", "discount_amount", "sales_amount", "sales_amount_based_on_list_price", "sales_cost_amount").distinct()


# In[ ]:


fato_vendas.show(5)


# In[ ]:


dim_tempo = df_stage.select("date_key", "invoice_date", "invoice_day", "invoice_month", "invoice_year", "quarter").distinct()


# In[ ]:


dim_tempo.show(5)


# In[ ]:


dim_cliente = df_stage.select("customer_key", "customer", "customer_type", "phone", "address_number", "customer_address_1", "customer_address_2", "customer_address_3", "customer_address_4", "division", "division_name", "line_of_business", "business_family", "business_unit", "regional_sales_mgr", "search_type", "zip_code").distinct()


# In[ ]:


dim_cliente.show(5)


# In[ ]:


#Salvando os arquivos da modelagem


# In[ ]:


salvar_df(fato_vendas, 'fato_vendas')


# In[ ]:


salvar_df(dim_local, 'dim_local')


# In[ ]:


salvar_df(dim_cliente, 'dim_cliente')


# In[ ]:


salvar_df(dim_tempo, 'dim_tempo')


# In[ ]:


# Testes para comparar com o Power BI


# In[ ]:


fato_vendas.select('invoice_number').distinct().count()


# In[ ]:


fato_vendas.select('item_number').count()


# In[ ]:


fato_vendas.groupby('customer_key').count().show(10)


# In[ ]:


fato_vendas.groupby('region_code').count().show()


# In[ ]:


ranking_sales_rep = fato_vendas.groupby('sales_rep').agg(count('sales_amount').alias('faturamento'))


# In[ ]:


ranking_sales_rep.orderBy(desc('faturamento')).show(10)

