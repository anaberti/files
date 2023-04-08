# Cria estrutura de diretorios do desafio
mkdir desafio_curso
cd desafio_curso

#!/bin/bash
PASTAS=("app" "gold" "raw" "run" "scripts")
for i in "${PASTAS[@]}"
do
echo "i"
mkdir $i
done

# Cria demais subdiretorios
#!/bin/bash
cd scripts
mkdir pre_process
mkdir process
mkdir hql
cd hql

#!/bin/bash
TABELAS = ("clientes" "vendas" "divisao" "endereco" "regiao")
for i in "${TABELAS[@]}"
do
curl -O https://raw.githubusercontent.com/anaberti/files/main/create_table_$t.hql
done
cd ..
cd ..

# importa os arquivos .csv
DADOS=("clientes" "vendas" "divisao" "endereco" "regiao")
#!bin/bash
cd raw
for i in "${DADOS[@]}"
do
echo "$i"
mkdir $i
cd $i
curl -O https://raw.githubusercontent.com/anaberti/files/main/$i.csv
cd ..
done

# Transferência arquivos para hdfs
cd raw
DADOS=("CLIENTES" "VENDAS" "DIVISAO" "ENDERECO" "REGIAO")
for i in "${DADOS[@]}"
do
echo "$i"
cd $i
hdfs dfs -mkdir -p /datalake/raw/$i
hdfs dfs -copyFromLocal $i.csv /datalake/raw/$i
cd ..
done