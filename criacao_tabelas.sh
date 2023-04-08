
#!/bin/bash

# Cria e usa database
beeline -u jdbc:hive2://localhost:10000 -e 'create database if not exists DESAFIO_CURSO;'

# Criacao tabelas
#!/bin/bash
DADOS=("clientes" "vendas" "divisao" "endereco" "regiao" "vendas")
for i in "${DADOS[@]}"
do
beeline -u jdbc:hive2://localhost:10000/DESAFIO_CURSO -f ../../input/desafio_curso/scripts/hql/create_table_$i.hql
done
