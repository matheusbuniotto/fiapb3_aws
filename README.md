Pipeline de dados completo para extrair, processar e analisar dados do pregão da B3, utilizando AWS S3, Glue, Lambda e Athena. 

![fluxo](https://github.com/matheusbuniotto/fiapb3_aws/blob/main/diagrama.jpg)

Itens desenvolvidos durante o projeto no AWS Lab:

• Requisito 1: scrap de dados do site da B3 com dados do pregão.

• Requisito 2: os dados brutos devem ser ingeridos no s3 em formato parquet com partição diária. 

• Requisito 3: o bucket aciona uma [**lambda**](https://github.com/matheusbuniotto/fiapb3_aws/blob/main/lambda_function.py), que por sua vez irá chamar o job de ETL no glue. 

• Requisito 4: o [**job Glue**](https://github.com/matheusbuniotto/fiapb3_aws/blob/main/ETL%20B3.json) realiza:

    A: agrupamento numérico, sumarização, contagem ou soma.  
    B: renomear duas colunas existentes além das de agrupamento.  
    C: realizar um cálculo com campos de data, exemplo, poder ser du
    ração, comparação, diferença entre datas. 
![glue](https://github.com/matheusbuniotto/fiapb3_aws/blob/main/glue_task_dag_vertical.png)
• Requisito 6: os dados refinados no job glue são salvos no 
formato parquet em uma pasta chamada /refined no S3, particionado por data 
e pela abreviação da ação do pregão.  

• Requisito 7: o job Glue automaticamente cataloga o dado no 
Glue Catalog e criar uma tabela no banco de dados default do Glue 
Catalog. 

• Requisito 8: os dados estão disponíveis e legíveis no Athena. 
