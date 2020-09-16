# COS838-Topicos-Especiais-em-Banco-de-Dados-II

## Requisitos
- Sistema operacional Linux

## Como executar
Executar dentro da pasta do projeto

    sbt clean package
    
Caso não tenha o SBT instalado, executar

    echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | sudo apt-key add
    sudo apt-get update
    sudo apt-get install sbt

ou seguir as instruções na documentação oficial em [Installing sbt on Linux](https://www.scala-sbt.org/1.x/docs/Installing-sbt-on-Linux.html)

Para executar um projeto em um cluster da Google Cloud, seguir documentação oficial em [Use the Cloud Storage connector with Apache Spark](https://cloud.google.com/dataproc/docs/tutorials/gcs-connector-spark-tutorial)
