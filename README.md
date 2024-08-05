# Documentação da Bovespa Scraping

## Introdução

Esta é uma solução que faz o scraping dos dados consolidados das ações da Bovespa (D-1), faz o ETL através do Glue e exibe os dados no Athena. 

## Autor

- Kirk Patrick (MLET1 - Grupo 66)
- Você pode entrar em contato com o autor pelo LinkedIn: [https://www.linkedin.com/in/kirkgo/](https://www.linkedin.com/in/kirkgo/)


## Requisitos

 - Terraform
 - Python 3.10.9
 - Conta AWS

## Como Rodar a Aplicação

### Passo 1: Clonar o Repositório

Clone o repositório da API na sua máquina local.

    git https://github.com/kirkgo/bovespa-scraping.git
    cd bovespa-scraping.git

### Passo 2: Credenciais de Acesso

Será necessário exportar suas credenciais da AWS para rodar o Terraform (as credenciais abaixo são apenas um exemplo):

- export AWS_DEFAULT_REGION=us-east-1
- export AWS_SECRET_ACCESS_KEY=POAIUDFA2P98AFKAA6YUeq5SX
- export AWS_ACCESS_KEY_ID=SAJQK8I7UY

### Passo 3: Execute o Terraform

Entre no diretório do terraform e execute:

    terraform init
    terraform plan
    terraform apply

Os comandos acima vão iniciar o terraform e fazer o deploy dos seguintes componentes na infraestrutra da AWS:

- S3
- Lambda Function
- Glue Job

### Passo 4: Execute o scraping.py

O scraping.py é o script responsável por fazer o scraping de dados da Bovespa. Depois de efetuado o scraping dos dados ele irá gerar uma arquivo no formato parquet e em seguida fará o upload do arquivo no bucket S3 que foi criado ao rodar o terraform.

### Passo 5: Acesse o Console da AWS

Após executar o scraping.py, você pode acessar o console da AWS e verificar o Job do Glue rodando na lista de jobs para efetuar a transformação dos dados. Após a transformação concluída, você poderá consultar o resultado usando o AWS Athena e conferir o arquivo no S3, dentro de /refined.

### Passo 6: Desinstalar a Solução

Para não haver custos elevados associados à sua conta, após testar a aplicação faça: 

- Exclua os arquivos que estão dentro do bucket
- Exclua o database 'default' do AWS Glue
- Rode o comando: terraform destroy

Com isso tudo o que foi criado será completamente removido da sua conta. 

## Considerações Finais

Este guia fornece as instruções básicas para configurar, rodar e utilizar a aplicação de scraping da Bovespa com o S3, Lambda Function e AWS Glue. Para qualquer dúvida ou problema, consulte a documentação oficial da AWS, Terraform ou entre em contato com o desenvolvedor responsável.


