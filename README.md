# 🚀 Desafio Técnico - Gaudium

## 📁 Estrutura dos dados

Arquivo de entrada: `Dados brutos.csv`
| Coluna       | Tipo |
|--------------|------|
| nome_cliente | str  |
| cidade       | str  |
| estado       | str  |
| nome_produto | str  |
| categoria    | str  |
| fabricante   | str  |
| data         | date |
| qtd_vendida  | int  |
| valor_total  | int  |

A configuração ``inferSchema=True`` do método de leitura de arquivo PySpark foi capaz de capturar o schema de forma correta

## 📋 Identificação de entidades e fatos

A partir de uma análise inicial, foi possível identificar as dimensões/contexto de clientes, produtos e data e os fatos, relacionados as medidas de quantidade vendida e valor total

As tabelas de dimensões criadas foram:

### Cliente

A tabela de cliente utiliza o nome, cidade e estado para identificar um cliente único e o id gerado para essa tabela é um hash obtido através da combinação desses três valores

| Coluna       | Tipo |
|--------------|------|
| id_cliente   | int  |
| nome_cliente | str  |
| cidade       | str  |
| estado       | str  |

### Produtos

A tabela de produtos utiliza o nome e fabricante para identificar um produto único e o id gerado para essa tabela é um hash obtido através da combinação desses dois valores

| Coluna       | Tipo |
|--------------|------|
| id_produtos   | int  |
| nome_produto | str  |
| categoria       | str  |
| fabricante       | str  |

### Datas

A tabela de dias é gerado referenciando a data da primeira compra registrada e a data da última compra registrada como bases para gerar o intervalo completo de dias nesse intervalo. Além disso, a tabela de datas contém informações como o dia, mês, ano e dia da semana para possível utilização por parte da equipe de análise ou da equipe de ciência de dados

| Coluna       | Tipo |
|--------------|------|
| data   | date  |
| dia | int  |
| mes       | int  |
|    ano    | int  |
|    dia_da_semana    | int  |

### Vendas fato

A tabela de vendas fato é a tabela central, no qual o contexto dessas dimensões são aplicadas. 
A granularidade dela é por cliente, dia e produto e contém as informações de quantidade vendida e total. Ela foi gerada usando o arquivo bruto como base.

| Coluna       | Tipo |
|--------------|------|
| data   | date  |
| id_cliente | int  |
| id_produto       | int  |
|    qtde_vendida    | int  |
|    valor_total    | int  |

## 🏗 Diagrama do modelo e das etapas de transformação dos dados

O diagrama é uma representação simplificada de como os dados estão sendo transformados. Ela não segue uma estrutura usual de diagramação de ETL, visto que o intuito é simplismente ilustrar de forma clara o processo específico para o desenvolvimento desse projeto particular.

![image](imgs/diagram.jpg)

## ▶️ Como executar o código

Código principal

Pré-requisito para executar o código
- Suba os arquivos main.py e config.toml para um ambiente com suporte ao pyspark
- Suba o arquivo .csv. Por padrão o script espera que o arquivo esteja em um diretório data/raw/ no mesmo local que o main, porém, isso pode ser alterado no arquivo de configuração (config.toml)
- Execute o código main.py
- Os arquivos serão gerados dentro do diretórios determinados pelo arquivo de configuração (por padrão, data/processed/)

---
Código notebook

Versão utilizada para testagem individual de cada base de dados e pode ser utilizado para investigar cada processo individualemente

- Suba o notebook e o arquivo .csv em um ambiente com suporte ao pyspark

## 🛠 Tecnologias utilizadas

- PySpark
- Spark SQL
- Microsoft Fabric (simulado via notebooks)
- Git e GitHub
- Figma (Para geração dos diagramas)

## 📂 Arquivos no repositório

A estrutura de arquivos do repositório

- `notebooks/modelagem.ipynb`
    - Notebook que pode ser utilizado para gerar os dados, porém, mais recomendado para testar funções e transformações específicas para algum dos arquivos
- `main.py`
    - Script principal
- `config.toml`
    - Arquivo de configuração para execução do script `main.py`
    - A partir desse arquivo, pode ser alterado o caminho dos arquivos de entrada e saída de cada tabela, quais arquivos devem ser consolidados e também quais colunas serão utilizadas como identificadores únicos
- `requirements_dev.txt`
    - Arquivo com os pacotes utilizados e suas versões

#### Arquivos gerados
Os arquivos de saída foram geradas de forma a simular um processo de ETL.

```
data
├───processed
│   └── cliente
│       ├── clientes_YYYYmmmdd.csv
│       └── clientes.csv
│   └── datas
│       ├── datas_YYYYmmmdd.csv
│       └── datas.csv
│   └── produtos
│       ├── produtos_YYYYmmmdd.csv
│       └── produtos.csv
│   └── vendas_fato.csv
│       ├── vendas_fato_YYYYmmmdd.csv
│       └── vendas_fato.csv
└───raw
    └─── Dados brutos.csv
```

Observação:
- São gerados dois arquivos para cada grupo de dados, uma versão que sempre será a mais recente e outra contendo a informação da data de geração, que pode ser utilizado para versionamento.
 

## ✅ Observações
