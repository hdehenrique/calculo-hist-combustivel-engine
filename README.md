# Visão Geral

Este projeto foi desenvolvido como parte de um desafio técnico proposto por uma empresa. O objetivo era processar e analisar uma série histórica de preços de combustíveis do Brasil, referente ao ano de 2019. O arquivo de entrada contém as seguintes colunas:

> DATA INICIAL, DATA FINAL, REGIÃO, ESTADO, MUNICÍPIO, PRODUTO, NÚMERO DE POSTOS PESQUISADOS, UNIDADE DE MEDIDA, PREÇO MÉDIO REVENDA, DESVIO PADRÃO REVENDA, PREÇO MÍNIMO REVENDA, PREÇO MÁXIMO REVENDA, MARGEM MÉDIA REVENDA, COEF DE VARIAÇÃO REVENDA, PREÇO MÉDIO DISTRIBUIÇÃO, DESVIO PADRÃO DISTRIBUIÇÃO, PREÇO MÍNIMO DISTRIBUIÇÃO, PREÇO MÁXIMO DISTRIBUIÇÃO, COEF DE VARIAÇÃO DISTRIBUIÇÃO.

O desafio consistia em, utilizando Apache Spark (Scala ou Python):

- Agrupar os dados semanais por mês e calcular as médias de valores de cada combustível por cidade.
- Calcular a média de valor do combustível por estado e região.
- Calcular a variância e a variação absoluta do máximo e mínimo de cada cidade, mês a mês.
- Identificar as 5 cidades que possuem a maior diferença entre o combustível mais barato e o mais caro.

Este projeto implementa um pipeline de processamento desses dados utilizando Apache Spark com Scala, realizando todas as análises solicitadas.

# Arquitetura

- **Apache Spark**: Plataforma principal para processamento distribuído e análise dos dados.
- **Scala**: Linguagem utilizada para desenvolvimento do pipeline.
- **CSV**: Fonte de dados contendo a série histórica de preços de combustíveis.
- **Maven**: Gerenciador de dependências e build.

# Fluxo do Pipeline

1. Carrega os dados históricos do arquivo CSV.
2. Realiza parsing e validação dos dados.
3. Agrega e calcula médias mensais por cidade.
4. Calcula médias por estado e região.
5. Calcula variância e variação absoluta dos preços.
6. Identifica as cidades com maior diferença de preços.
7. Exibe os resultados no console.

# Boas Práticas Adotadas

- **Organização modular**: Separação clara entre domínio, engine de processamento, input e configuração Spark.
- **Conversão e validação de dados**: Parsing robusto dos dados do CSV para tipos corretos.
- **Reuso de sessão Spark**: Instância única de SparkSession para toda a aplicação.
- **Tratamento de exceções**: Validação e tratamento de dados inconsistentes.
- **Logs e monitoramento**: Nível de log configurável para facilitar troubleshooting.
- **Documentação e comentários**: Código comentado para facilitar manutenção e entendimento.

# Como Executar

1. Instale as dependências:
   ```sh
   mvn clean package
   ```
2. Execute a aplicação:
   ```sh
   spark-submit --class br.com.empresa.apps.batch.CalcHistCombustivel target/CalculadoraCombustivel-*.jar
   ```

# Exemplo de Output

## Médias Mensais por Cidade

| Mês     | Cidade         | Produto      | Preço Médio Revenda |
|---------|----------------|--------------|---------------------|
| 2019-01 | São Paulo      | Gasolina     | 4.25                |
| 2019-01 | Rio de Janeiro | Etanol       | 3.10                |
| ...     | ...            | ...          | ...                 |

## Média por Estado

| Estado | Produto   | Preço Médio Revenda |
|--------|-----------|---------------------|
| SP     | Gasolina  | 4.30                |
| RJ     | Etanol    | 3.15                |
| ...    | ...       | ...                 |

## Média por Região

| Região   | Produto   | Preço Médio Revenda |
|----------|-----------|---------------------|
| Sudeste  | Gasolina  | 4.35                |
| Nordeste | Etanol    | 3.05                |
| ...      | ...       | ...                 |

## Variância e Variação Absoluta dos Preços por Cidade/Mês

| Cidade         | Mês     | Produto   | Variância | Variação Absoluta |
|----------------|---------|-----------|-----------|-------------------|
| São Paulo      | 2019-01 | Gasolina  | 0.02      | 0.30              |
| Rio de Janeiro | 2019-01 | Etanol    | 0.03      | 0.25              |
| ...            | ...     | ...       | ...       | ...               |

## Top 5 Cidades com Maior Diferença de Preço

| Cidade         | Produto   | Diferença Máxima |
|----------------|-----------|------------------|
| São Paulo      | Gasolina  | 0.50             |
| Belo Horizonte | Etanol    | 0.45             |
| ...            | ...       | ...              |

# Estrutura de Pastas

```
src/
├── main/
│   ├── resources/
│   │   └── SEMANAL_MUNICIPIOS-2019.csv   # Dados de entrada
│   └── scala/
│       └── br/com/empresa/
│           ├── apps/batch/               # Ponto de entrada da aplicação
│           ├── domain/                   # Modelos de dados
│           ├── engine/                   # Lógica de processamento
│           ├── input/                    # Carregamento dos dados
│           └── spark/                    # Configuração Spark
```


# Observações

- O projeto foi desenvolvido para análise de dados reais, mas pode ser adaptado para outros cenários de processamento de dados históricos.
- Recomenda-se revisar o arquivo de configuração (`src/main/resources/application.conf`) para ajustar caminhos e parâmetros conforme o ambiente.
- Os resultados são exibidos no console, mas podem ser facilmente exportados para arquivos ou bancos de dados conforme necessidade.