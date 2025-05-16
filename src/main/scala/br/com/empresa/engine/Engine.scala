package br.com.empresa.engine

import br.com.empresa.domain.{DetalhesCombustivel, DetalhesCombustivel2, DetalhesCombustivel3, DetalhesCombustivelAverage, DetalhesCombustivelDifference, DetalhesCombustivelVariance, DetalhesCombustivelVariation}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ColumnName, DataFrame, Dataset, SparkSession}

// Classe responsável por toda a lógica de processamento e agregação dos dados de combustível
class Engine (ss: SparkSession) {
  import ss.implicits._

  // Renomeia as colunas do DataFrame para nomes compatíveis com as case classes do domínio
  def formatToDadosCombustivel(df: DataFrame) = {
    df.withColumnRenamed("DATA INICIAL", "dataInicial")
      .withColumnRenamed("DATA FINAL", "dataFinal")
      .withColumnRenamed("REGIÃO", "regiao")
      .withColumnRenamed("ESTADO", "estado")
      .withColumnRenamed("MUNICÍPIO", "municipio")
      .withColumnRenamed("REGIÃO", "regiao")
      .withColumnRenamed("ESTADO", "estado")
      .withColumnRenamed("MUNICÍPIO", "municipio")
      .withColumnRenamed("PRODUTO", "produto")
      .withColumnRenamed("NÚMERO DE POSTOS PESQUISADOS", "numeroDePostosPesquisados")
      .withColumnRenamed("UNIDADE DE MEDIDA", "unidadeDeMedida")
      .withColumnRenamed("PREÇO MÉDIO REVENDA", "precoMedioRevenda")
      .withColumnRenamed("DESVIO PADRÃO REVENDA", "desvioPadraoRevenda")
      .withColumnRenamed("PREÇO MÍNIMO REVENDA", "precoMinimoRevenda")
      .withColumnRenamed("PREÇO MÁXIMO REVENDA", "precoMaximoRevenda")
      .withColumnRenamed("MARGEM MÉDIA REVENDA", "margemMediaRevenda")
      .withColumnRenamed("COEF DE VARIAÇÃO REVENDA", "coefDeVariaçãoRevenda")
      .withColumnRenamed("PREÇO MÉDIO DISTRIBUIÇÃO", "precoMedioDistribuicao")
      .withColumnRenamed("DESVIO PADRÃO DISTRIBUIÇÃO", "desvioPadraoDistribuicao")
      .withColumnRenamed("PREÇO MÉDIO REVENDA", "precoMedioRevenda")
      .withColumnRenamed("PREÇO MÍNIMO DISTRIBUIÇÃO", "precoMinimoDistribuicao")
      .withColumnRenamed("PREÇO MÁXIMO DISTRIBUIÇÃO", "precoMaximoDistribuicao")
      .withColumnRenamed("COEF DE VARIAÇÃO DISTRIBUIÇÃO", "coefDeVariaçãoDistribuicao")
  }

  // Converte Dataset de DadosCombustivel3 (strings) para DadosCombustivel2 (tipos corretos)
  def parseToDadosCombustivel(ds: Dataset[DetalhesCombustivel3]): Dataset[DetalhesCombustivel2] = {
    ds.map(p => DetalhesCombustivel.parseTo(p))
  }

  // Calcula médias agrupadas por coluna informada (estado, região, etc)
  def calculateAverage(df: Dataset[DetalhesCombustivel2], columnName: String): DataFrame = {
    df.groupBy("produto", columnName)
      .agg(avg("precoMedioRevenda"),
        avg("precoMinimoRevenda"),
        avg("precoMaximoRevenda"),
        avg("margemMediaRevenda"),
        avg("precoMedioDistribuicao"),
        avg("precoMinimoDistribuicao"),
        avg("precoMaximoDistribuicao")
      )
  }

  // Calcula médias agrupadas por mês, produto e coluna informada
  def calculateAverageByMonthAndProduct(df: Dataset[DetalhesCombustivel2], columnName: String): DataFrame = {
    df.groupBy("monthYear", "produto", columnName)
      .agg(avg("precoMedioRevenda"),
        avg("precoMinimoRevenda"),
        avg("precoMaximoRevenda"),
        avg("margemMediaRevenda"),
        avg("precoMedioDistribuicao"),
        avg("precoMinimoDistribuicao"),
        avg("precoMaximoDistribuicao")
      )
  }

  // Calcula médias agrupadas por mês e coluna informada (ex: município)
  def calculateAverageByMonth(df: Dataset[DetalhesCombustivel2], columnName: String): DataFrame = {
    df.groupBy("monthYear", columnName)
      .agg(avg("precoMedioRevenda"),
        avg("precoMinimoRevenda"),
        avg("precoMaximoRevenda"),
        avg("margemMediaRevenda"),
        avg("precoMedioDistribuicao"),
        avg("precoMinimoDistribuicao"),
        avg("precoMaximoDistribuicao")
      )
      .withColumnRenamed("monthYear", "monthYearAverage")
      .withColumnRenamed("municipio", "municipioAverage")
  }

  // Realiza o join entre o dataset original e o dataset de médias para análises adicionais
  def joinDS(parsed: Dataset[DetalhesCombustivel2], df: DataFrame) = {
    parsed.join(df,
      parsed.col("monthYear") === df.col("monthYearAverage") &&
      parsed.col("municipio") === df.col("municipioAverage"))
      .drop("municipioAverage")
      .drop("monthYearAverage")
      .withColumnRenamed("avg(precoMedioRevenda)", "avgPrecoMedioRevenda")
      .withColumnRenamed("avg(precoMinimoRevenda)", "avgPrecoMinimoRevenda")
      .withColumnRenamed("avg(precoMaximoRevenda)", "avgPrecoMaximoRevenda")
      .withColumnRenamed("avg(margemMediaRevenda)", "avgMargemMediaRevenda")
      .withColumnRenamed("avg(precoMedioDistribuicao)", "avgPrecoMedioDistribuicao")
      .withColumnRenamed("avg(precoMinimoDistribuicao)", "avgPrecoMinimoDistribuicao")
      .withColumnRenamed("avg(precoMaximoDistribuicao)", "avgPrecoMaximoDistribuicao")
      .as[DetalhesCombustivelAverage]
  }

  // Calcula a variância dos preços mínimos e máximos de revenda por município e mês
  def calculateVariance (df: Dataset[DetalhesCombustivelAverage]) = {
    val list = df.collect.toList
    val ds = list.map(p=> {
      val varianceMinRevenda = Math.pow(p.precoMinimoRevenda - p.avgPrecoMinimoRevenda, 2)
      val varianceMaxRevenda = Math.pow(p.precoMaximoRevenda - p.avgPrecoMaximoRevenda, 2)
      DetalhesCombustivelVariance(p.monthYear, p.municipio, varianceMinRevenda, varianceMaxRevenda)
    }).toDF

    ds.groupBy("monthYear", "municipio")
    .avg("varianceMinRevenda", "varianceMaxRevenda")
  }

  // Calcula a variação absoluta dos preços mínimos e máximos de revenda por município e mês
  def calculateVariation (df: Dataset[DetalhesCombustivelAverage]) = {
    val list = df.collect().toList
    val ds = list.map(p=> {
      val variationMinRevenda = p.precoMaximoRevenda - p.avgPrecoMaximoRevenda
      val variationMaxRevenda = p.precoMinimoRevenda - p.avgPrecoMinimoRevenda
      DetalhesCombustivelVariation(p.dataInicial, p.dataFinal, p.monthYear, p.regiao, p.estado, p.municipio, p.produto,
        p.numeroDePostosPesquisados, p.unidadeDeMedida, p.precoMedioRevenda, p.desvioPadraoRevenda, p.precoMinimoRevenda, p.precoMaximoRevenda,
        p.margemMediaRevenda, p.coefDeVariaçãoRevenda, p.precoMedioDistribuicao, p.desvioPadraoDistribuicao, p.precoMinimoDistribuicao, p.precoMaximoDistribuicao,
        p.coefDeVariaçãoDistribuicao, p.avgPrecoMedioRevenda, p.avgPrecoMinimoRevenda, p.avgPrecoMaximoRevenda, p.avgMargemMediaRevenda, p.avgPrecoMedioDistribuicao,
        p.avgPrecoMinimoDistribuicao, p.avgPrecoMedioDistribuicao, variationMinRevenda, variationMaxRevenda)
    }).toDF

    ds.groupBy("monthYear", "municipio")
      .avg("variationMinRevenda", "variationMaxRevenda")
  }

  // Calcula as 5 cidades com maior diferença entre o preço máximo e mínimo de revenda
  def calculateDifference (df: Dataset[DetalhesCombustivelAverage]) = {
    val ds = df.groupBy("municipio").agg(max("precoMaximoRevenda")- min("precoMinimoRevenda")as "difference")
    ds.sort(desc("difference")).take(5)
  }

}

