
package br.com.empresa.apps.batch

// Importação das classes de domínio e módulos de processamento
import br.com.empresa.domain.{DetalhesCombustivel, DetalhesCombustivel2, DetalhesCombustivel3}
import br.com.empresa.engine.Engine
import br.com.empresa.input.DataInput
import br.com.empresa.spark.Spark
import org.apache.log4j.Level
import org.joda.time.DateTime
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

// Objeto principal da aplicação
object CalcHistCombustivel {

 def main(args: Array[String]): Unit = {
  // Inicializa a sessão Spark
  val spark = SparkSession.builder()
    .appName("Meu Spark App")
    .master("local[*]")  // <- Define que o Spark rodará localmente
    .getOrCreate()

  // Instancia o carregador de dados
  val loadData = new DataInput
  // Instancia a engine de processamento
  val engine = new Engine(spark)

  import spark.implicits._

  // Define o nível de log do Spark para ERROR
  Spark.setLogLevel(Level.ERROR)

  // Carrega o CSV de entrada
  val x = loadData.loadCSV(spark)
  // Formata os dados para o modelo DadosCombustivel3
  val formated: Dataset[DetalhesCombustivel3] = engine.formatToDadosCombustivel(x).as[DetalhesCombustivel3]
  // Faz o parsing para o modelo final de dados
  val parsed = engine.parseToDadosCombustivel(formated)

  // Agrupa por mês e cidade, calculando médias
  val groupedMonth = engine.calculateAverageByMonth(parsed, "municipio")
  // Calcula médias por estado
  val groupedState = engine.calculateAverage(parsed, "estado")
  // Calcula médias por região
  val groupedRegion = engine.calculateAverage(parsed, "regiao")
  // Junta os datasets para análises adicionais
  val data = engine.joinDS(parsed, groupedMonth)
  // Calcula variância dos dados
  val variance = engine.calculateVariance(data)
  // Calcula variação absoluta dos dados
  val variation = engine.calculateVariation(data)
  // Calcula as 5 cidades com maior diferença de preço
  val maxDifference = engine.calculateDifference(data)

  // Exibe resultados das análises conforme solicitado no enunciado
  println("a) Agrupamento por mês e cálculo das médias por cidade.")
  groupedMonth.sort("monthYearAverage", "municipioAverage").show(truncate = false, numRows = 4000)

  println("b) Média de valor do combustível por estado e região.")
  println("por estado")
  groupedState.sort("estado", "produto").show(truncate = false, numRows = 4000)
  println("por região")
  groupedRegion.sort("regiao", "produto").show(truncate = false, numRows = 4000)

  println("c) Variância e variação absoluta do preço mês a mês por cidade.")
  println("variância")
  variance.show(truncate = false, numRows = 4000)
  println("variação absoluta")
  variation.show(truncate = false, numRows = 4000)

  println("d) As 5 cidades com maior diferença de preços de combustível.")
  maxDifference.map(p => println(p))
 }
}