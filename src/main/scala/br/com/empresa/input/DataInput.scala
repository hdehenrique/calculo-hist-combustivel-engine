package br.com.empresa.input

import br.com.empresa.domain.{DetalhesCombustivel, DetalhesCombustivel2, DetalhesCombustivel3}
import br.com.empresa.settings.Settings
import org.apache.spark
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

// Classe responsável pelo carregamento dos dados do arquivo CSV
class DataInput {

  // Função para ler o arquivo CSV usando SparkSession e retornar um DataFrame
  def loadCSV(ss: SparkSession) = {
    import ss.implicits._
    ss.read.format("csv").option("header", "true").load(Settings.path)
  }

}
