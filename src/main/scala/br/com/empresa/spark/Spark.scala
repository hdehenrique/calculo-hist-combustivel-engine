package br.com.empresa.spark

import br.com.empresa.settings.Settings
import org.apache.log4j.Level
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

// Objeto utilitário para configuração e gerenciamento da sessão Spark
object Spark extends Serializable {
  // Define o diretório do Hadoop para ambiente Windows
  System.setProperty("hadoop.home.dir", Settings.winUtils)

  // Instância única da SparkSession para toda a aplicação
  val session: SparkSession = this.setSession()

  // Função para definir o nível de log do Spark
  def setLogLevel(level: Level): Unit = {
    this.getContext.setLogLevel(level.toString)
  }

  // Cria e configura a SparkSession
  def setSession(): SparkSession = {
    val conf: SparkConf = new SparkConf()

    SparkSession
      .builder()
      .config(conf)
      .appName(Settings.appName)
      .getOrCreate()
  }

  // Retorna a SparkSession atual
  def getSession: SparkSession = {
    this.session
  }

  // Retorna o SparkContext associado à sessão
  def getContext: SparkContext = {
    this.getSession.sparkContext
  }

  // Retorna a configuração do SparkContext
  def getContextConf: SparkConf = {
    this.getContext.getConf
  }

  // Encerra o SparkContext e a SparkSession
  def stop(): Unit = {

    this.getContext.stop()
    this.getSession.stop()
  }
}