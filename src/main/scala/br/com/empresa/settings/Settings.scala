package br.com.empresa.settings

import com.typesafe.config.ConfigFactory

// Objeto responsável por carregar e disponibilizar as configurações da aplicação a partir do application.conf
object Settings {

  // Carrega o arquivo de configuração padrão
  private val config = ConfigFactory.load()
  // Obtém as configurações específicas do Hadoop
  private val hadoopSettings = config.getConfig("hadoop_settings")
  // Obtém as configurações da aplicação CalculadoraCombustivel
  private val weblogGen = config.getConfig("calculadora-combustivel")
  // Caminho do winutils para ambiente Windows
  lazy val winUtils: String = hadoopSettings.getString("winUtils")
  // Nome da aplicação Spark
  lazy val appName: String = weblogGen.getString("appName")
  // Caminho do arquivo CSV de entrada
  lazy val path: String = weblogGen.getString("locationPath")




}
