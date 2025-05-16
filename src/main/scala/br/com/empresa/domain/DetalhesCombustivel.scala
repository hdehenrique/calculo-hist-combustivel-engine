
package br.com.empresa.domain

import java.text.SimpleDateFormat
import java.sql.{Date, Timestamp}


// Definição da case class principal que representa os dados de combustível já tratados e tipados
case class DetalhesCombustivel(
                         dataInicial:Date, // Data inicial do período
                         dataFinal: Date, // Data final do período
                         monthYear: String, // Mês/ano do registro
                         regiao: String, // Região do Brasil
                         estado: String, // Estado
                         municipio: String, // Município
                         produto: String, // Tipo de combustível
                         numeroDePostosPesquisados: Int, // Número de postos pesquisados
                         unidadeDeMedida: String, // Unidade de medida do preço
                         precoMedioRevenda: Double, // Preço médio de revenda
                         desvioPadraoRevenda: Double, // Desvio padrão do preço de revenda
                         precoMinimoRevenda: Double, // Preço mínimo de revenda
                         precoMaximoRevenda: Double, // Preço máximo de revenda
                         margemMediaRevenda: Option[Double], // Margem média de revenda (opcional)
                         coefDeVariaçãoRevenda: Double, // Coeficiente de variação da revenda
                         precoMedioDistribuicao: Option[Double], // Preço médio de distribuição (opcional)
                         desvioPadraoDistribuicao: Option[Double], // Desvio padrão da distribuição (opcional)
                         precoMinimoDistribuicao: Option[Double], // Preço mínimo de distribuição (opcional)
                         precoMaximoDistribuicao: Option[Double], // Preço máximo de distribuição (opcional)
                         coefDeVariaçãoDistribuicao: Option[Double] // Coeficiente de variação da distribuição (opcional)
                      )

// Versão intermediária dos dados, utilizada em etapas de transformação
case class DetalhesCombustivel2(
                              dataInicial:Date,
                              dataFinal: Date,
                              monthYear: String,
                              regiao: String,
                              estado: String,
                              municipio: String,
                              produto: String,
                              numeroDePostosPesquisados: Int,
                              unidadeDeMedida: String,
                              precoMedioRevenda: Double,
                              desvioPadraoRevenda: Double,
                              precoMinimoRevenda: Double,
                              precoMaximoRevenda: Double,
                              margemMediaRevenda: Option[Double],
                              coefDeVariaçãoRevenda: Double,
                              precoMedioDistribuicao: Option[Double],
                              desvioPadraoDistribuicao: Option[Double],
                              precoMinimoDistribuicao: Option[Double],
                              precoMaximoDistribuicao: Option[Double],
                              coefDeVariaçãoDistribuicao: Option[Double]
                            )

// Versão bruta dos dados, diretamente do CSV, todos os campos como String
case class DetalhesCombustivel3(
                               dataInicial:String,
                               dataFinal: String,
                               regiao: String,
                               estado: String,
                               municipio: String,
                               produto: String,
                               numeroDePostosPesquisados: String,
                               unidadeDeMedida: String,
                               precoMedioRevenda: String,
                               desvioPadraoRevenda: String,
                               precoMinimoRevenda: String,
                               precoMaximoRevenda: String,
                               margemMediaRevenda: String,
                               coefDeVariaçãoRevenda: String,
                               precoMedioDistribuicao: String,
                               desvioPadraoDistribuicao: String,
                               precoMinimoDistribuicao: String,
                               precoMaximoDistribuicao: String,
                               coefDeVariaçãoDistribuicao: String
                             )

object DetalhesCombustivel {
  // Função utilitária para converter String para java.sql.Date
  def dateFormat(date: String) = {
    val dateFormat = new SimpleDateFormat("dd/MM/yyyy")
    val parsedDate = dateFormat.parse(date)
    new Date(parsedDate.getTime)
  }

  // Retorna o mês de uma data java.sql.Date
  def getMonthByDate(date: Date) = {
    date.toLocalDate.getMonthValue
  }

  // Retorna o ano de uma data java.sql.Date
  def getYearByDate(date: Date) = {
    date.toLocalDate.getYear
  }

  // Retorna o menor valor entre dois inteiros
  def getMin(start: Int, end: Int) = {
    if(start < end) start else end
  }

  // Converte DadosCombustivel3 (strings) para DadosCombustivel2 (tipos corretos)
  def parseTo (p: DetalhesCombustivel3)= {
    val start = dateFormat(p.dataInicial)
    val end = dateFormat(p.dataFinal)
    val date = if(start.toLocalDate.isBefore(end.toLocalDate)){start} else {end}
    val montYear = s"""${this.getMonthByDate(date)}/${this.getYearByDate(date)}"""
    DetalhesCombustivel2(
      start,
      end,
      montYear,
      p.regiao,
      p.estado,
      p.municipio,
      p.produto,
      p.numeroDePostosPesquisados.toInt,
      p.unidadeDeMedida,
      p.precoMedioRevenda.replace(",", ".").toDouble,
      p.desvioPadraoRevenda.replace(",", ".").toDouble,
      p.precoMinimoRevenda.replace(",", ".").toDouble,
      p.precoMaximoRevenda.replace(",", ".").toDouble,
      try {
        Some(p.margemMediaRevenda.replace(",", ".").toDouble)
      } catch {
        case e: Exception => None
      },
      p.coefDeVariaçãoRevenda.replace(",", ".").toDouble,
      try {
        Some(p.precoMedioDistribuicao.replace(",", ".").toDouble)
      } catch {
        case e: Exception => None
      },
      try {
        Some(p.desvioPadraoDistribuicao.replace(",", ".").toDouble)
      } catch {
        case e: Exception => None
      },
      try {
        Some(p.precoMinimoDistribuicao.replace(",", ".").toDouble)
      } catch {
        case e: Exception => None
      },
      try {
        Some(p.precoMaximoDistribuicao.replace(",", ".").toDouble)
      } catch {
        case e: Exception => None
      },
      try {
        Some(p.coefDeVariaçãoDistribuicao.replace(",", ".").toDouble)
      } catch {
        case e: Exception => None
      }
    )
  }

}
