package br.com.empresa.domain

import java.sql.Date

// Case class que representa os dados de combustível agregados por média, incluindo campos originais e campos de médias calculadas
case class DetalhesCombustivelAverage(
                                     dataInicial: Date,
                                     dataFinal: Date,
                                     monthYear: String,
                                     regiao: String,
                                     estado: String,
                                     municipio: String,
                                     produto: String,
                                     numeroDePostosPesquisados: Integer,
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
                                     coefDeVariaçãoDistribuicao: Option[Double],
                                     avgPrecoMedioRevenda: Double, // Média do preço médio de revenda
                                     avgPrecoMinimoRevenda: Double, // Média do preço mínimo de revenda
                                     avgPrecoMaximoRevenda: Double, // Média do preço máximo de revenda
                                     avgMargemMediaRevenda: Option[Double], // Média da margem média de revenda
                                     avgPrecoMedioDistribuicao: Option[Double], // Média do preço médio de distribuição
                                     avgPrecoMinimoDistribuicao: Option[Double], // Média do preço mínimo de distribuição
                                     avgPrecoMaximoDistribuicao: Option[Double] // Média do preço máximo de distribuição
                                    )