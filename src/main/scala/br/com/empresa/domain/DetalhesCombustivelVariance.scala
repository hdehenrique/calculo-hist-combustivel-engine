package br.com.empresa.domain

import java.sql.Date

// Case class para armazenar a variância dos preços mínimos e máximos de revenda por município e mês
case class DetalhesCombustivelVariance(monthYear: String,
                                       municipio: String,
                                       varianceMinRevenda: Double,
                                       varianceMaxRevenda: Double
                                   )

// Case class para armazenar a variação absoluta dos preços de revenda e campos agregados por mês e município
case class DetalhesCombustivelVariation(
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
                                      avgPrecoMedioRevenda: Double,
                                      avgPrecoMinimoRevenda: Double,
                                      avgPrecoMaximoRevenda: Double,
                                      avgMargemMediaRevenda: Option[Double],
                                      avgPrecoMedioDistribuicao: Option[Double],
                                      avgPrecoMinimoDistribuicao: Option[Double],
                                      avgPrecoMaximoDistribuicao: Option[Double],
                                      variationMinRevenda: Double,
                                      variationMaxRevenda: Double
                                    )

// Case class para armazenar a diferença máxima de preço de revenda por município
case class DetalhesCombustivelDifference(municipio: String,
                                         differenceRevenda: Double
                                     )