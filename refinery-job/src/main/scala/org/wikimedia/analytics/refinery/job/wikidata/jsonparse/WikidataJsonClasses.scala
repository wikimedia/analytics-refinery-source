package org.wikimedia.analytics.refinery.job.wikidata.jsonparse

/**
 * This set of classes allow to parse wikidata JSON dump using json4s library
 */

case class JsonLanguageValue(
                              language: String,
                              value: String
                            )

case class JsonSiteLink(
                         site: String,
                         title: String,
                         badges: Option[Seq[String]],
                         url: Option[String]
                       )

case class JsonDataValue(
                          `type`: String, // string, wikibase-entityid, globecoordinate, quantity, time
                          value: String // to be interpreted based on --^
                        )

case class JsonSnak(
                     snaktype: String, // value, novalue, somevalue
                     property: String, // P22- should the same as the one it applies
                     datatype: Option[String],
                     datavalue: Option[JsonDataValue],
                     hash: Option[String]
                   )

case class JsonReference(
                          snaks: Map[String, Seq[JsonSnak]], // property -> [snaks]
                          `snaks-order`: Seq[String], // order of snacks using property
                          hash: Option[String]
                        )

case class JsonClaim(
                      id: String,
                      mainsnak: JsonSnak,
                      `type`: Option[String], // statement, claim
                      rank: Option[String], // preferred, normal, deprecated
                      qualifiers: Option[Map[String, Seq[JsonSnak]]], // property -> [snaks]
                      references: Option[Seq[JsonReference]]
                    )

case class JsonEntity(
                       id: String, // P22, Q333
                       `type`: String, // item, property
                       labels: Option[Map[String, JsonLanguageValue]], // lang -> value
                       descriptions: Option[Map[String, JsonLanguageValue]], // lang -> value
                       aliases: Option[Map[String, Seq[JsonLanguageValue]]], // lang -> [values]
                       claims: Map[String, Seq[JsonClaim]], // property -> [claims]
                       sitelinks: Option[Map[String, JsonSiteLink]]
                     )
