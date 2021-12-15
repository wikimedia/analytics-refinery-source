package org.wikimedia.analytics.refinery.job.structureddata.jsonparse

/**
 * This set of classes allow to parse structured data JSON dump using json4s library
 * Schema docs: https://doc.wikimedia.org/Wikibase/master/php/md_docs_topics_json.html
 */

case class JsonSiteLink(
                         site: String,
                         title: String,
                         badges: Option[Seq[String]],
                         url: Option[String]
                       )

case class JsonLanguageValue(
                              language: String,
                              value: String
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
                          snaks: Option[Map[String, Seq[JsonSnak]]], // property -> [snaks]
                          `snaks-order`: Option[Seq[String]], // order of snacks using property
                          hash: Option[String]
                        )

case class JsonClaim(
                      id: String,
                      mainsnak: JsonSnak,
                      `type`: Option[String], // statement, claim
                      rank: Option[String], // preferred, normal, deprecated
                      qualifiers: Option[Map[String, Seq[JsonSnak]]], // property -> [snaks]
                      `qualifiers-order`: Option[Seq[String]],
                      references: Option[Seq[JsonReference]]
                    )

/**
 * CommonsJsonEntity and WikidataJsonEntity are both based off of the Wikibase JSON schema.
 * They are handled differently here because commons json does not have a few fields
 * (aliases, sitelinks, datatype), and the claims field is called statements.
 * Other than that, they operate over the same types of data.
 */

case class CommonsJsonEntity(
                              id: String, // M34875
                              `type`: String, // mediainfo
                              labels: Option[Map[String, JsonLanguageValue]], // lang -> value
                              descriptions: Option[Map[String, JsonLanguageValue]], // lang -> value
                              statements: Option[Map[String, Seq[JsonClaim]]], // property -> [statements]
                              lastrevid: Option[Long]
                            )

case class WikidataJsonEntity(
                               id: String, // P22, Q333
                               `type`: String, // item, property
                               datatype: Option[String], // only in properties
                               labels: Option[Map[String, JsonLanguageValue]], // lang -> value
                               descriptions: Option[Map[String, JsonLanguageValue]], // lang -> value
                               aliases: Option[Map[String, Seq[JsonLanguageValue]]], // lang -> [values]
                               claims: Option[Map[String, Seq[JsonClaim]]], // property -> [claims]
                               sitelinks: Option[Map[String, JsonSiteLink]], // only in items
                               lastrevid: Option[Long]
                             )