package org.wikimedia.analytics.refinery.job.structureddata.jsonparse

/**
 * These classes convert StructuredDataJsonClasses to a more data-friendly schema.
 * Details:
 *  - Maps of snaks and site-links are flattened, as their keys are referenced in the values
 *  - Some fields are renamed to prevent operator-name conflict in SQL
 */


/**
  * This object provide functions helping to convert from Json classes to Table classes
  *
  * Note: The nullify functions transform empty Map/Array to None (Option), preventing
  * hitting a bug of Spark not managing to write empty arrays in hive parquet tables
  * (https://issues.apache.org/jira/browse/SPARK-31345).
  * TODO: Remove the nullify function when upgrading to Spark 3
  */
object JsonToTableUtility {
  def nullifyMap[T, U](it: Option[Map[T, U]]): Option[Map[T, U]] = {
    if (it.isEmpty || it.get.isEmpty) None
    else it
  }

  def nullifySeq[T](it: Option[Seq[T]]): Option[Seq[T]] = {
    if (it.isEmpty || it.get.isEmpty) None
    else it
  }

  private def mapToSeq[T, U]
    (converter: T => U)
    (seqMap: Option[Map[String, Seq[T]]]): Option[Seq[U]] = {
    nullifyMap[String, Seq[T]](seqMap).map(_.flatMap {
      case (_, jsList) => jsList.map(js => converter(js))
    }.toSeq)
  }

  val snakMapToSnakSeq = mapToSeq[JsonSnak, Snak](js => new Snak(js))_
  val claimMapToClaimSeq = mapToSeq[JsonClaim, Claim](jc => new Claim(jc))_

  def langMapToStringMap(lm: Option[Map[String, JsonLanguageValue]]): Option[Map[String,String]] = {
    nullifyMap[String, JsonLanguageValue](lm).map(_.map({ case (lang, jlv) => lang -> jlv.value}))
  }

}

case class SiteLink(
                     site: String,
                     title: String,
                     badges: Option[Seq[String]],
                     url: Option[String]
                   ) {
  def this(jsl: JsonSiteLink) = this(jsl.site, jsl.title, JsonToTableUtility.nullifySeq[String](jsl.badges), jsl.url)
}

case class DataValue(
                      typ: String, // string, wikibase-entityid, globecoordinate, quantity, time
                      value: String // to be interpreted based on --^
                    ) {
  def this(jdv: JsonDataValue) = this(jdv.`type`, jdv.value)
}

case class Snak(
                 typ: String, // value, novalue, somevalue
                 property: String, // P22- should the same as the one it applies
                 dataType: Option[String],
                 dataValue: Option[DataValue],
                 hash: Option[String]
               ) {
  def this(js: JsonSnak) =
    this(
      js.snaktype,
      js.property,
      js.datatype,
      js.datavalue.map(jdv => DataValue(jdv.`type`, jdv.value)),
      js.hash
    )
}

case class Reference(
                      snaks: Option[Seq[Snak]],
                      snaksOrder: Option[Seq[String]], // order of snacks using property
                      hash: Option[String]
                    ) {
  def this(jr: JsonReference) =
    this(
      JsonToTableUtility.snakMapToSnakSeq(jr.snaks),
      JsonToTableUtility.nullifySeq[String](jr.`snaks-order`),
      jr.hash
    )
}

case class Claim(
                  id: String,
                  mainSnak: Snak,
                  typ: Option[String], // statement, claim
                  rank: Option[String], // preferred, normal, deprecated
                  qualifiers: Option[Seq[Snak]], // [snaks]
                  qualifiersOrder: Option[Seq[String]], // order of qualifiers using property
                  references: Option[Seq[Reference]]
                ) {
  def this(jc: JsonClaim) =
    this(
      jc.id,
      new Snak(jc.mainsnak),
      jc.`type`,
      jc.rank,
      JsonToTableUtility.snakMapToSnakSeq(jc.qualifiers),
      JsonToTableUtility.nullifySeq[String](jc.`qualifiers-order`),
      JsonToTableUtility.nullifySeq[JsonReference](jc.references).map(_.map(jr => new Reference(jr)))
    )
}

/**
 * CommonsEntity and WikidataEntity are both based off of the Wikibase JSON schema
 * with minor differences. See more in JsonClass definitions.
 */

case class CommonsEntity(
                          id: String, // M34875
                          typ: String, // mediainfo
                          labels: Option[Map[String, String]], // lang -> value
                          descriptions: Option[Map[String, String]], // lang -> value
                          statements: Option[Seq[Claim]],
                          lastRevId: Option[Long]
                        ) {
  def this(je: CommonsJsonEntity) =
    this(
      je.id,
      je.`type`,
      JsonToTableUtility.langMapToStringMap(je.labels),
      JsonToTableUtility.langMapToStringMap(je.descriptions),
      JsonToTableUtility.claimMapToClaimSeq(je.statements),
      je.lastrevid
    )
}

case class WikidataEntity(
                           id: String, // P22, Q333
                           typ: String, // item, property
                           dataType: Option[String], // only in properties
                           labels: Option[Map[String, String]], // lang -> value
                           descriptions: Option[Map[String, String]], // lang -> value
                           aliases: Option[Map[String, Seq[String]]], // lang -> [values]
                           claims: Option[Seq[Claim]],
                           siteLinks: Option[Seq[SiteLink]], // only in items
                           lastRevId: Option[Long]
                         ) {
  def this(je: WikidataJsonEntity) =
    this(
      je.id,
      je.`type`,
      je.datatype,
      JsonToTableUtility.langMapToStringMap(je.labels),
      JsonToTableUtility.langMapToStringMap(je.descriptions),
      JsonToTableUtility.nullifyMap[String, Seq[JsonLanguageValue]](je.aliases).map(_.map{ case (lang, jlvList) => lang -> jlvList.map(_.value)}),
      JsonToTableUtility.claimMapToClaimSeq(je.claims),
      JsonToTableUtility.nullifyMap[String, JsonSiteLink](je.sitelinks).map(_.map { case (_, jsl) => new SiteLink(jsl)}.toSeq),
      je.lastrevid
    )
}
