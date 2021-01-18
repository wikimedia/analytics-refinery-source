package org.wikimedia.analytics.refinery.job.structureddata.jsonparse

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._
import org.scalatest.{FlatSpec, Matchers}

class TestJsonDumpConverter extends FlatSpec with Matchers with DataFrameSuiteBase {

  private lazy implicit val formats = DefaultFormats

  private val wikidataEntityStrings = Seq(
    """
      |{
      |  "pageid": 186,
      |  "ns": 0,
      |  "title": "Q60",
      |  "lastrevid": 199780882,
      |  "modified": "2015-02-27T14:37:20Z",
      |  "id": "Q60",
      |  "type": "item",
      |  "aliases": {
      |      "en": [
      |        {
      |          "language": "en",
      |          "value": "NYC"
      |        },
      |        {
      |          "language": "en",
      |          "value": "New York"
      |        }
      |      ],
      |      "fr": [
      |        {
      |            "language": "fr",
      |            "value": "New York City"
      |        },
      |        {
      |            "language": "fr",
      |            "value": "NYC"
      |        }
      |      ],
      |      "zh-mo": [
      |        {
      |            "language": "zh-mo",
      |            "value": "\u7d10\u7d04\u5e02"
      |        }
      |    ]
      |    },
      |    "labels": {
      |    "en": {
      |        "language": "en",
      |        "value": "New York City"
      |    },
      |    "ar": {
      |        "language": "ar",
      |        "value": "\u0645\u062f\u064a\u0646\u0629 \u0646\u064a\u0648 \u064a\u0648\u0631\u0643"
      |    },
      |    "fr": {
      |        "language": "fr",
      |        "value": "New York City"
      |    },
      |    "my": {
      |        "language": "my",
      |        "value": "\u1014\u101a\u1030\u1038\u101a\u1031\u102c\u1000\u103a\u1019\u103c\u102d\u102f\u1037"
      |    },
      |    "ps": {
      |        "language": "ps",
      |        "value": "\u0646\u064a\u0648\u064a\u0627\u0631\u06a9"
      |    }
      |    },
      |    "descriptions": {
      |    "en": {
      |        "language": "en",
      |        "value": "largest city in New York and the United States of America"
      |    },
      |    "it": {
      |        "language": "it",
      |        "value": "citt\u00e0 degli Stati Uniti d'America"
      |    },
      |    "pl": {
      |        "language": "pl",
      |        "value": "miasto w Stanach Zjednoczonych"
      |    },
      |    "ro": {
      |        "language": "ro",
      |        "value": "ora\u015ful cel mai mare din SUA"
      |    }
      |    },
      |    "claims": {
      |    "P1151": [
      |        {
      |            "id": "Q60$6f832804-4c3f-6185-38bd-ca00b8517765",
      |            "mainsnak": {
      |                "snaktype": "value",
      |                "property": "P1151",
      |                "datatype": "string",
      |                "datavalue": {
      |                    "value": "fake string value",
      |                    "type": "string"
      |                }
      |            },
      |            "type": "statement",
      |            "rank": "normal"
      |        }
      |    ],
      |    "P625": [
      |        {
      |            "id": "q60$f00c56de-4bac-e259-b146-254897432868",
      |            "mainsnak": {
      |                "snaktype": "value",
      |                "property": "P625",
      |                "datatype": "globe-coordinate",
      |                "datavalue": {
      |                    "value": {
      |                        "latitude": 40.67,
      |                        "longitude": -73.94,
      |                        "altitude": null,
      |                        "precision": 0.00027777777777778,
      |                        "globe": "http://www.wikidata.org/entity/Q2"
      |                    },
      |                    "type": "globecoordinate"
      |                }
      |            },
      |            "type": "statement",
      |            "rank": "normal",
      |            "references": [
      |                {
      |                    "hash": "7eb64cf9621d34c54fd4bd040ed4b61a88c4a1a0",
      |                    "snaks": {
      |                        "P143": [
      |                            {
      |                                "snaktype": "value",
      |                                "property": "P143",
      |                                "datatype": "wikibase-item",
      |                                "datavalue": {
      |                                    "value": {
      |                                        "entity-type": "item",
      |                                        "numeric-id": 328
      |                                    },
      |                                    "type": "wikibase-entityid"
      |                                }
      |                            }
      |                        ]
      |                    },
      |                    "snaks-order": [
      |                        "P143"
      |                    ]
      |                }
      |            ]
      |        }
      |    ],
      |    "P150": [
      |        {
      |            "id": "Q60$bdddaa06-4e4b-f369-8954-2bb010aaa057",
      |            "mainsnak": {
      |                "snaktype": "value",
      |                "property": "P150",
      |                "datatype": "wikibase-item",
      |                "datavalue": {
      |                    "value": {
      |                        "entity-type": "item",
      |                        "numeric-id": 11299
      |                    },
      |                    "type": "wikibase-entityid"
      |                }
      |            },
      |            "type": "statement",
      |            "rank": "normal"
      |        },
      |        {
      |            "id": "Q60$0e484d5b-41a5-1594-7ae1-c3768c6206f6",
      |            "mainsnak": {
      |                "snaktype": "value",
      |                "property": "P150",
      |                "datatype": "wikibase-item",
      |                "datavalue": {
      |                    "value": {
      |                        "entity-type": "item",
      |                        "numeric-id": 18419
      |                    },
      |                    "type": "wikibase-entityid"
      |                }
      |            },
      |            "type": "statement",
      |            "rank": "normal"
      |        },
      |        {
      |            "id": "Q60$e5000a60-42fc-2aba-f16d-bade1d2e8a58",
      |            "mainsnak": {
      |                "snaktype": "value",
      |                "property": "P150",
      |                "datatype": "wikibase-item",
      |                "datavalue": {
      |                    "value": {
      |                        "entity-type": "item",
      |                        "numeric-id": 18424
      |                    },
      |                    "type": "wikibase-entityid"
      |                }
      |            },
      |            "type": "statement",
      |            "rank": "normal"
      |        },
      |        {
      |            "id": "Q60$4d90d6f4-4ab8-26bd-f2a5-4ac2a6eb48cd",
      |            "mainsnak": {
      |                "snaktype": "value",
      |                "property": "P150",
      |                "datatype": "wikibase-item",
      |                "datavalue": {
      |                    "value": {
      |                        "entity-type": "item",
      |                        "numeric-id": 18426
      |                    },
      |                    "type": "wikibase-entityid"
      |                }
      |            },
      |            "type": "statement",
      |            "rank": "normal"
      |        },
      |        {
      |            "id": "Q60$ede49e3c-44f6-75a3-eb74-6a89886e30c9",
      |            "mainsnak": {
      |                "snaktype": "value",
      |                "property": "P150",
      |                "datatype": "wikibase-item",
      |                "datavalue": {
      |                    "value": {
      |                        "entity-type": "item",
      |                        "numeric-id": 18432
      |                    },
      |                    "type": "wikibase-entityid"
      |                }
      |            },
      |            "type": "statement",
      |            "rank": "normal"
      |        }
      |    ],
      |    "P6": [
      |        {
      |            "id": "Q60$5cc8fc79-4807-9800-dbea-fe9c20ab273b",
      |            "mainsnak": {
      |                "snaktype": "value",
      |                "property": "P6",
      |                "datatype": "wikibase-item",
      |                "datavalue": {
      |                    "value": {
      |                        "entity-type": "item",
      |                        "numeric-id": 4911497
      |                    },
      |                    "type": "wikibase-entityid"
      |                }
      |            },
      |            "qualifiers": {
      |                "P580": [
      |                    {
      |                        "hash": "c53f3ca845b789e543ed45e3e1ecd1dd950e30dc",
      |                        "snaktype": "value",
      |                        "property": "P580",
      |                        "datatype": "time",
      |                        "datavalue": {
      |                            "value": {
      |                                "time": "+00000002014-01-01T00:00:00Z",
      |                                "timezone": 0,
      |                                "before": 0,
      |                                "after": 0,
      |                                "precision": 11,
      |                                "calendarmodel": "http://www.wikidata.org/entity/Q1985727"
      |                            },
      |                            "type": "time"
      |                        }
      |                    }
      |                ]
      |            },
      |            "qualifiers-order": [
      |                "P580"
      |            ],
      |            "type": "statement",
      |            "rank": "preferred"
      |        },
      |        {
      |            "id": "q60$cad4e313-4b5e-e089-08b9-3b1c7998e762",
      |            "mainsnak": {
      |                "snaktype": "value",
      |                "property": "P6",
      |                "datatype": "wikibase-item",
      |                "datavalue": {
      |                    "value": {
      |                        "entity-type": "item",
      |                        "numeric-id": 607
      |                    },
      |                    "type": "wikibase-entityid"
      |                }
      |            },
      |            "qualifiers": {
      |                "P580": [
      |                    {
      |                        "hash": "47c515b79f80e24e03375b327f2ac85184765d5b",
      |                        "snaktype": "value",
      |                        "property": "P580",
      |                        "datatype": "time",
      |                        "datavalue": {
      |                            "value": {
      |                                "time": "+00000002002-01-01T00:00:00Z",
      |                                "timezone": 0,
      |                                "before": 0,
      |                                "after": 0,
      |                                "precision": 11,
      |                                "calendarmodel": "http://www.wikidata.org/entity/Q1985727"
      |                            },
      |                            "type": "time"
      |                        }
      |                    }
      |                ],
      |                "P582": [
      |                    {
      |                        "hash": "1f463f78538c49ef6adf3a9b18e211af7195240a",
      |                        "snaktype": "value",
      |                        "property": "P582",
      |                        "datatype": "time",
      |                        "datavalue": {
      |                            "value": {
      |                                "time": "+00000002013-12-31T00:00:00Z",
      |                                "timezone": 0,
      |                                "before": 0,
      |                                "after": 0,
      |                                "precision": 11,
      |                                "calendarmodel": "http://www.wikidata.org/entity/Q1985727"
      |                            },
      |                            "type": "time"
      |                        }
      |                    }
      |                ]
      |            },
      |            "qualifiers-order": [
      |                "P580",
      |                "P582"
      |            ],
      |            "P856": [
      |       {
      |           "id": "Q60$4e3e7a42-4ec4-b7c3-7570-b103eb2bc1ac",
      |           "mainsnak": {
      |               "snaktype": "value",
      |               "property": "P856",
      |               "datatype": "url",
      |               "datavalue": {
      |                   "value": "http://nyc.gov/",
      |                   "type": "string"
      |               }
      |           },
      |           "type": "statement",
      |           "rank": "normal"
      |       }
      |    ]
      |    }]},
      |    "sitelinks": {
      |    "afwiki": {
      |       "site": "afwiki",
      |       "title": "New York Stad",
      |       "badges": []
      |    },
      |    "dewiki": {
      |        "site": "dewiki",
      |        "title": "New York City",
      |        "badges": [
      |            "Q17437798"
      |        ]
      |    },
      |    "dewikinews": {
      |        "site": "dewikinews",
      |        "title": "Kategorie:New York",
      |        "badges": []
      |    },
      |    "elwiki": {
      |        "site": "elwiki",
      |        "title": "\u039d\u03ad\u03b1 \u03a5\u03cc\u03c1\u03ba\u03b7",
      |        "badges": []
      |    },
      |    "enwiki": {
      |        "site": "enwiki",
      |        "title": "New York City",
      |        "badges": []
      |    },
      |    "zhwikivoyage": {
      |        "site": "zhwikivoyage",
      |        "title": "\u7d10\u7d04",
      |        "badges": []
      |    },
      |    "zuwiki": {
      |        "site": "zuwiki",
      |        "title": "New York (idolobha)",
      |        "badges": []
      |    }
      |  }
      |}
    """.stripMargin,
    """
      |{
      |  "pageid": 186,
      |  "ns": 0,
      |  "title": "Q60",
      |  "lastrevid": 199780882,
      |  "modified": "2015-02-27T14:37:20Z",
      |  "id": "Q60",
      |  "type": "item",
      |  "claims": {
      |    "P1151": [
      |        {
      |            "id": "Q60$6f832804-4c3f-6185-38bd-ca00b8517765",
      |            "mainsnak": {
      |                "snaktype": "value",
      |                "property": "P1151",
      |                "datatype": "wikibase-item",
      |                "datavalue": {
      |                    "value": {
      |                        "entity-type": "item",
      |                        "numeric-id": 6342720
      |                    },
      |                    "type": "wikibase-entityid"
      |                }
      |            },
      |            "type": "statement",
      |            "rank": "normal"
      |        }
      |    ],
      |    "P625": [
      |        {
      |            "id": "q60$f00c56de-4bac-e259-b146-254897432868",
      |            "mainsnak": {
      |                "snaktype": "value",
      |                "property": "P625",
      |                "datatype": "globe-coordinate",
      |                "datavalue": {
      |                    "value": {
      |                        "latitude": 40.67,
      |                        "longitude": -73.94,
      |                        "altitude": null,
      |                        "precision": 0.00027777777777778,
      |                        "globe": "http://www.wikidata.org/entity/Q2"
      |                    },
      |                    "type": "globecoordinate"
      |                }
      |            },
      |            "type": "statement",
      |            "rank": "normal",
      |            "references": [
      |                {
      |                    "hash": "7eb64cf9621d34c54fd4bd040ed4b61a88c4a1a0",
      |                    "snaks": {
      |                        "P143": [
      |                            {
      |                                "snaktype": "value",
      |                                "property": "P143",
      |                                "datatype": "wikibase-item",
      |                                "datavalue": {
      |                                    "value": {
      |                                        "entity-type": "item",
      |                                        "numeric-id": 328
      |                                    },
      |                                    "type": "wikibase-entityid"
      |                                }
      |                            }
      |                        ]
      |                    },
      |                    "snaks-order": [
      |                        "P143"
      |                    ]
      |                }
      |            ]
      |        }
      |    ],
      |    "P150": [
      |        {
      |            "id": "Q60$bdddaa06-4e4b-f369-8954-2bb010aaa057",
      |            "mainsnak": {
      |                "snaktype": "value",
      |                "property": "P150",
      |                "datatype": "wikibase-item",
      |                "datavalue": {
      |                    "value": {
      |                        "entity-type": "item",
      |                        "numeric-id": 11299
      |                    },
      |                    "type": "wikibase-entityid"
      |                }
      |            },
      |            "type": "statement",
      |            "rank": "normal"
      |        },
      |        {
      |            "id": "Q60$0e484d5b-41a5-1594-7ae1-c3768c6206f6",
      |            "mainsnak": {
      |                "snaktype": "value",
      |                "property": "P150",
      |                "datatype": "wikibase-item",
      |                "datavalue": {
      |                    "value": {
      |                        "entity-type": "item",
      |                        "numeric-id": 18419
      |                    },
      |                    "type": "wikibase-entityid"
      |                }
      |            },
      |            "type": "statement",
      |            "rank": "normal"
      |        },
      |        {
      |            "id": "Q60$e5000a60-42fc-2aba-f16d-bade1d2e8a58",
      |            "mainsnak": {
      |                "snaktype": "value",
      |                "property": "P150",
      |                "datatype": "wikibase-item",
      |                "datavalue": {
      |                    "value": {
      |                        "entity-type": "item",
      |                        "numeric-id": 18424
      |                    },
      |                    "type": "wikibase-entityid"
      |                }
      |            },
      |            "type": "statement",
      |            "rank": "normal"
      |        },
      |        {
      |            "id": "Q60$4d90d6f4-4ab8-26bd-f2a5-4ac2a6eb48cd",
      |            "mainsnak": {
      |                "snaktype": "value",
      |                "property": "P150",
      |                "datatype": "wikibase-item",
      |                "datavalue": {
      |                    "value": {
      |                        "entity-type": "item",
      |                        "numeric-id": 18426
      |                    },
      |                    "type": "wikibase-entityid"
      |                }
      |            },
      |            "type": "statement",
      |            "rank": "normal"
      |        },
      |        {
      |            "id": "Q60$ede49e3c-44f6-75a3-eb74-6a89886e30c9",
      |            "mainsnak": {
      |                "snaktype": "value",
      |                "property": "P150",
      |                "datatype": "wikibase-item",
      |                "datavalue": {
      |                    "value": {
      |                        "entity-type": "item",
      |                        "numeric-id": 18432
      |                    },
      |                    "type": "wikibase-entityid"
      |                }
      |            },
      |            "type": "statement",
      |            "rank": "normal"
      |        }
      |    ],
      |    "P6": [
      |        {
      |            "id": "Q60$5cc8fc79-4807-9800-dbea-fe9c20ab273b",
      |            "mainsnak": {
      |                "snaktype": "value",
      |                "property": "P6",
      |                "datatype": "wikibase-item",
      |                "datavalue": {
      |                    "value": {
      |                        "entity-type": "item",
      |                        "numeric-id": 4911497
      |                    },
      |                    "type": "wikibase-entityid"
      |                }
      |            },
      |            "qualifiers": {
      |                "P580": [
      |                    {
      |                        "hash": "c53f3ca845b789e543ed45e3e1ecd1dd950e30dc",
      |                        "snaktype": "value",
      |                        "property": "P580",
      |                        "datatype": "time",
      |                        "datavalue": {
      |                            "value": {
      |                                "time": "+00000002014-01-01T00:00:00Z",
      |                                "timezone": 0,
      |                                "before": 0,
      |                                "after": 0,
      |                                "precision": 11,
      |                                "calendarmodel": "http://www.wikidata.org/entity/Q1985727"
      |                            },
      |                            "type": "time"
      |                        }
      |                    }
      |                ]
      |            },
      |            "qualifiers-order": [
      |                "P580"
      |            ],
      |            "type": "statement",
      |            "rank": "preferred"
      |        },
      |        {
      |            "id": "q60$cad4e313-4b5e-e089-08b9-3b1c7998e762",
      |            "mainsnak": {
      |                "snaktype": "value",
      |                "property": "P6",
      |                "datatype": "wikibase-item",
      |                "datavalue": {
      |                    "value": {
      |                        "entity-type": "item",
      |                        "numeric-id": 607
      |                    },
      |                    "type": "wikibase-entityid"
      |                }
      |            },
      |            "qualifiers": {
      |                "P580": [
      |                    {
      |                        "hash": "47c515b79f80e24e03375b327f2ac85184765d5b",
      |                        "snaktype": "value",
      |                        "property": "P580",
      |                        "datatype": "time",
      |                        "datavalue": {
      |                            "value": {
      |                                "time": "+00000002002-01-01T00:00:00Z",
      |                                "timezone": 0,
      |                                "before": 0,
      |                                "after": 0,
      |                                "precision": 11,
      |                                "calendarmodel": "http://www.wikidata.org/entity/Q1985727"
      |                            },
      |                            "type": "time"
      |                        }
      |                    }
      |                ],
      |                "P582": [
      |                    {
      |                        "hash": "1f463f78538c49ef6adf3a9b18e211af7195240a",
      |                        "snaktype": "value",
      |                        "property": "P582",
      |                        "datatype": "time",
      |                        "datavalue": {
      |                            "value": {
      |                                "time": "+00000002013-12-31T00:00:00Z",
      |                                "timezone": 0,
      |                                "before": 0,
      |                                "after": 0,
      |                                "precision": 11,
      |                                "calendarmodel": "http://www.wikidata.org/entity/Q1985727"
      |                            },
      |                            "type": "time"
      |                        }
      |                    }
      |                ]
      |            },
      |            "qualifiers-order": [
      |                "P580",
      |                "P582"
      |            ],
      |            "P856": [
      |       {
      |           "id": "Q60$4e3e7a42-4ec4-b7c3-7570-b103eb2bc1ac",
      |           "mainsnak": {
      |               "snaktype": "value",
      |               "property": "P856",
      |               "datatype": "url",
      |               "datavalue": {
      |                   "value": "http://nyc.gov/",
      |                   "type": "string"
      |               }
      |           },
      |           "type": "statement",
      |           "rank": "normal"
      |       }
      |    ]
      |    }]},
      |    "sitelinks": {
      |    "afwiki": {
      |       "site": "afwiki",
      |       "title": "New York Stad",
      |       "badges": []
      |    },
      |    "dewiki": {
      |        "site": "dewiki",
      |        "title": "New York City",
      |        "badges": [
      |            "Q17437798"
      |        ]
      |    },
      |    "dewikinews": {
      |        "site": "dewikinews",
      |        "title": "Kategorie:New York",
      |        "badges": []
      |    },
      |    "elwiki": {
      |        "site": "elwiki",
      |        "title": "\u039d\u03ad\u03b1 \u03a5\u03cc\u03c1\u03ba\u03b7",
      |        "badges": []
      |    },
      |    "enwiki": {
      |        "site": "enwiki",
      |        "title": "New York City",
      |        "badges": []
      |    },
      |    "zhwikivoyage": {
      |        "site": "zhwikivoyage",
      |        "title": "\u7d10\u7d04",
      |        "badges": []
      |    },
      |    "zuwiki": {
      |        "site": "zuwiki",
      |        "title": "New York (idolobha)",
      |        "badges": []
      |    }
      |  }
      |}
      """.stripMargin,
    """
      |{"type":"item","id":"Q912321","labels":{"zh-hans":{"language":"zh-hans","value":"\u65b0\u7f55\u5e03\u4ec0\u5c14\u5dde\u5404\u53bf\u5217\u8868"},"zh-hant":{"language":"zh-hant","value":"\u65b0\u7f55\u5e03\u4ec0\u723e\u5dde\u5404\u7e23\u5217\u8868"},"zh-hk":{"language":"zh-hk","value":"\u65b0\u7f55\u5e03\u6b8a\u723e\u5dde\u5404\u7e23\u5217\u8868"},"zh-tw":{"language":"zh-tw","value":"\u65b0\u7f55\u5e03\u590f\u5dde\u5404\u7e23\u5217\u8868"},"eu":{"language":"eu","value":"New Hampshireko konderrien zerrenda"},"pl":{"language":"pl","value":"Lista hrabstw w stanie New Hampshire"},"ko":{"language":"ko","value":"\ub274\ud584\ud504\uc154 \uc8fc\uc758 \uad70 \ubaa9\ub85d"},"fr":{"language":"fr","value":"Comt\u00e9s de l'\u00c9tat du New Hampshire"},"en":{"language":"en","value":"List of counties in New Hampshire"},"es":{"language":"es","value":"Anexo:Condados de Nuevo Hampshire"},"ro":{"language":"ro","value":"List\u0103 de comitate din statul New Hampshire"},"ca":{"language":"ca","value":"Llista de comtats de Nou Hampshire"},"uk":{"language":"uk","value":"\u0421\u043f\u0438\u0441\u043e\u043a \u043e\u043a\u0440\u0443\u0433\u0456\u0432 \u0448\u0442\u0430\u0442\u0443 \u041d\u044c\u044e-\u0413\u0435\u043c\u043f\u0448\u0438\u0440"},"it":{"language":"it","value":"Contee del New Hampshire"},"de":{"language":"de","value":"Liste der Countys in New Hampshire"},"ja":{"language":"ja","value":"\u30cb\u30e5\u30fc\u30cf\u30f3\u30d7\u30b7\u30e3\u30fc\u5dde\u306e\u90e1\u4e00\u89a7"},"sv":{"language":"sv","value":"Lista \u00f6ver countyn i New Hampshire"},"nl":{"language":"nl","value":"lijst van county's in New Hampshire"},"sq":{"language":"sq","value":"Qarqe n\u00eb New Hampshire"},"nb":{"language":"nb","value":"Liste over fylker i New Hampshire"},"ru":{"language":"ru","value":"\u0421\u043f\u0438\u0441\u043e\u043a \u043e\u043a\u0440\u0443\u0433\u043e\u0432 \u041d\u044c\u044e-\u0413\u044d\u043c\u043f\u0448\u0438\u0440\u0430"},"zh":{"language":"zh","value":"\u65b0\u7f55\u5e03\u4ec0\u5c14\u5dde\u5404\u53bf\u5217\u8868"},"pt":{"language":"pt","value":"Anexo:Lista de condados de Nova Hampshire"},"bar":{"language":"bar","value":"Countys in New Hampshire"},"fa":{"language":"fa","value":"\u0641\u0647\u0631\u0633\u062a \u0634\u0647\u0631\u0633\u062a\u0627\u0646\u200c\u0647\u0627\u06cc \u0646\u06cc\u0648\u0647\u0645\u067e\u0634\u0627\u06cc\u0631"},"ar":{"language":"ar","value":"\u0642\u0627\u0626\u0645\u0629 \u0645\u0642\u0627\u0637\u0639\u0627\u062a \u0648\u0644\u0627\u064a\u0629 \u0646\u064a\u0648\u0647\u0627\u0645\u0628\u0634\u064a\u0631"},"he":{"language":"he","value":"\u05de\u05d7\u05d5\u05d6\u05d5\u05ea \u05e0\u05d9\u05d5 \u05d4\u05de\u05e4\u05e9\u05d9\u05d9\u05e8"},"hu":{"language":"hu","value":"New Hampshire megy\u00e9inek list\u00e1ja"}},"descriptions":{"fr":{"language":"fr","value":"page de liste de Wikip\u00e9dia"},"de":{"language":"de","value":"Wikimedia-Liste"},"en":{"language":"en","value":"Wikimedia list article"},"nl":{"language":"nl","value":"Wikimedia-lijst"},"bg":{"language":"bg","value":"\u0423\u0438\u043a\u0438\u043c\u0435\u0434\u0438\u044f \u0441\u043f\u0438\u0441\u044a\u043a"}},"aliases":{"zh":[{"language":"zh","value":"\u65b0\u7f55\u5e03\u4ec0\u5c14\u5dde\u884c\u653f\u533a\u5212"}],"eu":[{"language":"eu","value":"Hampshire Berriko konderrien zerrenda"}],"ko":[{"language":"ko","value":"\ub274\ud584\ud504\uc154 \uc8fc\uc758 \uad70"},{"language":"ko","value":"\ub274\ud584\ud504\uc154 \uc8fc\uc758 \uad70\uc758 \ubaa9\ub85d"}],"fr":[{"language":"fr","value":"Comtes de l'Etat du New Hampshire"}],"ca":[{"language":"ca","value":"Llista de comtats de Nova Hampshire"}],"ar":[{"language":"ar","value":"\u0645\u0642\u0627\u0637\u0639\u0627\u062a \u0648\u0644\u0627\u064a\u0629 \u0646\u064a\u0648\u0647\u0627\u0645\u0628\u0634\u064a\u0631 \u0627\u0644\u0623\u0645\u0631\u064a\u0643\u064a\u0629"}]},"claims":{"P360":[{"mainsnak":{"snaktype":"value","property":"P360","datavalue":{"value":{"entity-type":"item","numeric-id":13414753,"id":"Q13414753"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"q912321$4922a9af-4215-57db-2fae-8372b21f9de4","rank":"normal"}],"P31":[{"mainsnak":{"snaktype":"value","property":"P31","datavalue":{"value":{"entity-type":"item","numeric-id":13406463,"id":"Q13406463"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"Q912321$34046EF6-325F-469B-BCE8-CB18751F4101","rank":"normal"}],"P910":[{"mainsnak":{"snaktype":"value","property":"P910","datavalue":{"value":{"entity-type":"item","numeric-id":8798910,"id":"Q8798910"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"Q912321$45957E03-3D64-4119-9550-222509AECAC3","rank":"normal","references":[{"hash":"50f57a3dbac4708ce4ae4a827c0afac7fcdb4a5c","snaks":{"P143":[{"snaktype":"value","property":"P143","datavalue":{"value":{"entity-type":"item","numeric-id":11920,"id":"Q11920"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}]},"snaks-order":["P143"]}]}],"P373":[{"mainsnak":{"snaktype":"value","property":"P373","datavalue":{"value":"Counties of New Hampshire","type":"string"},"datatype":"string"},"type":"statement","id":"Q912321$350a3b90-4bd3-faae-3e6c-e2b72501306e","rank":"normal"}]},"sitelinks":{"euwiki":{"site":"euwiki","title":"New Hampshireko konderrien zerrenda","badges":[]},"plwiki":{"site":"plwiki","title":"Lista hrabstw w stanie New Hampshire","badges":[]},"kowiki":{"site":"kowiki","title":"\ub274\ud584\ud504\uc154 \uc8fc\uc758 \uad70 \ubaa9\ub85d","badges":[]},"frwiki":{"site":"frwiki","title":"Comt\u00e9s de l'\u00c9tat du New Hampshire","badges":[]},"eswiki":{"site":"eswiki","title":"Anexo:Condados de Nuevo Hampshire","badges":[]},"nowiki":{"site":"nowiki","title":"Liste over fylker i New Hampshire","badges":[]},"rowiki":{"site":"rowiki","title":"List\u0103 de comitate din statul New Hampshire","badges":[]},"cawiki":{"site":"cawiki","title":"Llista de comtats de Nou Hampshire","badges":[]},"ukwiki":{"site":"ukwiki","title":"\u0421\u043f\u0438\u0441\u043e\u043a \u043e\u043a\u0440\u0443\u0433\u0456\u0432 \u0448\u0442\u0430\u0442\u0443 \u041d\u044c\u044e-\u0413\u0435\u043c\u043f\u0448\u0438\u0440","badges":[]},"itwiki":{"site":"itwiki","title":"Contee del New Hampshire","badges":[]},"dewiki":{"site":"dewiki","title":"Liste der Countys in New Hampshire","badges":[]},"jawiki":{"site":"jawiki","title":"\u30cb\u30e5\u30fc\u30cf\u30f3\u30d7\u30b7\u30e3\u30fc\u5dde\u306e\u90e1\u4e00\u89a7","badges":[]},"simplewiki":{"site":"simplewiki","title":"List of counties in New Hampshire","badges":[]},"svwiki":{"site":"svwiki","title":"Lista \u00f6ver countyn i New Hampshire","badges":[]},"nlwiki":{"site":"nlwiki","title":"Lijst van county's in New Hampshire","badges":[]},"barwiki":{"site":"barwiki","title":"Countys in New Hampshire","badges":[]},"sqwiki":{"site":"sqwiki","title":"Qarqe n\u00eb New Hampshire","badges":[]},"ruwiki":{"site":"ruwiki","title":"\u0421\u043f\u0438\u0441\u043e\u043a \u043e\u043a\u0440\u0443\u0433\u043e\u0432 \u041d\u044c\u044e-\u0413\u044d\u043c\u043f\u0448\u0438\u0440\u0430","badges":[]},"enwiki":{"site":"enwiki","title":"List of counties in New Hampshire","badges":["Q17506997"]},"zhwiki":{"site":"zhwiki","title":"\u65b0\u7f55\u5e03\u4ec0\u5c14\u5dde\u884c\u653f\u533a\u5212","badges":["Q17506997"]},"ptwiki":{"site":"ptwiki","title":"Lista de condados de Nova Hampshire","badges":["Q17506997"]},"fawiki":{"site":"fawiki","title":"\u0641\u0647\u0631\u0633\u062a \u0634\u0647\u0631\u0633\u062a\u0627\u0646\u200c\u0647\u0627\u06cc \u0646\u06cc\u0648\u0647\u0645\u067e\u0634\u0627\u06cc\u0631","badges":[]},"hewiki":{"site":"hewiki","title":"\u05de\u05d7\u05d5\u05d6\u05d5\u05ea \u05e0\u05d9\u05d5 \u05d4\u05de\u05e4\u05e9\u05d9\u05d9\u05e8","badges":[]},"arwiki":{"site":"arwiki","title":"\u0642\u0627\u0626\u0645\u0629 \u0645\u0642\u0627\u0637\u0639\u0627\u062a \u0648\u0644\u0627\u064a\u0629 \u0646\u064a\u0648\u0647\u0627\u0645\u0628\u0634\u064a\u0631 \u0627\u0644\u0623\u0645\u0631\u064a\u0643\u064a\u0629","badges":[]},"huwiki":{"site":"huwiki","title":"New Hampshire megy\u00e9inek list\u00e1ja","badges":[]}}}""".stripMargin,
    """
      |{"type":"item","id":"Q61023","labels":{"pt":{"language":"pt","value":"Hermann von Eichhorn"},"pl":{"language":"pl","value":"Hermann von Eichhorn"},"ru":{"language":"ru","value":"\u042d\u0439\u0445\u0433\u043e\u0440\u043d, \u0413\u0435\u0440\u043c\u0430\u043d \u0444\u043e\u043d"},"fr":{"language":"fr","value":"Hermann von Eichhorn"},"es":{"language":"es","value":"Hermann von Eichhorn"},"en":{"language":"en","value":"Hermann von Eichhorn"},"uk":{"language":"uk","value":"\u0413\u0435\u0440\u043c\u0430\u043d \u0444\u043e\u043d \u0415\u0439\u0445\u0433\u043e\u0440\u043d"},"it":{"language":"it","value":"Hermann von Eichhorn"},"de":{"language":"de","value":"Hermann von Eichhorn"},"sl":{"language":"sl","value":"Hermann von Eichhorn"},"ja":{"language":"ja","value":"\u30d8\u30eb\u30de\u30f3\u30fb\u30d5\u30a9\u30f3\u30fb\u30a2\u30a4\u30d2\u30db\u30eb\u30f3"},"hr":{"language":"hr","value":"Hermann von Eichhorn"},"nl":{"language":"nl","value":"Hermann von Eichhorn"},"sv":{"language":"sv","value":"Hermann von Eichhorn"},"ca":{"language":"ca","value":"Hermann von Eichhorn"},"oc":{"language":"oc","value":"Hermann von Eichhorn"},"vi":{"language":"vi","value":"Hermann von Eichhorn"},"fi":{"language":"fi","value":"Hermann von Eichhorn"},"fa":{"language":"fa","value":"\u0647\u0631\u0645\u0627\u0646 \u0641\u0646 \u0622\u06cc\u0634\u0647\u0648\u0631\u0646"},"nb":{"language":"nb","value":"Hermann von Eichhorn"},"da":{"language":"da","value":"Hermann von Eichhorn"},"nn":{"language":"nn","value":"Hermann von Eichhorn"},"he":{"language":"he","value":"\u05d4\u05e8\u05de\u05df \u05e4\u05d5\u05df \u05d0\u05d9\u05d9\u05db\u05d4\u05d5\u05e8\u05df"},"be":{"language":"be","value":"\u0413\u0435\u0440\u043c\u0430\u043d \u0444\u043e\u043d \u0410\u0439\u0445\u0433\u043e\u0440\u043d"}},"descriptions":{"it":{"language":"it","value":"generale prussiano"},"fr":{"language":"fr","value":"g\u00e9n\u00e9ral prussien"},"de":{"language":"de","value":"preu\u00dfischer Offizier, zuletzt Generalfeldmarschall im Ersten Weltkrieg"},"en":{"language":"en","value":"German general"},"es":{"language":"es","value":"prusiano y alem\u00e1n"},"nl":{"language":"nl","value":"Duits soldaat (1848-1918)"},"he":{"language":"he","value":"\u05d0\u05d9\u05e9 \u05e6\u05d1\u05d0 \u05d2\u05e8\u05de\u05e0\u05d9"}},"aliases":{"ru":[{"language":"ru","value":"\u042d\u0439\u0445\u0433\u043e\u0440\u043d \u0413\u0435\u0440\u043c\u0430\u043d \u0444\u043e\u043d"},{"language":"ru","value":"\u042d\u0439\u0445\u0433\u043e\u0440\u043d, \u0413\u0435\u0440\u043c\u0430\u043d"},{"language":"ru","value":"\u0413\u0435\u0440\u043c\u0430\u043d \u0444\u043e\u043d \u042d\u0439\u0433\u0445\u043e\u0440\u043d"},{"language":"ru","value":"\u0410\u0439\u0445\u0433\u043e\u0440\u043d, \u0413\u0435\u0440\u043c\u0430\u043d \u0444\u043e\u043d"},{"language":"ru","value":"\u042d\u0439\u0433\u0445\u043e\u0440\u043d"},{"language":"ru","value":"\u0413\u0435\u0440\u043c\u0430\u043d \u0444\u043e\u043d \u042d\u0439\u0445\u0433\u043e\u0440\u043d"},{"language":"ru","value":"\u0413\u0435\u0440\u043c\u0430\u043d \u042d\u0439\u0445\u0433\u043e\u0440\u043d"},{"language":"ru","value":"\u0413. \u0444\u043e\u043d \u042d\u0439\u0445\u0433\u043e\u0440\u043d"},{"language":"ru","value":"\u0424\u043e\u043d \u042d\u0439\u0433\u0445\u043e\u0440\u043d"}],"uk":[{"language":"uk","value":"\u0415\u0439\u0445\u0433\u043e\u0440\u043d \u0413\u0435\u0440\u043c\u0430\u043d"},{"language":"uk","value":"\u0413\u0435\u0440\u043c\u0430\u043d \u0444\u043e\u043d \u0404\u0439\u0445\u0433\u043e\u0440\u043d"},{"language":"uk","value":"\u0410\u0439\u0445\u0433\u043e\u0440\u043d \u0413\u0435\u0440\u043c\u0430\u043d"},{"language":"uk","value":"\u0410\u0439\u0445\u0433\u043e\u0440\u043d \u0413\u0435\u0440\u043c\u0430\u043d \u0444\u043e\u043d"}],"hr":[{"language":"hr","value":"Eichhorn Hermann"},{"language":"hr","value":"Hermann von Eichorn"}],"sv":[{"language":"sv","value":"Eichhorn, Hermann von"}],"de":[{"language":"de","value":"Emil Gottfried Hermann von Eichhorn"}]},"claims":{"P21":[{"mainsnak":{"snaktype":"value","property":"P21","datavalue":{"value":{"entity-type":"item","numeric-id":6581097,"id":"Q6581097"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"q61023$FD063299-0595-43FB-AF1D-4F1FD15484BA","rank":"normal","references":[{"hash":"39f3ce979f9d84a0ebf09abe1702bf22326695e9","snaks":{"P143":[{"snaktype":"value","property":"P143","datavalue":{"value":{"entity-type":"item","numeric-id":169514,"id":"Q169514"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}]},"snaks-order":["P143"]},{"hash":"a51d6594fee36c7452eaed2db35a4833613a7078","snaks":{"P143":[{"snaktype":"value","property":"P143","datavalue":{"value":{"entity-type":"item","numeric-id":54919,"id":"Q54919"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}]},"snaks-order":["P143"]},{"hash":"50f57a3dbac4708ce4ae4a827c0afac7fcdb4a5c","snaks":{"P143":[{"snaktype":"value","property":"P143","datavalue":{"value":{"entity-type":"item","numeric-id":11920,"id":"Q11920"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}]},"snaks-order":["P143"]},{"hash":"298b51a5d4075225a5c70e053aefbf566adc9c26","snaks":{"P248":[{"snaktype":"value","property":"P248","datavalue":{"value":{"entity-type":"item","numeric-id":36578,"id":"Q36578"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}],"P813":[{"snaktype":"value","property":"P813","datavalue":{"value":{"time":"+2014-04-09T00:00:00Z","timezone":0,"before":0,"after":0,"precision":11,"calendarmodel":"http:\/\/www.wikidata.org\/entity\/Q1985727"},"type":"time"},"datatype":"time"}]},"snaks-order":["P248","P813"]}]}],"P119":[{"mainsnak":{"snaktype":"value","property":"P119","datavalue":{"value":{"entity-type":"item","numeric-id":643891,"id":"Q643891"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"q61023$E362BDEB-7723-4E5E-9185-2F721CA19E71","rank":"normal"}],"P214":[{"mainsnak":{"snaktype":"value","property":"P214","datavalue":{"value":"45082068","type":"string"},"datatype":"external-id"},"type":"statement","id":"q61023$0D4A3427-27D3-469A-9774-EE31F6303BBE","rank":"normal","references":[{"hash":"7eb64cf9621d34c54fd4bd040ed4b61a88c4a1a0","snaks":{"P143":[{"snaktype":"value","property":"P143","datavalue":{"value":{"entity-type":"item","numeric-id":328,"id":"Q328"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}]},"snaks-order":["P143"]},{"hash":"004ec6fbee857649acdbdbad4f97b2c8571df97b","snaks":{"P143":[{"snaktype":"value","property":"P143","datavalue":{"value":{"entity-type":"item","numeric-id":48183,"id":"Q48183"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}]},"snaks-order":["P143"]}]}],"P373":[{"mainsnak":{"snaktype":"value","property":"P373","datavalue":{"value":"Hermann von Eichhorn","type":"string"},"datatype":"string"},"type":"statement","id":"q61023$903F8208-477A-4F4F-8F38-73AC0B0E201D","rank":"normal","references":[{"hash":"004ec6fbee857649acdbdbad4f97b2c8571df97b","snaks":{"P143":[{"snaktype":"value","property":"P143","datavalue":{"value":{"entity-type":"item","numeric-id":48183,"id":"Q48183"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}]},"snaks-order":["P143"]}]}],"P227":[{"mainsnak":{"snaktype":"value","property":"P227","datavalue":{"value":"117497819","type":"string"},"datatype":"external-id"},"type":"statement","id":"q61023$CE916EC2-8C35-458A-86FE-D79F4E4C1643","rank":"normal","references":[{"hash":"004ec6fbee857649acdbdbad4f97b2c8571df97b","snaks":{"P143":[{"snaktype":"value","property":"P143","datavalue":{"value":{"entity-type":"item","numeric-id":48183,"id":"Q48183"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}]},"snaks-order":["P143"]}]}],"P19":[{"mainsnak":{"snaktype":"value","property":"P19","datavalue":{"value":{"entity-type":"item","numeric-id":1799,"id":"Q1799"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"q61023$7A649D6F-66D8-415B-90AE-6CE9223BD760","rank":"normal","references":[{"hash":"004ec6fbee857649acdbdbad4f97b2c8571df97b","snaks":{"P143":[{"snaktype":"value","property":"P143","datavalue":{"value":{"entity-type":"item","numeric-id":48183,"id":"Q48183"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}]},"snaks-order":["P143"]},{"hash":"8168a9dede71a2b10695bfd72033606263f2f9fb","snaks":{"P248":[{"snaktype":"value","property":"P248","datavalue":{"value":{"entity-type":"item","numeric-id":36578,"id":"Q36578"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}],"P813":[{"snaktype":"value","property":"P813","datavalue":{"value":{"time":"+2014-12-10T00:00:00Z","timezone":0,"before":0,"after":0,"precision":11,"calendarmodel":"http:\/\/www.wikidata.org\/entity\/Q1985727"},"type":"time"},"datatype":"time"}]},"snaks-order":["P248","P813"]}]}],"P20":[{"mainsnak":{"snaktype":"value","property":"P20","datavalue":{"value":{"entity-type":"item","numeric-id":1899,"id":"Q1899"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"q61023$E14FE570-B1B0-4226-9BCA-FD51BF71CD39","rank":"normal","references":[{"hash":"a1b58282921c52bb86fc766f0b7a4c8f0e9d3681","snaks":{"P248":[{"snaktype":"value","property":"P248","datavalue":{"value":{"entity-type":"item","numeric-id":36578,"id":"Q36578"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}],"P813":[{"snaktype":"value","property":"P813","datavalue":{"value":{"time":"+2014-12-30T00:00:00Z","timezone":0,"before":0,"after":0,"precision":11,"calendarmodel":"http:\/\/www.wikidata.org\/entity\/Q1985727"},"type":"time"},"datatype":"time"}]},"snaks-order":["P248","P813"]}]}],"P27":[{"mainsnak":{"snaktype":"value","property":"P27","datavalue":{"value":{"entity-type":"item","numeric-id":183,"id":"Q183"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"q61023$503821B9-97C7-46E5-A6F0-ABF09ECB9AE9","rank":"normal","references":[{"hash":"004ec6fbee857649acdbdbad4f97b2c8571df97b","snaks":{"P143":[{"snaktype":"value","property":"P143","datavalue":{"value":{"entity-type":"item","numeric-id":48183,"id":"Q48183"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}]},"snaks-order":["P143"]}]}],"P569":[{"mainsnak":{"snaktype":"value","property":"P569","datavalue":{"value":{"time":"+1848-02-13T00:00:00Z","timezone":0,"before":0,"after":0,"precision":11,"calendarmodel":"http:\/\/www.wikidata.org\/entity\/Q1985727"},"type":"time"},"datatype":"time"},"type":"statement","id":"q61023$1E8AB9D9-CC50-4687-B6E0-DADEBEFD4225","rank":"normal","references":[{"hash":"7eb64cf9621d34c54fd4bd040ed4b61a88c4a1a0","snaks":{"P143":[{"snaktype":"value","property":"P143","datavalue":{"value":{"entity-type":"item","numeric-id":328,"id":"Q328"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}]},"snaks-order":["P143"]},{"hash":"298b51a5d4075225a5c70e053aefbf566adc9c26","snaks":{"P248":[{"snaktype":"value","property":"P248","datavalue":{"value":{"entity-type":"item","numeric-id":36578,"id":"Q36578"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}],"P813":[{"snaktype":"value","property":"P813","datavalue":{"value":{"time":"+2014-04-09T00:00:00Z","timezone":0,"before":0,"after":0,"precision":11,"calendarmodel":"http:\/\/www.wikidata.org\/entity\/Q1985727"},"type":"time"},"datatype":"time"}]},"snaks-order":["P248","P813"]}]}],"P570":[{"mainsnak":{"snaktype":"value","property":"P570","datavalue":{"value":{"time":"+1918-07-30T00:00:00Z","timezone":0,"before":0,"after":0,"precision":11,"calendarmodel":"http:\/\/www.wikidata.org\/entity\/Q1985727"},"type":"time"},"datatype":"time"},"type":"statement","id":"q61023$B00BDB98-EC6B-4FC4-B8EA-2550DB4A3ECD","rank":"normal","references":[{"hash":"7eb64cf9621d34c54fd4bd040ed4b61a88c4a1a0","snaks":{"P143":[{"snaktype":"value","property":"P143","datavalue":{"value":{"entity-type":"item","numeric-id":328,"id":"Q328"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}]},"snaks-order":["P143"]},{"hash":"298b51a5d4075225a5c70e053aefbf566adc9c26","snaks":{"P248":[{"snaktype":"value","property":"P248","datavalue":{"value":{"entity-type":"item","numeric-id":36578,"id":"Q36578"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}],"P813":[{"snaktype":"value","property":"P813","datavalue":{"value":{"time":"+2014-04-09T00:00:00Z","timezone":0,"before":0,"after":0,"precision":11,"calendarmodel":"http:\/\/www.wikidata.org\/entity\/Q1985727"},"type":"time"},"datatype":"time"}]},"snaks-order":["P248","P813"]}]}],"P31":[{"mainsnak":{"snaktype":"value","property":"P31","datavalue":{"value":{"entity-type":"item","numeric-id":5,"id":"Q5"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"Q61023$93AA4F2E-5747-44C8-8F1B-54E3A5B9FA24","rank":"normal","references":[{"hash":"d6e3ab4045fb3f3feea77895bc6b27e663fc878a","snaks":{"P143":[{"snaktype":"value","property":"P143","datavalue":{"value":{"entity-type":"item","numeric-id":206855,"id":"Q206855"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}]},"snaks-order":["P143"]}]}],"P166":[{"mainsnak":{"snaktype":"value","property":"P166","datavalue":{"value":{"entity-type":"item","numeric-id":156478,"id":"Q156478"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"Q61023$B4F9AC8B-342E-4581-923B-0D8CF49EA4F8","rank":"normal","references":[{"hash":"7eb64cf9621d34c54fd4bd040ed4b61a88c4a1a0","snaks":{"P143":[{"snaktype":"value","property":"P143","datavalue":{"value":{"entity-type":"item","numeric-id":328,"id":"Q328"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}]},"snaks-order":["P143"]}]},{"mainsnak":{"snaktype":"value","property":"P166","datavalue":{"value":{"entity-type":"item","numeric-id":94145,"id":"Q94145"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"Q61023$33F9EB32-56EA-4253-BFBB-BC62989815AE","rank":"normal"},{"mainsnak":{"snaktype":"value","property":"P166","datavalue":{"value":{"entity-type":"item","numeric-id":18084456,"id":"Q18084456"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"Q61023$D2114B23-EAEB-4943-9835-70F90176105F","rank":"normal"}],"P607":[{"mainsnak":{"snaktype":"value","property":"P607","datavalue":{"value":{"entity-type":"item","numeric-id":154942,"id":"Q154942"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"Q61023$321DC321-F71F-4410-A4BA-C45953C0C653","rank":"normal","references":[{"hash":"7eb64cf9621d34c54fd4bd040ed4b61a88c4a1a0","snaks":{"P143":[{"snaktype":"value","property":"P143","datavalue":{"value":{"entity-type":"item","numeric-id":328,"id":"Q328"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}]},"snaks-order":["P143"]}]},{"mainsnak":{"snaktype":"value","property":"P607","datavalue":{"value":{"entity-type":"item","numeric-id":46083,"id":"Q46083"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"Q61023$FE14B859-B663-4149-8AC7-50B039CAC151","rank":"normal","references":[{"hash":"7eb64cf9621d34c54fd4bd040ed4b61a88c4a1a0","snaks":{"P143":[{"snaktype":"value","property":"P143","datavalue":{"value":{"entity-type":"item","numeric-id":328,"id":"Q328"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}]},"snaks-order":["P143"]}]},{"mainsnak":{"snaktype":"value","property":"P607","datavalue":{"value":{"entity-type":"item","numeric-id":361,"id":"Q361"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"Q61023$70D6BD9D-73A6-4582-BDD5-92D7CB8F527A","rank":"normal","references":[{"hash":"7eb64cf9621d34c54fd4bd040ed4b61a88c4a1a0","snaks":{"P143":[{"snaktype":"value","property":"P143","datavalue":{"value":{"entity-type":"item","numeric-id":328,"id":"Q328"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}]},"snaks-order":["P143"]}]}],"P646":[{"mainsnak":{"snaktype":"value","property":"P646","datavalue":{"value":"\/m\/0by930","type":"string"},"datatype":"external-id"},"type":"statement","id":"Q61023$C1E01A81-242E-40DC-AED3-3226C098F865","rank":"normal","references":[{"hash":"af38848ab5d9d9325cffd93a5ec656cc6ca889ed","snaks":{"P248":[{"snaktype":"value","property":"P248","datavalue":{"value":{"entity-type":"item","numeric-id":15241312,"id":"Q15241312"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}],"P577":[{"snaktype":"value","property":"P577","datavalue":{"value":{"time":"+2013-10-28T00:00:00Z","timezone":0,"before":0,"after":0,"precision":11,"calendarmodel":"http:\/\/www.wikidata.org\/entity\/Q1985727"},"type":"time"},"datatype":"time"}]},"snaks-order":["P248","P577"]}]}],"P18":[{"mainsnak":{"snaktype":"value","property":"P18","datavalue":{"value":"Hermann von Eichhorn.jpg","type":"string"},"datatype":"commonsMedia"},"type":"statement","id":"Q61023$4B9C4F7D-294D-459E-918C-4D9A8659AE29","rank":"normal","references":[{"hash":"f70116eac7f49194478b3025330bfd8dcffa3c69","snaks":{"P143":[{"snaktype":"value","property":"P143","datavalue":{"value":{"entity-type":"item","numeric-id":8447,"id":"Q8447"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}]},"snaks-order":["P143"]}]}],"P735":[{"mainsnak":{"snaktype":"value","property":"P735","datavalue":{"value":{"entity-type":"item","numeric-id":1158570,"id":"Q1158570"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"Q61023$FEBDCBAF-CD6C-48BC-9CB7-61DD3F272FEC","rank":"normal"}],"P1196":[{"mainsnak":{"snaktype":"value","property":"P1196","datavalue":{"value":{"entity-type":"item","numeric-id":149086,"id":"Q149086"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"Q61023$499A0ACC-C684-4298-89CF-96AF2DF0B58D","rank":"normal"}],"P269":[{"mainsnak":{"snaktype":"value","property":"P269","datavalue":{"value":"176960988","type":"string"},"datatype":"external-id"},"type":"statement","id":"Q61023$FB954882-E87B-43B5-9CD8-8A06FACF122D","rank":"normal","references":[{"hash":"ca9e0c52719e50a884b361d1a9a31d004195d376","snaks":{"P143":[{"snaktype":"value","property":"P143","datavalue":{"value":{"entity-type":"item","numeric-id":54919,"id":"Q54919"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}],"P813":[{"snaktype":"value","property":"P813","datavalue":{"value":{"time":"+2015-10-06T00:00:00Z","timezone":0,"before":0,"after":0,"precision":11,"calendarmodel":"http:\/\/www.wikidata.org\/entity\/Q1985727"},"type":"time"},"datatype":"time"}]},"snaks-order":["P143","P813"]}]}],"P106":[{"mainsnak":{"snaktype":"value","property":"P106","datavalue":{"value":{"entity-type":"item","numeric-id":47064,"id":"Q47064"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"Q61023$86445135-00C2-4612-9321-B5087C880BFC","rank":"normal"}],"P410":[{"mainsnak":{"snaktype":"value","property":"P410","datavalue":{"value":{"entity-type":"item","numeric-id":7820253,"id":"Q7820253"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"Q61023$C6AA033E-C023-4283-A47A-E7E1DBD6B057","rank":"normal","references":[{"hash":"3e9859118d01bc62b5dbe8939be812333eb7c594","snaks":{"P143":[{"snaktype":"value","property":"P143","datavalue":{"value":{"entity-type":"item","numeric-id":1551807,"id":"Q1551807"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}]},"snaks-order":["P143"]}]}],"P598":[{"mainsnak":{"snaktype":"value","property":"P598","datavalue":{"value":{"entity-type":"item","numeric-id":275229,"id":"Q275229"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"Q61023$C96DE689-1D92-4AE2-8963-434C0F3CDEB2","rank":"normal","references":[{"hash":"f70116eac7f49194478b3025330bfd8dcffa3c69","snaks":{"P143":[{"snaktype":"value","property":"P143","datavalue":{"value":{"entity-type":"item","numeric-id":8447,"id":"Q8447"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}]},"snaks-order":["P143"]}]}]},"sitelinks":{"ptwiki":{"site":"ptwiki","title":"Hermann von Eichhorn","badges":[]},"plwiki":{"site":"plwiki","title":"Hermann von Eichhorn","badges":[]},"ruwiki":{"site":"ruwiki","title":"\u042d\u0439\u0445\u0433\u043e\u0440\u043d, \u0413\u0435\u0440\u043c\u0430\u043d \u0444\u043e\u043d","badges":[]},"frwiki":{"site":"frwiki","title":"Hermann von Eichhorn","badges":[]},"eswiki":{"site":"eswiki","title":"Hermann von Eichhorn","badges":[]},"enwiki":{"site":"enwiki","title":"Hermann von Eichhorn","badges":[]},"ukwiki":{"site":"ukwiki","title":"\u0413\u0435\u0440\u043c\u0430\u043d \u0444\u043e\u043d \u0415\u0439\u0445\u0433\u043e\u0440\u043d","badges":[]},"itwiki":{"site":"itwiki","title":"Hermann von Eichhorn","badges":[]},"slwiki":{"site":"slwiki","title":"Hermann von Eichhorn","badges":[]},"jawiki":{"site":"jawiki","title":"\u30d8\u30eb\u30de\u30f3\u30fb\u30d5\u30a9\u30f3\u30fb\u30a2\u30a4\u30d2\u30db\u30eb\u30f3","badges":[]},"hrwiki":{"site":"hrwiki","title":"Hermann von Eichhorn","badges":[]},"nlwiki":{"site":"nlwiki","title":"Hermann von Eichhorn","badges":[]},"svwiki":{"site":"svwiki","title":"Hermann von Eichhorn","badges":[]},"viwiki":{"site":"viwiki","title":"Hermann von Eichhorn","badges":[]},"fiwiki":{"site":"fiwiki","title":"Hermann von Eichhorn","badges":[]},"fawiki":{"site":"fawiki","title":"\u0647\u0631\u0645\u0627\u0646 \u0641\u0646 \u0622\u06cc\u0634\u0647\u0648\u0631\u0646","badges":[]},"simplewiki":{"site":"simplewiki","title":"Hermann von Eichhorn","badges":[]},"hewiki":{"site":"hewiki","title":"\u05d4\u05e8\u05de\u05df \u05e4\u05d5\u05df \u05d0\u05d9\u05d9\u05db\u05d4\u05d5\u05e8\u05df","badges":[]},"dewiki":{"site":"dewiki","title":"Hermann von Eichhorn (Generalfeldmarschall)","badges":[]},"bewiki":{"site":"bewiki","title":"\u0413\u0435\u0440\u043c\u0430\u043d \u0444\u043e\u043d \u0410\u0439\u0445\u0433\u043e\u0440\u043d","badges":[]}}}""".stripMargin,
    """
      |{"type":"item","id":"Q626","labels":{"en":{"language":"en","value":"Volga"},"pl":{"language":"pl","value":"Wo\u0142ga"},"ru":{"language":"ru","value":"\u0412\u043e\u043b\u0433\u0430"},"fr":{"language":"fr","value":"Volga"},"be-tarask":{"language":"be-tarask","value":"\u0412\u043e\u043b\u0433\u0430"},"de":{"language":"de","value":"Wolga"},"it":{"language":"it","value":"Volga"},"es":{"language":"es","value":"Volga"},"vo":{"language":"vo","value":"Volga"},"en-ca":{"language":"en-ca","value":"Volga River"},"en-gb":{"language":"en-gb","value":"Volga River"},"af":{"language":"af","value":"Wolga"},"am":{"language":"am","value":"\u126e\u120d\u130b \u12c8\u1295\u12dd"},"an":{"language":"an","value":"R\u00edo Volga"},"ang":{"language":"ang","value":"Folga"},"ar":{"language":"ar","value":"\u0641\u0648\u0644\u063a\u0627"},"arz":{"language":"arz","value":"\u0646\u0647\u0631 \u0627\u0644\u0641\u0648\u0644\u062c\u0627"},"ay":{"language":"ay","value":"Volga"},"az":{"language":"az","value":"Volqa \u00e7ay\u0131"},"ba":{"language":"ba","value":"\u0412\u043e\u043b\u0433\u0430"},"bar":{"language":"bar","value":"Wolga"},"be":{"language":"be","value":"\u0420\u0430\u043a\u0430 \u0412\u043e\u043b\u0433\u0430"},"bg":{"language":"bg","value":"\u0412\u043e\u043b\u0433\u0430"},"bn":{"language":"bn","value":"\u09ad\u09cb\u09b2\u0997\u09be \u09a8\u09a6\u09c0"},"bo":{"language":"bo","value":"\u0f67\u0fa5\u0f7c\u0f62\u0f0b\u0f45\u0f71\u0f0b\u0f42\u0f59\u0f44\u0f0b\u0f54\u0f7c\u0f0d"},"br":{"language":"br","value":"Volga"},"bs":{"language":"bs","value":"Volga"},"ca":{"language":"ca","value":"Volga"},"cs":{"language":"cs","value":"Volha"},"cu":{"language":"cu","value":"\u0412\u043b\u044c\u0433\u0430"},"cv":{"language":"cv","value":"\u0410\u0442\u0103\u043b"},"cy":{"language":"cy","value":"Afon Volga"},"da":{"language":"da","value":"Volga"},"dsb":{"language":"dsb","value":"Wolga"},"el":{"language":"el","value":"\u0392\u03cc\u03bb\u03b3\u03b1\u03c2"},"eo":{"language":"eo","value":"Volgo"},"et":{"language":"et","value":"Volga"},"eu":{"language":"eu","value":"Volga"},"fa":{"language":"fa","value":"\u0631\u0648\u062f\u062e\u0627\u0646\u0647 \u0648\u0644\u06af\u0627"},"fi":{"language":"fi","value":"Volga"},"fy":{"language":"fy","value":"Wolga"},"ga":{"language":"ga","value":"An Volga"},"gl":{"language":"gl","value":"R\u00edo Volga"},"he":{"language":"he","value":"\u05d5\u05d5\u05dc\u05d2\u05d4"},"hi":{"language":"hi","value":"\u0935\u094b\u0932\u094d\u0917\u093e \u0928\u0926\u0940"},"hif":{"language":"hif","value":"Volga Naddi"},"hr":{"language":"hr","value":"Volga"},"hsb":{"language":"hsb","value":"Wolga"},"hu":{"language":"hu","value":"Volga"},"hy":{"language":"hy","value":"\u054e\u0578\u056c\u0563\u0561"},"id":{"language":"id","value":"Sungai Volga"},"ilo":{"language":"ilo","value":"Karayan Volga"},"io":{"language":"io","value":"Volga"},"is":{"language":"is","value":"Volga"},"ja":{"language":"ja","value":"\u30f4\u30a9\u30eb\u30ac\u5ddd"},"jv":{"language":"jv","value":"Kali Volga"},"ka":{"language":"ka","value":"\u10d5\u10dd\u10da\u10d2\u10d0"},"kk":{"language":"kk","value":"\u0415\u0434\u0456\u043b"},"kn":{"language":"kn","value":"\u0cb5\u0ccb\u0cb2\u0ccd\u0c97\u0cbe \u0ca8\u0ca6\u0cbf"},"ko":{"language":"ko","value":"\ubcfc\uac00 \uac15"},"koi":{"language":"koi","value":"\u0412\u043e\u043b\u0433\u0430"},"krc":{"language":"krc","value":"\u0418\u0442\u0438\u043b"},"ku":{"language":"ku","value":"Volga"},"la":{"language":"la","value":"Rha"},"lad":{"language":"lad","value":"Volga"},"lb":{"language":"lb","value":"Wolga"},"lez":{"language":"lez","value":"\u0412\u043e\u043b\u0433\u0430"},"li":{"language":"li","value":"Volga"},"lt":{"language":"lt","value":"Volga"},"lv":{"language":"lv","value":"Volga"},"mdf":{"language":"mdf","value":"\u0420\u0430\u0432\u0430"},"mhr":{"language":"mhr","value":"\u042e\u043b"},"mk":{"language":"mk","value":"\u0412\u043e\u043b\u0433\u0430"},"ml":{"language":"ml","value":"\u0d35\u0d4b\u0d7e\u0d17 \u0d28\u0d26\u0d3f"},"mn":{"language":"mn","value":"\u0418\u0436\u0438\u043b \u043c\u04e9\u0440\u04e9\u043d"},"mr":{"language":"mr","value":"\u0935\u094b\u0932\u094d\u0917\u093e \u0928\u0926\u0940"},"mrj":{"language":"mrj","value":"\u0419\u044b\u043b"},"ms":{"language":"ms","value":"Sungai Volga"},"mwl":{"language":"mwl","value":"Riu Bolga"},"my":{"language":"my","value":"\u1017\u1031\u102c\u103a\u101c\u103a\u1002\u102b\u1019\u103c\u1005\u103a"},"myv":{"language":"myv","value":"\u0420\u0430\u0432"},"na":{"language":"na","value":"Volga"},"nds":{"language":"nds","value":"Wolga"},"nl":{"language":"nl","value":"Wolga"},"nn":{"language":"nn","value":"Volga"},"oc":{"language":"oc","value":"V\u00f2lga"},"os":{"language":"os","value":"\u0412\u043e\u043b\u0433\u00e6"},"pms":{"language":"pms","value":"V\u00f2lga"},"pnb":{"language":"pnb","value":"\u062f\u0631\u06cc\u0627\u0626\u06d2 \u0648\u0648\u0644\u06af\u0627"},"pt":{"language":"pt","value":"Rio Volga"},"pt-br":{"language":"pt-br","value":"Rio Volga"},"rm":{"language":"rm","value":"Wolga"},"ro":{"language":"ro","value":"Volga"},"roa-tara":{"language":"roa-tara","value":"Volga"},"rue":{"language":"rue","value":"\u0412\u043e\u043b\u0491\u0430"},"scn":{"language":"scn","value":"Volga"},"sh":{"language":"sh","value":"Volga"},"sk":{"language":"sk","value":"Volga"},"sl":{"language":"sl","value":"Volga"},"sq":{"language":"sq","value":"Vollga"},"sr":{"language":"sr","value":"\u0412\u043e\u043b\u0433\u0430"},"stq":{"language":"stq","value":"Wolga"},"sv":{"language":"sv","value":"Volga"},"sw":{"language":"sw","value":"Volga"},"szl":{"language":"szl","value":"Wo\u0142ga"},"ta":{"language":"ta","value":"\u0bb5\u0bcb\u0bb2\u0bcd\u0b95\u0bbe \u0b86\u0bb1\u0bc1"},"tg":{"language":"tg","value":"\u0414\u0430\u0440\u0451\u0438 \u0412\u043e\u043b\u0433\u0430"},"th":{"language":"th","value":"\u0e41\u0e21\u0e48\u0e19\u0e49\u0e33\u0e27\u0e2d\u0e25\u0e01\u0e32"},"tk":{"language":"tk","value":"Wolga"},"tl":{"language":"tl","value":"Ilog Volga"},"tr":{"language":"tr","value":"Volga Nehri"},"tt":{"language":"tt","value":"\u0130del"},"udm":{"language":"udm","value":"\u0412\u043e\u043b\u0433\u0430"},"ug":{"language":"ug","value":"\u06cb\u0648\u0644\u06af\u0627 \u062f\u06d5\u0631\u064a\u0627\u0633\u0649"},"uk":{"language":"uk","value":"\u0412\u043e\u043b\u0433\u0430"},"ur":{"language":"ur","value":"\u062f\u0631\u06cc\u0627\u0626\u06d2 \u0648\u0648\u0644\u06af\u0627"},"vec":{"language":"vec","value":"Volga"},"vi":{"language":"vi","value":"S\u00f4ng Volga"},"war":{"language":"war","value":"Salog Volga"},"yi":{"language":"yi","value":"\u05d5\u05d5\u05d0\u05dc\u05d2\u05d0"},"zh":{"language":"zh","value":"\u4f0f\u5c14\u52a0\u6cb3"},"yue":{"language":"yue","value":"\u4f0f\u52a0\u6cb3"},"sco":{"language":"sco","value":"Volga"},"vep":{"language":"vep","value":"Volg"},"ckb":{"language":"ckb","value":"\u0695\u0648\u0648\u0628\u0627\u0631\u06cc \u06a4\u06c6\u06b5\u06af\u0627"},"pa":{"language":"pa","value":"\u0a35\u0a4b\u0a32\u0a17\u0a3e \u0a26\u0a30\u0a3f\u0a06"},"ia":{"language":"ia","value":"Fluvio Volga"},"uz":{"language":"uz","value":"Volga"},"kv":{"language":"kv","value":"\u0412\u043e\u043b\u0433\u0430"},"fo":{"language":"fo","value":"Volga"},"nb":{"language":"nb","value":"Volga"},"crh-latn":{"language":"crh-latn","value":"\u0130dil"},"gsw":{"language":"gsw","value":"Wolga"},"sgs":{"language":"sgs","value":"Vuolga"},"bxr":{"language":"bxr","value":"\u042d\u0436\u044d\u043b \u043c\u04af\u0440\u044d\u043d"},"ce":{"language":"ce","value":"\u0412\u043e\u043b\u0433\u0430"},"lmo":{"language":"lmo","value":"Volga"},"sah":{"language":"sah","value":"\u0412\u043e\u043b\u0433\u0430"},"pcd":{"language":"pcd","value":"Volga"},"gn":{"language":"gn","value":"Volga"},"xmf":{"language":"xmf","value":"\u10d5\u10dd\u10da\u10d2\u10d0"},"new":{"language":"new","value":"\u092d\u094b\u0932\u094d\u0917\u093e \u0916\u0941\u0938\u0940"},"kaa":{"language":"kaa","value":"Volga"},"ne":{"language":"ne","value":"\u092d\u094b\u0932\u094d\u0917\u093e"},"ky":{"language":"ky","value":"\u0412\u043e\u043b\u0433\u0430 \u0434\u0430\u0440\u044b\u044f\u0441\u044b"},"bho":{"language":"bho","value":"\u0935\u094b\u0932\u094d\u0917\u093e"},"ny":{"language":"ny","value":"Mtsinje wa Volga"},"mzn":{"language":"mzn","value":"\u0648\u0644\u06af\u0627"},"hak":{"language":"hak","value":"Volga h\u00f2"},"wuu":{"language":"wuu","value":"\u6d6e\u5361\u6cb3"},"as":{"language":"as","value":"\u09ad\u09b2\u09cd\u0997\u09be \u09a8\u09a6\u09c0"},"te":{"language":"te","value":"\u0c35\u0c4b\u0c32\u0c4d\u0c17\u0c3e \u0c28\u0c26\u0c3f"}},"descriptions":{"en":{"language":"en","value":"river in Russia, the longest river in Europe"},"ru":{"language":"ru","value":"\u0440\u0435\u043a\u0430 \u0432 \u0420\u043e\u0441\u0441\u0438\u0438, \u0441\u0430\u043c\u0430\u044f \u0434\u043b\u0438\u043d\u043d\u0430\u044f \u0440\u0435\u043a\u0430 \u0432 \u0415\u0432\u0440\u043e\u043f\u0435"},"pl":{"language":"pl","value":"Wielka rzeka w Rosji przeduralskiej."},"fr":{"language":"fr","value":"fleuve de Russie"},"de":{"language":"de","value":"gr\u00f6\u00dfter Strom Europas"},"it":{"language":"it","value":"fiume della Russia europea"},"es":{"language":"es","value":"R\u00edo de Rusia"},"ilo":{"language":"ilo","value":"karayan idiay Rusia ken kaatiddogan idiay Europa"},"hu":{"language":"hu","value":"foly\u00f3 Oroszorsz\u00e1g eur\u00f3pai ter\u00fclet\u00e9n, Eur\u00f3pa leghosszabb foly\u00f3ja"},"fa":{"language":"fa","value":"\u0631\u0648\u062f\u06cc \u062f\u0631 \u0631\u0648\u0633\u06cc\u0647\u060c \u06cc\u06a9\u06cc \u0627\u0632 \u0646\u0645\u0627\u062f\u0647\u0627\u06cc \u0645\u0644\u06cc \u0631\u0648\u0633\u06cc\u0647"},"zh-hans":{"language":"zh-hans","value":"\u4f4d\u4e8e\u4fc4\u7f57\u65af\u897f\u5357\u90e8\u7684\u6cb3\u6d41\uff0c\u6b27\u6d32\u6700\u957f\u7684\u6cb3\u6d41\uff0c\u4e16\u754c\u6700\u957f\u7684\u5185\u6d41\u6cb3"},"uk":{"language":"uk","value":"\u0440\u0456\u0447\u043a\u0430 \u0432 \u0420\u043e\u0441\u0456\u0457, \u0437 \u0434\u043e\u0432\u0436\u0438\u043d\u043e\u044e 3 530 \u043a\u043c \u2014 \u043d\u0430\u0439\u0431\u0456\u043b\u044c\u0448\u0430 \u0440\u0456\u043a\u0430 \u0432 \u0404\u0432\u0440\u043e\u043f\u0456"},"gl":{"language":"gl","value":"r\u00edo ruso o m\u00e1is longo e caudaloso de Europa."},"nl":{"language":"nl","value":"rivier in Rusland"},"hsb":{"language":"hsb","value":"r\u011bka we wuchodnej Europje"},"he":{"language":"he","value":"\u05e0\u05d4\u05e8 \u05d1\u05e8\u05d5\u05e1\u05d9\u05d4"},"ar":{"language":"ar","value":"\u0646\u0647\u0631 \u0641\u064a \u0631\u0648\u0633\u064a\u0627\u060c \u0648\u0623\u0637\u0648\u0644 \u0646\u0647\u0631 \u0641\u064a \u0623\u0648\u0631\u0628\u0627"},"cs":{"language":"cs","value":"\u0159eka v Rusku, nejdel\u0161\u00ed \u0159eka v Evrop\u011b"},"lv":{"language":"lv","value":"Upe Krievij\u0101, gar\u0101k\u0101 upe Eirop\u0101"}},"aliases":{"en":[{"language":"en","value":"Volga River"},{"language":"en","value":"River Volga"}],"gl":[{"language":"gl","value":"Volga"}],"ar":[{"language":"ar","value":"\u0646\u0647\u0631 \u0641\u0648\u0644\u063a\u0627"},{"language":"ar","value":"\u0641\u0648\u0644\u063a\u0627 (\u0646\u0647\u0631)"}],"it":[{"language":"it","value":"fiume Volga"}],"fr":[{"language":"fr","value":"Fleuve Volga"}],"ru":[{"language":"ru","value":"\u0420\u0435\u043a\u0430 \u0412\u043e\u043b\u0433\u0430"}],"ba":[{"language":"ba","value":"\u0418\u0499\u0435\u043b"}]},"claims":{"P31":[{"mainsnak":{"snaktype":"value","property":"P31","datavalue":{"value":{"entity-type":"item","numeric-id":4022,"id":"Q4022"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"q626$2A16CC81-6B07-46AC-981E-C504DD381864","rank":"normal","references":[{"hash":"288ab581e7d2d02995a26dfa8b091d96e78457fc","snaks":{"P143":[{"snaktype":"value","property":"P143","datavalue":{"value":{"entity-type":"item","numeric-id":206855,"id":"Q206855"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}]},"snaks-order":["P143"]}]}],"P1245":[{"mainsnak":{"snaktype":"value","property":"P1245","datavalue":{"value":"754860","type":"string"},"datatype":"external-id"},"type":"statement","id":"Q626$6a2eed30-462c-41c2-6ea1-f6e5286c1eaa","rank":"normal"}],"P17":[{"mainsnak":{"snaktype":"value","property":"P17","datavalue":{"value":{"entity-type":"item","numeric-id":159,"id":"Q159"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"q626$647ABBBE-C93A-458E-8288-5B05D353DF25","rank":"normal","references":[{"hash":"bc5ce58e4b2385b419be93c80d8334afb66be0de","snaks":{"P143":[{"snaktype":"value","property":"P143","datavalue":{"value":{"entity-type":"item","numeric-id":1768199,"id":"Q1768199"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}]},"snaks-order":["P143"]}]}],"P373":[{"mainsnak":{"snaktype":"value","property":"P373","datavalue":{"value":"Volga","type":"string"},"datatype":"string"},"type":"statement","id":"q626$A2C8B430-99C3-496E-9EBD-B0472ADAB144","rank":"normal"}],"P403":[{"mainsnak":{"snaktype":"value","property":"P403","datavalue":{"value":{"entity-type":"item","numeric-id":5484,"id":"Q5484"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"q626$3DF977DC-B181-4484-9BA9-A34299C31778","rank":"normal","references":[{"hash":"288ab581e7d2d02995a26dfa8b091d96e78457fc","snaks":{"P143":[{"snaktype":"value","property":"P143","datavalue":{"value":{"entity-type":"item","numeric-id":206855,"id":"Q206855"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}]},"snaks-order":["P143"]}]}],"P30":[{"mainsnak":{"snaktype":"value","property":"P30","datavalue":{"value":{"entity-type":"item","numeric-id":46,"id":"Q46"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"q626$53E8B08D-F216-42EC-9544-DB1BB58C925E","rank":"normal","references":[{"hash":"3bf39867b037e8e494a8389ae8a03bad6825a7fc","snaks":{"P143":[{"snaktype":"value","property":"P143","datavalue":{"value":{"entity-type":"item","numeric-id":191168,"id":"Q191168"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}]},"snaks-order":["P143"]}]}],"P646":[{"mainsnak":{"snaktype":"value","property":"P646","datavalue":{"value":"\/m\/0h2hw","type":"string"},"datatype":"external-id"},"type":"statement","id":"q626$46e1a7dc-455d-c965-74ac-e782627e209b","rank":"normal"}],"P625":[{"mainsnak":{"snaktype":"value","property":"P625","datavalue":{"value":{"latitude":45.841666666667,"longitude":47.971388888889,"altitude":null,"precision":0.00027777777777778,"globe":"http:\/\/www.wikidata.org\/entity\/Q2"},"type":"globecoordinate"},"datatype":"globe-coordinate"},"type":"statement","id":"q626$F8E8096A-FF38-478E-AA42-C2DF42D62512","rank":"normal","references":[{"hash":"fa278ebfc458360e5aed63d5058cca83c46134f1","snaks":{"P143":[{"snaktype":"value","property":"P143","datavalue":{"value":{"entity-type":"item","numeric-id":328,"id":"Q328"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}]},"snaks-order":["P143"]}]},{"mainsnak":{"snaktype":"value","property":"P625","datavalue":{"value":{"latitude":57.251331194444,"longitude":32.467965694444,"altitude":null,"precision":2.7777777777778e-8,"globe":"http:\/\/www.wikidata.org\/entity\/Q2"},"type":"globecoordinate"},"datatype":"globe-coordinate"},"type":"statement","qualifiers":{"P518":[{"snaktype":"value","property":"P518","hash":"2c7de14bc888b5d4d007acc4940641fc46b79ba0","datavalue":{"value":{"entity-type":"item","numeric-id":7376362,"id":"Q7376362"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}]},"qualifiers-order":["P518"],"id":"Q626$BC3EE40B-DFC7-4BDC-A733-C2392F70D693","rank":"normal","references":[{"hash":"288ab581e7d2d02995a26dfa8b091d96e78457fc","snaks":{"P143":[{"snaktype":"value","property":"P143","datavalue":{"value":{"entity-type":"item","numeric-id":206855,"id":"Q206855"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}]},"snaks-order":["P143"]}]},{"mainsnak":{"snaktype":"value","property":"P625","datavalue":{"value":{"latitude":45.6972,"longitude":47.8625,"altitude":null,"precision":0.0001,"globe":"http:\/\/www.wikidata.org\/entity\/Q2"},"type":"globecoordinate"},"datatype":"globe-coordinate"},"type":"statement","qualifiers":{"P518":[{"snaktype":"value","property":"P518","hash":"544b353c2ef8ad17b20e5e38254f37c3b3858ebf","datavalue":{"value":{"entity-type":"item","numeric-id":1233637,"id":"Q1233637"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}]},"qualifiers-order":["P518"],"id":"Q626$1FF91B3A-2B87-499F-87F8-3C64DC38FA91","rank":"normal","references":[{"hash":"288ab581e7d2d02995a26dfa8b091d96e78457fc","snaks":{"P143":[{"snaktype":"value","property":"P143","datavalue":{"value":{"entity-type":"item","numeric-id":206855,"id":"Q206855"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}]},"snaks-order":["P143"]}]}],"P402":[{"mainsnak":{"snaktype":"value","property":"P402","datavalue":{"value":"1730417","type":"string"},"datatype":"external-id"},"type":"statement","id":"Q626$845E242B-59BB-419A-A683-2DB47790F850","rank":"normal","references":[{"hash":"3bf39867b037e8e494a8389ae8a03bad6825a7fc","snaks":{"P143":[{"snaktype":"value","property":"P143","datavalue":{"value":{"entity-type":"item","numeric-id":191168,"id":"Q191168"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}]},"snaks-order":["P143"]}]}],"P227":[{"mainsnak":{"snaktype":"value","property":"P227","datavalue":{"value":"4079375-8","type":"string"},"datatype":"external-id"},"type":"statement","id":"Q626$5A219DBA-91A5-4050-A472-0EE91C074DD4","rank":"normal","references":[{"hash":"9a24f7c0208b05d6be97077d855671d1dfdbc0dd","snaks":{"P143":[{"snaktype":"value","property":"P143","datavalue":{"value":{"entity-type":"item","numeric-id":48183,"id":"Q48183"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}]},"snaks-order":["P143"]}]}],"P214":[{"mainsnak":{"snaktype":"value","property":"P214","datavalue":{"value":"244994202","type":"string"},"datatype":"external-id"},"type":"statement","id":"Q626$36CB7787-440B-468B-9641-84B2102505E4","rank":"normal","references":[{"hash":"759b2a264fff886006b6f49c3ef2f1acbfd1cef0","snaks":{"P143":[{"snaktype":"value","property":"P143","datavalue":{"value":{"entity-type":"item","numeric-id":54919,"id":"Q54919"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}]},"snaks-order":["P143"]}]}],"P349":[{"mainsnak":{"snaktype":"value","property":"P349","datavalue":{"value":"00629525","type":"string"},"datatype":"external-id"},"type":"statement","id":"Q626$0EB6FD07-5F11-4B3E-BBD8-5D7E301C1319","rank":"normal"}],"P18":[{"mainsnak":{"snaktype":"value","property":"P18","datavalue":{"value":"\u0412\u0438\u0434 \u043d\u0430 \u041a\u0438\u043d\u0435\u0448\u0435\u043c\u0441\u043a\u0438\u0439 \u043c\u043e\u0441\u0442 \u0438\u0437 \u0441\u0435\u043b\u0430 \u0412\u043e\u0437\u0434\u0432\u0438\u0436\u0435\u043d\u044c\u0435.JPG","type":"string"},"datatype":"commonsMedia"},"type":"statement","qualifiers":{"P2096":[{"snaktype":"value","property":"P2096","hash":"4992827215bf5430a56235e514c790ede3a899bd","datavalue":{"value":{"text":"\u0412\u043e\u043b\u0433\u0430 \u0443 \u0441\u0435\u043b\u0430 \u0412\u043e\u0437\u0434\u0432\u0438\u0436\u0435\u043d\u044c\u0435 \u0432 \u0418\u0432\u0430\u043d\u043e\u0432\u0441\u043a\u043e\u0439 \u043e\u0431\u043b\u0430\u0441\u0442\u0438","language":"ru"},"type":"monolingualtext"},"datatype":"monolingualtext"}]},"qualifiers-order":["P2096"],"id":"Q626$D7133168-A8FF-4900-BF15-CA86EE58B6AA","rank":"normal","references":[{"hash":"9a24f7c0208b05d6be97077d855671d1dfdbc0dd","snaks":{"P143":[{"snaktype":"value","property":"P143","datavalue":{"value":{"entity-type":"item","numeric-id":48183,"id":"Q48183"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}]},"snaks-order":["P143"]}]}],"P1200":[{"mainsnak":{"snaktype":"value","property":"P1200","datavalue":{"value":{"entity-type":"item","numeric-id":6718400,"id":"Q6718400"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"Q626$27B0E530-AB3D-490A-B10B-21AF09E2A114","rank":"normal","references":[{"hash":"288ab581e7d2d02995a26dfa8b091d96e78457fc","snaks":{"P143":[{"snaktype":"value","property":"P143","datavalue":{"value":{"entity-type":"item","numeric-id":206855,"id":"Q206855"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}]},"snaks-order":["P143"]}]}],"P1296":[{"mainsnak":{"snaktype":"value","property":"P1296","datavalue":{"value":"0071529","type":"string"},"datatype":"external-id"},"type":"statement","id":"Q626$D109FFE9-5FDD-42F5-9476-D98CCAB77454","rank":"normal","references":[{"hash":"b3fd5e254143d8b52cd96cff43af9af5e3a82358","snaks":{"P143":[{"snaktype":"value","property":"P143","datavalue":{"value":{"entity-type":"item","numeric-id":199693,"id":"Q199693"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}]},"snaks-order":["P143"]}]}],"P469":[{"mainsnak":{"snaktype":"value","property":"P469","datavalue":{"value":{"entity-type":"item","numeric-id":5484,"id":"Q5484"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"Q626$4784340D-0E36-425C-94B1-7F19E7ED54E5","rank":"normal"}],"P910":[{"mainsnak":{"snaktype":"value","property":"P910","datavalue":{"value":{"entity-type":"item","numeric-id":9201720,"id":"Q9201720"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"Q626$763C1F10-5E98-4470-AC2C-890670E3A770","rank":"normal","references":[{"hash":"60cce5d0acf0cc060796196012001192a048d885","snaks":{"P143":[{"snaktype":"value","property":"P143","datavalue":{"value":{"entity-type":"item","numeric-id":199698,"id":"Q199698"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}]},"snaks-order":["P143"]}]}],"P974":[{"mainsnak":{"snaktype":"value","property":"P974","datavalue":{"value":{"entity-type":"item","numeric-id":467419,"id":"Q467419"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"Q626$D860F6D4-363F-4E75-B86F-35F4EAB0ABF9","rank":"normal","references":[{"hash":"9a24f7c0208b05d6be97077d855671d1dfdbc0dd","snaks":{"P143":[{"snaktype":"value","property":"P143","datavalue":{"value":{"entity-type":"item","numeric-id":48183,"id":"Q48183"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}]},"snaks-order":["P143"]}]},{"mainsnak":{"snaktype":"value","property":"P974","datavalue":{"value":{"entity-type":"item","numeric-id":79082,"id":"Q79082"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"Q626$6D271924-C94F-44DB-83E8-872C81C92442","rank":"normal","references":[{"hash":"9a24f7c0208b05d6be97077d855671d1dfdbc0dd","snaks":{"P143":[{"snaktype":"value","property":"P143","datavalue":{"value":{"entity-type":"item","numeric-id":48183,"id":"Q48183"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}]},"snaks-order":["P143"]}]},{"mainsnak":{"snaktype":"value","property":"P974","datavalue":{"value":{"entity-type":"item","numeric-id":172089,"id":"Q172089"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"Q626$953918BE-3119-4617-AE08-8BDC1234718B","rank":"normal","references":[{"hash":"9a24f7c0208b05d6be97077d855671d1dfdbc0dd","snaks":{"P143":[{"snaktype":"value","property":"P143","datavalue":{"value":{"entity-type":"item","numeric-id":48183,"id":"Q48183"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}]},"snaks-order":["P143"]}]},{"mainsnak":{"snaktype":"value","property":"P974","datavalue":{"value":{"entity-type":"item","numeric-id":209891,"id":"Q209891"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"Q626$9cacadc6-41fc-ba0b-7eae-0d25ddf24803","rank":"normal","references":[{"hash":"fa278ebfc458360e5aed63d5058cca83c46134f1","snaks":{"P143":[{"snaktype":"value","property":"P143","datavalue":{"value":{"entity-type":"item","numeric-id":328,"id":"Q328"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}]},"snaks-order":["P143"]}]}],"P443":[{"mainsnak":{"snaktype":"value","property":"P443","datavalue":{"value":"Ru-\u0412\u043e\u043b\u0433\u0430.ogg","type":"string"},"datatype":"commonsMedia"},"type":"statement","id":"Q626$C98343CE-D5E1-485D-810F-CF4EBF18D7A7","rank":"normal"}],"P935":[{"mainsnak":{"snaktype":"value","property":"P935","datavalue":{"value":"Volga","type":"string"},"datatype":"string"},"type":"statement","id":"Q626$DF45CB82-DBF9-4C9C-A93E-354D17933D59","rank":"normal"}],"P884":[{"mainsnak":{"snaktype":"value","property":"P884","datavalue":{"value":"08010100112110000000017","type":"string"},"datatype":"external-id"},"type":"statement","id":"Q626$a4d3e844-4529-a82b-4995-017c9f867333","rank":"normal"}],"P1343":[{"mainsnak":{"snaktype":"value","property":"P1343","datavalue":{"value":{"entity-type":"item","numeric-id":2041543,"id":"Q2041543"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","qualifiers":{"P248":[{"snaktype":"value","property":"P248","hash":"8bbed2b1389a7e633300b312534ba34a18a34475","datavalue":{"value":{"entity-type":"item","numeric-id":23860757,"id":"Q23860757"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}]},"qualifiers-order":["P248"],"id":"Q626$A048E805-26A9-4A11-B9A9-434B65A687E1","rank":"normal"}],"P2043":[{"mainsnak":{"snaktype":"value","property":"P2043","datavalue":{"value":{"amount":"+3530","unit":"http:\/\/www.wikidata.org\/entity\/Q828224"},"type":"quantity"},"datatype":"quantity"},"type":"statement","id":"Q626$d937645d-4879-497b-86be-ddfb2e240884","rank":"normal","references":[{"hash":"988ba90ae92bb921fbe2408a94dcfc58f17fadd0","snaks":{"P854":[{"snaktype":"value","property":"P854","datavalue":{"value":"http:\/\/water-rf.ru\/\u0412\u043e\u0434\u043d\u044b\u0435_\u043e\u0431\u044a\u0435\u043a\u0442\u044b\/80\/\u0412\u043e\u043b\u0433\u0430","type":"string"},"datatype":"url"}]},"snaks-order":["P854"]}]}],"P2053":[{"mainsnak":{"snaktype":"value","property":"P2053","datavalue":{"value":{"amount":"+1360000","unit":"http:\/\/www.wikidata.org\/entity\/Q712226","upperBound":"+1360000","lowerBound":"+1360000"},"type":"quantity"},"datatype":"quantity"},"type":"statement","id":"Q626$688BE6D7-555D-4BAB-BCDD-7250737325EA","rank":"normal","references":[{"hash":"988ba90ae92bb921fbe2408a94dcfc58f17fadd0","snaks":{"P854":[{"snaktype":"value","property":"P854","datavalue":{"value":"http:\/\/water-rf.ru\/\u0412\u043e\u0434\u043d\u044b\u0435_\u043e\u0431\u044a\u0435\u043a\u0442\u044b\/80\/\u0412\u043e\u043b\u0433\u0430","type":"string"},"datatype":"url"}]},"snaks-order":["P854"]}]}],"P2225":[{"mainsnak":{"snaktype":"value","property":"P2225","datavalue":{"value":{"amount":"+8060","unit":"http:\/\/www.wikidata.org\/entity\/Q794261"},"type":"quantity"},"datatype":"quantity"},"type":"statement","id":"Q626$014DF131-A4F4-4BC8-9A8A-C734D9E1CE55","rank":"normal","references":[{"hash":"288ab581e7d2d02995a26dfa8b091d96e78457fc","snaks":{"P143":[{"snaktype":"value","property":"P143","datavalue":{"value":{"entity-type":"item","numeric-id":206855,"id":"Q206855"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}]},"snaks-order":["P143"]}]}],"P972":[{"mainsnak":{"snaktype":"value","property":"P972","datavalue":{"value":{"entity-type":"item","numeric-id":5460604,"id":"Q5460604"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"Q626$1453CB91-3BEA-4858-B5AC-BCDE13B4CA3B","rank":"normal"}],"P361":[{"mainsnak":{"snaktype":"value","property":"P361","datavalue":{"value":{"entity-type":"item","numeric-id":4173942,"id":"Q4173942"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"Q626$78f0bd5f-4f8f-3a26-9c79-2562ecda8b67","rank":"normal"}],"P2347":[{"mainsnak":{"snaktype":"value","property":"P2347","datavalue":{"value":"205603","type":"string"},"datatype":"external-id"},"type":"statement","id":"Q626$765AB1EE-F0CF-4DDF-9389-CE48BD87D40F","rank":"normal"}],"P1889":[{"mainsnak":{"snaktype":"value","property":"P1889","datavalue":{"value":{"entity-type":"item","numeric-id":1634214,"id":"Q1634214"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"Q626$6E58DA3E-EBFB-4937-8E5E-C56A73ED304A","rank":"normal"}],"P4614":[{"mainsnak":{"snaktype":"value","property":"P4614","datavalue":{"value":{"entity-type":"item","numeric-id":4079154,"id":"Q4079154"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"Q626$8adb6d3d-48a6-a922-d61e-5e7bd910531b","rank":"normal"}],"P885":[{"mainsnak":{"snaktype":"value","property":"P885","datavalue":{"value":{"entity-type":"item","numeric-id":194196,"id":"Q194196"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"Q626$49D82173-B909-43CC-893B-A3F9AAA5260A","rank":"normal","references":[{"hash":"c456dc5cd2117249948c288206ff3f8b1bf574f0","snaks":{"P143":[{"snaktype":"value","property":"P143","datavalue":{"value":{"entity-type":"item","numeric-id":8449,"id":"Q8449"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}]},"snaks-order":["P143"]}]}]},"sitelinks":{"commonswiki":{"site":"commonswiki","title":"Volga","badges":[]},"ruwikiquote":{"site":"ruwikiquote","title":"\u0412\u043e\u043b\u0433\u0430","badges":[]},"enwiki":{"site":"enwiki","title":"Volga River","badges":[]},"dewiki":{"site":"dewiki","title":"Wolga","badges":[]},"frwiki":{"site":"frwiki","title":"Volga","badges":[]},"eswiki":{"site":"eswiki","title":"Volga","badges":[]},"ruwiki":{"site":"ruwiki","title":"\u0412\u043e\u043b\u0433\u0430","badges":[]},"itwiki":{"site":"itwiki","title":"Volga","badges":[]},"jawiki":{"site":"jawiki","title":"\u30f4\u30a9\u30eb\u30ac\u5ddd","badges":[]},"nlwiki":{"site":"nlwiki","title":"Wolga","badges":[]},"plwiki":{"site":"plwiki","title":"Wo\u0142ga","badges":[]},"ptwiki":{"site":"ptwiki","title":"Rio Volga","badges":[]},"zhwiki":{"site":"zhwiki","title":"\u4f0f\u5c14\u52a0\u6cb3","badges":[]},"svwiki":{"site":"svwiki","title":"Volga","badges":[]},"fawiki":{"site":"fawiki","title":"\u0631\u0648\u062f \u0648\u0644\u06af\u0627","badges":[]},"hewiki":{"site":"hewiki","title":"\u05d5\u05d5\u05dc\u05d2\u05d4","badges":[]},"trwiki":{"site":"trwiki","title":"\u0130dil Nehri","badges":[]},"huwiki":{"site":"huwiki","title":"Volga","badges":[]},"fiwiki":{"site":"fiwiki","title":"Volga","badges":[]},"arwiki":{"site":"arwiki","title":"\u0641\u0648\u0644\u063a\u0627","badges":[]},"viwiki":{"site":"viwiki","title":"S\u00f4ng Volga","badges":[]},"nowiki":{"site":"nowiki","title":"Volga","badges":[]},"ukwiki":{"site":"ukwiki","title":"\u0412\u043e\u043b\u0433\u0430","badges":[]},"kowiki":{"site":"kowiki","title":"\ubcfc\uac00 \uac15","badges":[]},"cawiki":{"site":"cawiki","title":"Volga","badges":[]},"cswiki":{"site":"cswiki","title":"Volha","badges":[]},"srwiki":{"site":"srwiki","title":"\u0412\u043e\u043b\u0433\u0430","badges":[]},"rowiki":{"site":"rowiki","title":"Volga","badges":[]},"idwiki":{"site":"idwiki","title":"Sungai Volga","badges":[]},"dawiki":{"site":"dawiki","title":"Volga","badges":[]},"simplewiki":{"site":"simplewiki","title":"Volga River","badges":[]},"bgwiki":{"site":"bgwiki","title":"\u0412\u043e\u043b\u0433\u0430","badges":[]},"afwiki":{"site":"afwiki","title":"Wolga","badges":[]},"alswiki":{"site":"alswiki","title":"Wolga","badges":[]},"amwiki":{"site":"amwiki","title":"\u126e\u120d\u130b \u12c8\u1295\u12dd","badges":[]},"anwiki":{"site":"anwiki","title":"R\u00edo Volga","badges":[]},"angwiki":{"site":"angwiki","title":"Folga","badges":[]},"arzwiki":{"site":"arzwiki","title":"\u0646\u0647\u0631 \u0627\u0644\u0641\u0648\u0644\u062c\u0627","badges":[]},"aswiki":{"site":"aswiki","title":"\u09ad\u09b2\u09cd\u0997\u09be \u09a8\u09a6\u09c0","badges":[]},"astwiki":{"site":"astwiki","title":"Volga","badges":[]},"aywiki":{"site":"aywiki","title":"Volga","badges":[]},"azwiki":{"site":"azwiki","title":"Volqa","badges":[]},"bawiki":{"site":"bawiki","title":"\u0412\u043e\u043b\u0433\u0430","badges":[]},"barwiki":{"site":"barwiki","title":"Wolga","badges":[]},"bat_smgwiki":{"site":"bat_smgwiki","title":"Vuolga","badges":[]},"be_x_oldwiki":{"site":"be_x_oldwiki","title":"\u0412\u043e\u043b\u0433\u0430","badges":[]},"bhwiki":{"site":"bhwiki","title":"\u0935\u094b\u0932\u094d\u0917\u093e","badges":[]},"bnwiki":{"site":"bnwiki","title":"\u09ad\u09cb\u09b2\u0997\u09be \u09a8\u09a6\u09c0","badges":[]},"bowiki":{"site":"bowiki","title":"\u0f67\u0fa5\u0f7c\u0f62\u0f0b\u0f45\u0f71\u0f0b\u0f42\u0f59\u0f44\u0f0b\u0f54\u0f7c\u0f0d","badges":[]},"brwiki":{"site":"brwiki","title":"Volga","badges":[]},"bswiki":{"site":"bswiki","title":"Volga","badges":[]},"bxrwiki":{"site":"bxrwiki","title":"\u042d\u0436\u044d\u043b \u043c\u04af\u0440\u044d\u043d","badges":[]},"ckbwiki":{"site":"ckbwiki","title":"\u0695\u0648\u0648\u0628\u0627\u0631\u06cc \u06a4\u06c6\u06b5\u06af\u0627","badges":[]},"crhwiki":{"site":"crhwiki","title":"\u0130dil","badges":[]},"cuwiki":{"site":"cuwiki","title":"\u0412\u043b\u044c\u0433\u0430","badges":[]},"cvwiki":{"site":"cvwiki","title":"\u0410\u0442\u0103\u043b","badges":[]},"cywiki":{"site":"cywiki","title":"Afon Volga","badges":[]},"dsbwiki":{"site":"dsbwiki","title":"Wolga","badges":[]},"elwiki":{"site":"elwiki","title":"\u0392\u03cc\u03bb\u03b3\u03b1\u03c2","badges":[]},"eowiki":{"site":"eowiki","title":"Volgo","badges":[]},"etwiki":{"site":"etwiki","title":"Volga","badges":[]},"euwiki":{"site":"euwiki","title":"Volga","badges":[]},"fowiki":{"site":"fowiki","title":"Volga","badges":[]},"fywiki":{"site":"fywiki","title":"Wolga","badges":[]},"gawiki":{"site":"gawiki","title":"An Volga","badges":[]},"glwiki":{"site":"glwiki","title":"R\u00edo Volga","badges":[]},"gnwiki":{"site":"gnwiki","title":"Volga","badges":[]},"hakwiki":{"site":"hakwiki","title":"Volga h\u00f2","badges":[]},"hiwiki":{"site":"hiwiki","title":"\u0935\u094b\u0932\u094d\u0917\u093e \u0928\u0926\u0940","badges":[]},"hifwiki":{"site":"hifwiki","title":"Volga Naddi","badges":[]},"hrwiki":{"site":"hrwiki","title":"Volga","badges":[]},"hsbwiki":{"site":"hsbwiki","title":"Wolga","badges":[]},"hywiki":{"site":"hywiki","title":"\u054e\u0578\u056c\u0563\u0561 (\u0563\u0565\u057f)","badges":[]},"iawiki":{"site":"iawiki","title":"Fluvio Volga","badges":[]},"ilowiki":{"site":"ilowiki","title":"Karayan Volga","badges":[]},"iowiki":{"site":"iowiki","title":"Volga","badges":[]},"iswiki":{"site":"iswiki","title":"Volga","badges":[]},"jvwiki":{"site":"jvwiki","title":"Kali Volga","badges":[]},"kawiki":{"site":"kawiki","title":"\u10d5\u10dd\u10da\u10d2\u10d0","badges":[]},"kaawiki":{"site":"kaawiki","title":"Volga (da'rya)","badges":[]},"kbpwiki":{"site":"kbpwiki","title":"F\u0254l\u0269ka P\u0254\u0254","badges":[]},"kkwiki":{"site":"kkwiki","title":"\u0415\u0434\u0456\u043b","badges":[]},"knwiki":{"site":"knwiki","title":"\u0cb5\u0ccb\u0cb2\u0ccd\u0c97\u0cbe \u0ca8\u0ca6\u0cbf","badges":[]},"koiwiki":{"site":"koiwiki","title":"\u0412\u043e\u043b\u0433\u0430 (\u044e)","badges":[]},"krcwiki":{"site":"krcwiki","title":"\u0418\u0442\u0438\u043b","badges":[]},"kuwiki":{"site":"kuwiki","title":"Volga","badges":[]},"kvwiki":{"site":"kvwiki","title":"\u0412\u043e\u043b\u0433\u0430","badges":[]},"lawiki":{"site":"lawiki","title":"Rha","badges":[]},"ladwiki":{"site":"ladwiki","title":"Volga (rio)","badges":[]},"lbwiki":{"site":"lbwiki","title":"Wolga","badges":[]},"lezwiki":{"site":"lezwiki","title":"\u0412\u043e\u043b\u0433\u0430","badges":[]},"liwiki":{"site":"liwiki","title":"Volga","badges":[]},"lmowiki":{"site":"lmowiki","title":"Volga","badges":[]},"ltwiki":{"site":"ltwiki","title":"Volga","badges":[]},"lvwiki":{"site":"lvwiki","title":"Volga","badges":[]},"mdfwiki":{"site":"mdfwiki","title":"\u0420\u0430\u0432\u0430 (\u043b\u044f\u0439)","badges":[]},"mhrwiki":{"site":"mhrwiki","title":"\u042e\u043b","badges":[]},"mkwiki":{"site":"mkwiki","title":"\u0412\u043e\u043b\u0433\u0430","badges":[]},"mlwiki":{"site":"mlwiki","title":"\u0d35\u0d4b\u0d7e\u0d17 \u0d28\u0d26\u0d3f","badges":[]},"mnwiki":{"site":"mnwiki","title":"\u0418\u0436\u0438\u043b","badges":[]},"mrwiki":{"site":"mrwiki","title":"\u0935\u094b\u0932\u094d\u0917\u093e \u0928\u0926\u0940","badges":[]},"mrjwiki":{"site":"mrjwiki","title":"\u0419\u044b\u043b","badges":[]},"mswiki":{"site":"mswiki","title":"Sungai Volga","badges":[]},"mywiki":{"site":"mywiki","title":"\u1017\u1031\u102c\u103a\u101c\u103a\u1002\u102b\u1019\u103c\u1005\u103a","badges":[]},"myvwiki":{"site":"myvwiki","title":"\u0420\u0430\u0432","badges":[]},"mznwiki":{"site":"mznwiki","title":"\u0648\u0644\u06af\u0627","badges":[]},"nawiki":{"site":"nawiki","title":"Volga","badges":[]},"ndswiki":{"site":"ndswiki","title":"Wolga","badges":[]},"newwiki":{"site":"newwiki","title":"\u092d\u094b\u0932\u094d\u0917\u093e \u0916\u0941\u0938\u093f","badges":[]},"nnwiki":{"site":"nnwiki","title":"Volga","badges":[]},"nywiki":{"site":"nywiki","title":"Mtsinje wa Volga","badges":[]},"ocwiki":{"site":"ocwiki","title":"V\u00f2lga","badges":[]},"oswiki":{"site":"oswiki","title":"\u0412\u043e\u043b\u0433\u00e6","badges":[]},"pawiki":{"site":"pawiki","title":"\u0a35\u0a4b\u0a32\u0a17\u0a3e \u0a26\u0a30\u0a3f\u0a06","badges":[]},"pcdwiki":{"site":"pcdwiki","title":"Volga","badges":[]},"pmswiki":{"site":"pmswiki","title":"V\u00f2lga","badges":[]},"pnbwiki":{"site":"pnbwiki","title":"\u062f\u0631\u06cc\u0627\u0626\u06d2 \u0648\u0648\u0644\u06af\u0627","badges":[]},"rmwiki":{"site":"rmwiki","title":"Volga","badges":[]},"roa_tarawiki":{"site":"roa_tarawiki","title":"Volga","badges":[]},"ruewiki":{"site":"ruewiki","title":"\u0412\u043e\u043b\u0491\u0430","badges":[]},"sahwiki":{"site":"sahwiki","title":"\u0412\u043e\u043b\u0433\u0430","badges":[]},"scnwiki":{"site":"scnwiki","title":"Volga","badges":[]},"scowiki":{"site":"scowiki","title":"Volga","badges":[]},"sewiki":{"site":"sewiki","title":"Volga","badges":[]},"shwiki":{"site":"shwiki","title":"Volga","badges":[]},"skwiki":{"site":"skwiki","title":"Volga","badges":[]},"slwiki":{"site":"slwiki","title":"Volga","badges":[]},"sqwiki":{"site":"sqwiki","title":"Vollga","badges":[]},"stqwiki":{"site":"stqwiki","title":"Wolga","badges":[]},"swwiki":{"site":"swwiki","title":"Volga","badges":[]},"szlwiki":{"site":"szlwiki","title":"Wo\u0142ga","badges":[]},"tewiki":{"site":"tewiki","title":"\u0c35\u0c4b\u0c32\u0c4d\u0c17\u0c3e \u0c28\u0c26\u0c3f","badges":[]},"tgwiki":{"site":"tgwiki","title":"\u0412\u043e\u043b\u0433\u0430 (\u0434\u0430\u0440\u0451)","badges":[]},"thwiki":{"site":"thwiki","title":"\u0e41\u0e21\u0e48\u0e19\u0e49\u0e33\u0e27\u0e2d\u0e25\u0e01\u0e32","badges":[]},"tkwiki":{"site":"tkwiki","title":"Wolga","badges":[]},"tlwiki":{"site":"tlwiki","title":"Ilog Volga","badges":[]},"ttwiki":{"site":"ttwiki","title":"\u0418\u0434\u0435\u043b","badges":[]},"udmwiki":{"site":"udmwiki","title":"\u0412\u043e\u043b\u0433\u0430","badges":[]},"ugwiki":{"site":"ugwiki","title":"\u06cb\u0648\u0644\u06af\u0627 \u062f\u06d5\u0631\u064a\u0627\u0633\u0649","badges":[]},"urwiki":{"site":"urwiki","title":"\u062f\u0631\u06cc\u0627\u0626\u06d2 \u0648\u0648\u0644\u06af\u0627","badges":[]},"uzwiki":{"site":"uzwiki","title":"Volga","badges":[]},"vecwiki":{"site":"vecwiki","title":"Volga","badges":[]},"vepwiki":{"site":"vepwiki","title":"Volg","badges":[]},"vowiki":{"site":"vowiki","title":"Volga (flumed)","badges":[]},"warwiki":{"site":"warwiki","title":"Salog Volga","badges":[]},"wuuwiki":{"site":"wuuwiki","title":"\u6d6e\u5361\u6cb3","badges":[]},"xmfwiki":{"site":"xmfwiki","title":"\u10d5\u10dd\u10da\u10d2\u10d0","badges":[]},"yiwiki":{"site":"yiwiki","title":"\u05d5\u05d5\u05d0\u05dc\u05d2\u05d0","badges":[]},"zh_classicalwiki":{"site":"zh_classicalwiki","title":"\u7aa9\u74e6\u6cb3","badges":[]},"zh_yuewiki":{"site":"zh_yuewiki","title":"\u4f0f\u52a0\u6cb3","badges":[]},"tawiki":{"site":"tawiki","title":"\u0bb5\u0bcb\u0bb2\u0bcd\u0b95\u0bbe \u0b86\u0bb1\u0bc1","badges":[]},"mwlwiki":{"site":"mwlwiki","title":"Riu Volga","badges":[]},"bewiki":{"site":"bewiki","title":"\u0412\u043e\u043b\u0433\u0430","badges":[]},"cewiki":{"site":"cewiki","title":"\u0418\u0439\u0434\u0430\u043b","badges":[]},"kywiki":{"site":"kywiki","title":"\u042d\u0434\u0438\u043b \u0434\u0430\u0440\u044b\u044f\u0441\u044b","badges":[]},"azbwiki":{"site":"azbwiki","title":"\u0648\u0648\u0644\u0642\u0627","badges":[]}}}""".stripMargin
  )
  private val commonsEntityStrings = Seq(
    """
      |{"type":"mediainfo","id":"M76","labels":{},"descriptions":{},"statements":{"P571":[{"mainsnak":{"snaktype":"value","property":"P571","datavalue":{"value":{"time":"+2004-07-01T00:00:00Z","timezone":0,"before":0,"after":0,"precision":10,"calendarmodel":"http:\/\/www.wikidata.org\/entity\/Q1985727"},"type":"time"}},"type":"statement","id":"M76$B68618A8-13F1-4A89-84CC-6B93EC57B680","rank":"normal"}],"P275":[{"mainsnak":{"snaktype":"value","property":"P275","datavalue":{"value":{"entity-type":"item","numeric-id":30942811,"id":"Q30942811"},"type":"wikibase-entityid"}},"type":"statement","id":"M76$AF1C5EC6-560F-43CC-84DF-39FFE65DA334","rank":"normal"}],"P6216":[{"mainsnak":{"snaktype":"value","property":"P6216","datavalue":{"value":{"entity-type":"item","numeric-id":50423863,"id":"Q50423863"},"type":"wikibase-entityid"}},"type":"statement","id":"M76$6F781A4C-281F-4027-AA53-39BAFD39E683","rank":"normal"}],"P7482":[{"mainsnak":{"snaktype":"value","property":"P7482","datavalue":{"value":{"entity-type":"item","numeric-id":66458942,"id":"Q66458942"},"type":"wikibase-entityid"}},"type":"statement","id":"M76$C9E51947-96ED-4CCD-AB57-51949F4C8870","rank":"normal"}],"P1259":[{"mainsnak":{"snaktype":"value","property":"P1259","datavalue":{"value":{"latitude":52.9913,"longitude":6.5702,"altitude":null,"precision":0.0001,"globe":"http:\/\/www.wikidata.org\/entity\/Q2"},"type":"globecoordinate"}},"type":"statement","id":"M76$2DA5F4AA-859C-4D02-8C80-6FE2A1A6A5C7","rank":"normal"}],"P170":[{"mainsnak":{"snaktype":"somevalue","property":"P170"},"type":"statement","qualifiers":{"P3831":[{"snaktype":"value","property":"P3831","hash":"c5e04952fd00011abf931be1b701f93d9e6fa5d7","datavalue":{"value":{"entity-type":"item","numeric-id":33231,"id":"Q33231"},"type":"wikibase-entityid"}}],"P2093":[{"snaktype":"value","property":"P2093","hash":"2a087b86907501e19bf202d1ef9b0928227dfef7","datavalue":{"value":"Andre Engels","type":"string"}}],"P4174":[{"snaktype":"value","property":"P4174","hash":"2c7d417c4739ef08bf3ebc6ed503e6ae84b78b8c","datavalue":{"value":"Andre Engels","type":"string"}}],"P2699":[{"snaktype":"value","property":"P2699","hash":"e23bda3f654d736cb05311316b58d1513146d529","datavalue":{"value":"http:\/\/commons.wikimedia.org\/wiki\/User:Andre_Engels","type":"string"}}]},"qualifiers-order":["P3831","P2093","P4174","P2699"],"id":"M76$37240E97-7BAE-45F2-BA4E-3BD7998C07F2","rank":"normal"}],"P6757":[{"mainsnak":{"snaktype":"value","property":"P6757","datavalue":{"value":{"amount":"+0.004","unit":"http:\/\/www.wikidata.org\/entity\/Q11574"},"type":"quantity"}},"type":"statement","id":"M76$C7388097-D201-41E2-B2DC-0476EAC06876","rank":"normal"}],"P6790":[{"mainsnak":{"snaktype":"value","property":"P6790","datavalue":{"value":{"amount":"+3.5","unit":"1"},"type":"quantity"}},"type":"statement","id":"M76$71C303E3-1E8C-4C79-90D1-BB277551FBF5","rank":"normal"}],"P2151":[{"mainsnak":{"snaktype":"value","property":"P2151","datavalue":{"value":{"amount":"+8.64","unit":"http:\/\/www.wikidata.org\/entity\/Q174789"},"type":"quantity"}},"type":"statement","id":"M76$D92BA1C2-EFCF-4FDB-8B9D-8B7369052E24","rank":"normal"}],"P6789":[{"mainsnak":{"snaktype":"value","property":"P6789","datavalue":{"value":{"amount":"+100","unit":"1"},"type":"quantity"}},"type":"statement","id":"M76$37B6FBCE-0B5C-4551-B763-4A39EEA8DD39","rank":"normal"}],"P31":[{"mainsnak":{"snaktype":"value","property":"P31","datavalue":{"value":{"entity-type":"item","numeric-id":125191,"id":"Q125191"},"type":"wikibase-entityid"}},"type":"statement","id":"M76$D3C2DFB4-26E3-4962-88F2-F621FCFC1B09","rank":"normal"}]},"lastrevid":603841371}""".stripMargin,
    """
      |{"type":"mediainfo","id":"M99","labels":{},"descriptions":{},"statements":{"P180":[{"mainsnak":{"snaktype":"value","property":"P180","datavalue":{"value":{"entity-type":"item","numeric-id":17454866,"id":"Q17454866"},"type":"wikibase-entityid"}},"type":"statement","id":"M99$6D3656A3-8099-4236-8E72-DCFB72835036","rank":"normal"}],"P1071":[{"mainsnak":{"snaktype":"value","property":"P1071","datavalue":{"value":{"entity-type":"item","numeric-id":835118,"id":"Q835118"},"type":"wikibase-entityid"}},"type":"statement","id":"M99$BA7A5249-4272-464F-9B5D-CCC6A448C763","rank":"normal"}],"P170":[{"mainsnak":{"snaktype":"somevalue","property":"P170"},"type":"statement","qualifiers":{"P4174":[{"snaktype":"value","property":"P4174","hash":"2c7d417c4739ef08bf3ebc6ed503e6ae84b78b8c","datavalue":{"value":"Andre Engels","type":"string"}}],"P2699":[{"snaktype":"value","property":"P2699","hash":"94669b2e83d9cffc2a0d2136e0510ba3a045fcb3","datavalue":{"value":"https:\/\/commons.wikimedia.org\/wiki\/user:Andre_Engels","type":"string"}}],"P2093":[{"snaktype":"value","property":"P2093","hash":"2a087b86907501e19bf202d1ef9b0928227dfef7","datavalue":{"value":"Andre Engels","type":"string"}}]},"qualifiers-order":["P4174","P2699","P2093"],"id":"M99$CB909755-777B-42B9-93AF-0EA13ECE7D55","rank":"normal"}],"P275":[{"mainsnak":{"snaktype":"value","property":"P275","datavalue":{"value":{"entity-type":"item","numeric-id":30942811,"id":"Q30942811"},"type":"wikibase-entityid"}},"type":"statement","id":"M99$13262F7D-6A04-490A-B09C-15E4DE8D23EB","rank":"normal"}],"P6216":[{"mainsnak":{"snaktype":"value","property":"P6216","datavalue":{"value":{"entity-type":"item","numeric-id":50423863,"id":"Q50423863"},"type":"wikibase-entityid"}},"type":"statement","id":"M99$10353FED-3331-4E0E-8D2E-CD5C0793A5C9","rank":"normal"}],"P571":[{"mainsnak":{"snaktype":"value","property":"P571","datavalue":{"value":{"time":"+2004-08-01T00:00:00Z","timezone":0,"before":0,"after":0,"precision":10,"calendarmodel":"http:\/\/www.wikidata.org\/entity\/Q1985727"},"type":"time"}},"type":"statement","id":"M99$829CB76C-084A-4113-A535-3F7A7E2B41B4","rank":"normal"}],"P1259":[{"mainsnak":{"snaktype":"value","property":"P1259","datavalue":{"value":{"latitude":52.9442,"longitude":6.8001,"altitude":null,"precision":0.0001,"globe":"http:\/\/www.wikidata.org\/entity\/Q2"},"type":"globecoordinate"}},"type":"statement","id":"M99$0C3CD4B2-B7AA-49D3-ABC1-1E2E6048B531","rank":"normal"}]},"lastrevid":530053899}""".stripMargin,
    """
      |{"type":"mediainfo","id":"M189","labels":{},"descriptions":{},"statements":{"P7482":[{"mainsnak":{"snaktype":"value","property":"P7482","datavalue":{"value":{"entity-type":"item","numeric-id":66458942,"id":"Q66458942"},"type":"wikibase-entityid"}},"type":"statement","id":"M189$5DBBFBB7-87EB-4F4D-9C54-520D4A6178B8","rank":"normal"}],"P170":[{"mainsnak":{"snaktype":"somevalue","property":"P170"},"type":"statement","qualifiers":{"P3831":[{"snaktype":"value","property":"P3831","hash":"c5e04952fd00011abf931be1b701f93d9e6fa5d7","datavalue":{"value":{"entity-type":"item","numeric-id":33231,"id":"Q33231"},"type":"wikibase-entityid"}}],"P2093":[{"snaktype":"value","property":"P2093","hash":"e4885d4e6a217b9910f294b5bfdf0248929b8e1c","datavalue":{"value":"Quistnix","type":"string"}}],"P4174":[{"snaktype":"value","property":"P4174","hash":"e1e6d64358072ae4d907353162106afaf8983d5a","datavalue":{"value":"Quistnix","type":"string"}}],"P2699":[{"snaktype":"value","property":"P2699","hash":"d12f72ea36e05a04577394df51eb6624c312d066","datavalue":{"value":"http:\/\/commons.wikimedia.org\/wiki\/User:Quistnix","type":"string"}}]},"qualifiers-order":["P3831","P2093","P4174","P2699"],"id":"M189$00D3C5DF-D065-494D-BFEE-1D19FE417135","rank":"normal"}],"P6216":[{"mainsnak":{"snaktype":"value","property":"P6216","datavalue":{"value":{"entity-type":"item","numeric-id":50423863,"id":"Q50423863"},"type":"wikibase-entityid"}},"type":"statement","id":"M189$772A5A88-A904-4960-BA92-6AC38BF148C6","rank":"normal"}],"P275":[{"mainsnak":{"snaktype":"value","property":"P275","datavalue":{"value":{"entity-type":"item","numeric-id":50829104,"id":"Q50829104"},"type":"wikibase-entityid"}},"type":"statement","id":"M189$31ADBA43-5711-47BD-B569-026B2F79454F","rank":"normal"},{"mainsnak":{"snaktype":"value","property":"P275","datavalue":{"value":{"entity-type":"item","numeric-id":14946043,"id":"Q14946043"},"type":"wikibase-entityid"}},"type":"statement","id":"M189$E927B014-AB3B-4728-9DC5-40F2E1C49192","rank":"normal"}]},"lastrevid":427571512}""".stripMargin,
    """
      |{"type":"mediainfo","id":"M238","labels":{},"descriptions":{},"statements":{"P180":[{"mainsnak":{"snaktype":"value","property":"P180","datavalue":{"value":{"entity-type":"item","numeric-id":506126,"id":"Q506126"},"type":"wikibase-entityid"}},"type":"statement","id":"M238$4aad9d0f-4099-7d58-fb33-c7d03e53cf93","rank":"normal"}]},"lastrevid":604912849}""".stripMargin
  )

  private val wikidataRawLines = wikidataEntityStrings.map(_.split("\n").map(_.trim).mkString + ",")
  private val commonsRawLines = commonsEntityStrings.map(_ + ",")

  "JsonDumpConverter.parseEntity" should "work properly with wikidata examples entities" in {

    val jsonEntities = wikidataEntityStrings.map(s => JsonDumpConverter.parseEntity[WikidataJsonEntity](s))
    jsonEntities.size should be(5)
    val entities = jsonEntities.map(new WikidataEntity(_))
    entities.size should be(5)
    //entities.foreach(println)
  }

  it should "work properly with commons examples entities" in {

    val jsonEntities = commonsEntityStrings.map(s => JsonDumpConverter.parseEntity[CommonsJsonEntity](s))
    jsonEntities.size should be(4)
    val entities = jsonEntities.map(new CommonsEntity(_))
    entities.size should be(4)
    //entities.foreach(println)
  }

  "JsonDumpConverter.jsonLinesToTable" should "work properly with wikidata examples entities" in {

    val jsonLines = spark.sparkContext.parallelize(wikidataRawLines)
    jsonLines.count() should be(5)
    import spark.implicits._
    val table = JsonDumpConverter.jsonLinesToTable[WikidataEntity](spark, jsonLines, JsonDumpConverter.wikidataJsonConverter)
    table.count() should be(5)
    //table.show(10, false)
  }

  it should "work properly with commons examples entities" in {

    val jsonLines = spark.sparkContext.parallelize(commonsRawLines)
    jsonLines.count() should be(4)
    import spark.implicits._
    val table = JsonDumpConverter.jsonLinesToTable[CommonsEntity](spark, jsonLines, JsonDumpConverter.commonsJsonConverter)
    table.count() should be(4)
    //table.show(10, false)
  }


}
