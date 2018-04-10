package org.wikimedia.analytics.refinery.job.mediawikihistory.utils

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.scalatest.{BeforeAndAfterEach, FlatSpec}


// Left out of test class to prevent NotSerializable exception
case class EventTest(from: String, to: String) extends Edge[String] {
  override def fromKey = from
  override def toKey = to
}

case class StateTest(k: String) extends Vertex[String] {
  override def key = k
}

object StringRowKeyFormat extends RowKeyFormat[String] with Serializable {
  val struct = StructType(Seq(
    StructField("test_field", StringType, nullable = false)
  ))
  def toRow(k: String): Row = Row.fromTuple(new Tuple1(k))
  def toKey(r: Row): String = r.getString(0)
}


class TestSubgraphPartitioner
    extends FlatSpec
    with SharedSparkContext
    with BeforeAndAfterEach
    with RDDComparisons {

  var subgraphPartitioner = null.asInstanceOf[SubgraphPartitioner[String, EventTest, StateTest]]

  override def beforeEach(): Unit = {
    val username = System.getProperty("user.name")
    sc.setCheckpointDir(s"/tmp/${username}/unittest/refinery-source/refinery-job/TestSubgraphPartitioner")
    val sqlContext = new SQLContext(sc)
    sqlContext.sql("SET spark.sql.shuffle.partitions=2")
    subgraphPartitioner = new SubgraphPartitioner[String, EventTest, StateTest](sqlContext, StringRowKeyFormat)
  }

  // To prevent ordering errors
  def partitionToSet(events: RDD[EventTest], states: RDD[StateTest]): RDD[(Set[EventTest], Set[StateTest])] = {
    subgraphPartitioner.run(events, states).map {
      case (itEvents, itStates) => (itEvents.toSet, itStates.toSet)
    }
  }

  "SubgraphPartitioner" should "return empty results if given empty inputs" in {

    val events = sc.emptyRDD[EventTest]
    val states = sc.emptyRDD[StateTest]

    val result = partitionToSet(events, states)

    val expectedResult = sc.emptyRDD[(Set[EventTest], Set[StateTest])]

    assertRDDEquals(expectedResult, result) // succeed

  }

  it should "return only states if given only states inputs" in {
    val events = sc.emptyRDD[EventTest]
    val states = sc.parallelize(Seq(new StateTest("A"), new StateTest("B")))

    val result = partitionToSet(events, states)

    val expectedResult = sc.parallelize(Seq(
      // Subgraph 1
      (Set.empty[EventTest],
        Set(new StateTest("A"))),
      // Subgraph 2
      (Set.empty[EventTest],
        Set(new StateTest("B")))
    ))

    assertRDDEquals(expectedResult, result)

  }

  it should "return only events if given only events inputs" in {
    //
    //  A --> B --> C
    //        | --> D
    //
    //  E --> F
    val events = sc.parallelize(
        Seq(
            // subgraph 1
            new EventTest("A", "B"),
            new EventTest("B", "C"),
            new EventTest("B", "D"),
            // subgraph 2
            new EventTest("E", "F")
        ))
    val states = sc.emptyRDD[StateTest]

    val result = partitionToSet(events, states)

    val expectedResult = sc.parallelize(
      Seq(
        // Subgraph 1
        (Set(
          new EventTest("A", "B"),
          new EventTest("B", "C"),
          new EventTest("B", "D")
        ),
          Set.empty[StateTest]),
        // // Subgraph 2
        (Set(new EventTest("E", "F")),
          Set.empty[StateTest])
    ))

    assertRDDEquals(expectedResult, result)

  }

  it should "return grouped events and states for a simple input" in {
    //
    //  A* --> B* --> C
    //         |  -->  D
    //
    //  E  --> F*
    val events = sc.parallelize(
        Seq(
            // subgraph 1
            new EventTest("A", "B"),
            new EventTest("B", "C"),
            new EventTest("B", "D"),
            // subgraph 2
            new EventTest("E", "F")
        )
    )
    val states = sc.parallelize(
        Seq(
            // subgraph 1
            new StateTest("A"),
            new StateTest("B"),
            // subgraph 2
            new StateTest("F")
        )
    )

    val result = partitionToSet(events, states)

    val expectedResult = sc.parallelize(
      Seq(
        // Subgraph 1
        (Set(
          new EventTest("A", "B"),
          new EventTest("B", "C"),
          new EventTest("B", "D")
        ),
          Set(
            new StateTest("A"),
            new StateTest("B")
          )),
        // // Subgraph 2
        (Set(new EventTest("E", "F")),
          Set(new StateTest("F")))
      ))

    assertRDDEquals(expectedResult, result)

  }

  it should "return grouped events with cyclic graphs" in {
    //
    //  A --> B --> C
    //  ^-----------|
    //
    //  D --> E
    //  ^-----|
    val events = sc.parallelize(
        Seq(
            // subgraph 1
            new EventTest("A", "B"),
            new EventTest("B", "C"),
            new EventTest("C", "A"),
            // subgraph 2
            new EventTest("D", "E"),
            new EventTest("E", "D")
        ))

    val states = sc.emptyRDD[StateTest]

    val result = partitionToSet(events, states).map{
      case (itEvents, itStates) => (itEvents.toSet, itStates.toSet)
    }

    val expectedResult = sc.parallelize(
      Seq(
        // Subgraph 1
        (Set(
          new EventTest("A", "B"),
          new EventTest("B", "C"),
          new EventTest("C", "A")
        ),
          Set.empty[StateTest]),
        // // Subgraph 2
        (Set(
          new EventTest("D", "E"),
          new EventTest("E", "D")
        ),
          Set.empty[StateTest])
      ))

    assertRDDEquals(expectedResult, result)

  }

  it should "return grouped events with non-mono-oriented graphs" in {
    //
    //  A --> B --> C --> D
    //  |           |---> E
    //  |                 |
    //  |---> F --> G <---|
    //
    //  H --> I <-- J
    val events = sc.parallelize(
        Seq(
            // subgraph 1
            new EventTest("A", "B"),
            new EventTest("B", "C"),
            new EventTest("C", "D"),
            new EventTest("C", "E"),
            new EventTest("A", "F"),
            new EventTest("F", "G"),
            new EventTest("E", "G"),
            // subgraph 2
            new EventTest("H", "I"),
            new EventTest("J", "I")
        ))

    val states = sc.emptyRDD[StateTest]

    val result = partitionToSet(events, states)

    val expectedResult = sc.parallelize(
      Seq(
        // Subgraph 1
        (Set(
          new EventTest("A", "B"),
          new EventTest("B", "C"),
          new EventTest("C", "D"),
          new EventTest("C", "E"),
          new EventTest("A", "F"),
          new EventTest("F", "G"),
          new EventTest("E", "G")
        ),
          Set.empty[StateTest]),
        // // Subgraph 2
        (Set(
          new EventTest("H", "I"),
          new EventTest("J", "I")
        ),
          Set.empty[StateTest])
      ))

    assertRDDEquals(expectedResult, result)

  }

}
