package org.wikimedia.analytics.refinery.spark.utils

import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.reflect.ClassTag
import scala.util.Random

/**
  * WARNING !
  * To use this class you need to set the checkpoint directory of your spark context
  */

/**
  * [[RowKeyFormat]] trait to switch keys from
  * RDD to dataframe and back, with a function
  * providing grouping sets for stats gathering
  */
trait RowKeyFormat[T, S] {
  val struct: StructType
  def toRow(k: T): Row
  def toKey(r: Row): T
  def statsGroup(k: T): S
}

/**
  * [[Edge]] and [[Vertex]] traits
  * Defines functions to access objects keys
  */

trait Edge[T] {
  def fromKey: T
  def toKey: T
}

trait Vertex[T] {
  def key: T
}


/**
  * Generic subgraph partitioner for RDDs of objects [[E]] and [[V]]
  * implementing [[Edge]] and [[Vertex]] traits over a key type [[T]].
  * Stats are computed of how many subgraph partitions are generated
  * for groups of type [[S]] (generated from key [[T]])
  *
  * For internal reasons (generating unique IDs for vertices)
  * an implicit [[Ordering]] is needed for the key type [[T]].
  *
  * Also, since this class uses Spark (RDD and GraphFrames),
  * implicit [[ClassTag]] need to be defined for manipulated types (T, S, E, V).
  */
class SubgraphPartitioner[T, S, E <: Edge[T], V <: Vertex[T]](
    spark: SparkSession,
    rowKeyFormatter: RowKeyFormat[T, S],
    subgraphPartitionSizesAccumulator: Option[MapAccumulator[S, Long]] = None
)(
    implicit cmp: Ordering[T],
    cTagS: ClassTag[S],
    cTagT: ClassTag[T],
    cTagE: ClassTag[E],
    cTagV: ClassTag[V]
) extends Serializable {

  import org.apache.log4j.Logger
  import org.apache.spark.rdd.RDD
  import org.graphframes.GraphFrame

  @transient
  lazy val log: Logger = Logger.getLogger(this.getClass)

  /**
    * Builds the events global graph
    *
    * States are not needed at that time since we are only interested
    * in edges (connected states). States will be linked to connected
    * events after subgraphs are computed
    */
  private def makeGraph(events: RDD[E]): GraphFrame = {
    log.info("Building graph from events RDD")

    val verticesRdd = events
      .flatMap(e => Seq(e.fromKey, e.toKey)) // RDD[key]
      .distinct()
      .sortBy(e => e)(cmp, cTagT) // Ensure consistency if zipping happens more than once because of RDD re-computation
      .zipWithUniqueId() // RDD[(key, id)]
      .cache()

    val verticesDF = spark.createDataFrame(
      verticesRdd.map { case (key, id) => Row.fromTuple((id, rowKeyFormatter.toRow(key)))},
      StructType(Seq(StructField("id", LongType, nullable = false), StructField("key", rowKeyFormatter.struct, nullable = false)))
    )

    val edgesRdd = events
      .map(e => (e.fromKey, e.toKey)) // RDD[(fromKey, toKey)]
      .distinct()
      .keyBy(_._1) // RDD[(fromKey, (fromKey, toKey))]
      .join(verticesRdd.keyBy(_._1)) // RDD[(fromKey, ((fromKey, toKey), (key-fromKey, id-fromKey)))]
      .map(e => (e._2._1._2, e._2._2._2)) // RDD[(toKey, id-fromKey))]
      .join(verticesRdd.keyBy(_._1)) //RDD[(toKey, (id-fromKey, (key-toKey, id-toKey)))]
      .map(e => (e._2._1, e._2._2._2)) // RDD[(id-fromKey, id-toKey)]

    val edgesDF = spark.createDataFrame(edgesRdd.map(Row.fromTuple(_)),
      StructType(Seq(StructField("src", LongType, nullable = false), StructField("dst", LongType, nullable = false)))
    )

    GraphFrame(verticesDF, edgesDF)

  }

  /**
    * Re-associate events to their related subgraphs,
    * ensuring any event is only ever associated to one subgraph
    */
  private def extractEventsGroups(
      events: RDD[E],
      connectedComponents: RDD[(T, Long)]
  ): RDD[((Long, S), E)] = {
    log.info("Translating connected components to partitioned events")

    val eventsWithGroupingIds = events
      .keyBy(_.fromKey) // RDD[(fromKey, event)]
      .join(connectedComponents) // RDD[(fromKey, (event, groupId-fromKey))]
      .map(t => (t._2._1, rowKeyFormatter.statsGroup(t._1), t._2._2)) // RDD[(event, statGroup, groupId-fromKey)]
      .keyBy(_._1.toKey) // RDD[(toKey, (event, groupId-fromKey))]
      .join(connectedComponents) // RDD[(toKey, ((event, statGroup, groupId-fromKey), groupId-toKey))]
      .map(e => (e._2._1._1, (e._2._1._3, e._2._2), e._2._1._2)) // RDD[(event, (groupId-fromKey, groupId-toKey), statGroup)]
      .cache()

    // Check events relate to only one subgraph
    // This is true if groupId-fromKey == groupId-toKey for every event
    log.info("Checking partitioned events correctness")
    assert(eventsWithGroupingIds.filter(e => e._2._1 != e._2._2).count == 0)

    // We know groupId-fromKey == groupId-toKey so we only return one of them (same for statGroup)
    eventsWithGroupingIds.map(e => ((e._2._1, e._3), e._1)) // RDD[(groupId, statGroup, event)]
  }

  /**
    * Associate states to their related subgraphs (if any),
    * ensuring we have then same number of states before and after joining.
    */
  private def extractStatesGroups(
      states: RDD[V],
      connectedComponents: RDD[(T, Long)]
  ): RDD[((Long, S), V)] = {
    log.info("Translating connected components to partitioned states")

    val statesWithGroupingIds = states
      .keyBy(_.key) // RDD[(key, state)]
      .leftOuterJoin(connectedComponents) // RDD[(key, (state, Option[(groupId)]))]
      .map(t => {// Trick to prevent assigning all standalone states in same partition
        val statGroup = rowKeyFormatter.statsGroup(t._1)
        t match {
          case (k, (s, Some(g))) =>
            ((g, statGroup), s) // RDD[((groupId, statGroup), state)]
          case (k, (s, None)) =>
            val fakeId: Long = -math.abs(Random.nextLong())
            ((fakeId, statGroup), s) // RDD[((-fakeId, statGroup), state)]
        }})
      .cache()

    // Check each state is joined to at most one groupId
    log.info("Checking partitioned states correctness")
    assert(statesWithGroupingIds.count == states.count)

    statesWithGroupingIds // RDD[(groupId, state)]
  }

  def run(events: RDD[E], states: RDD[V]): RDD[(Iterable[E], Iterable[V])] = {
    log.info("Starting graph partitioning jobs")

    val graph = makeGraph(events)
    log.info("Extracting graph connected components")
    val connectedComponentsRdd = graph
      .connectedComponents.run() // DF[id: Long, key: Row, component: Long]
      .rdd
      .map(r => (rowKeyFormatter.toKey(r.getStruct(1)), r.getLong(2))) // RDD[(KEY, groupId)]
      .cache()

    val eventsGroups = extractEventsGroups(events, connectedComponentsRdd) // RDD[((groupId, statGroup), event)]
    val statesGroups = extractStatesGroups(states, connectedComponentsRdd) // RDD[((groupId, statGroup), state)]

    events.unpersist()
    states.unpersist()

    val partitionedRdd = eventsGroups
      .groupByKey() // RDD[((groupId, statGroup), Iterable[event])]
      .fullOuterJoin(statesGroups.groupByKey()) // RDD[((groupId, statGroup), (Option[Iterable[event]], Option[Iterable[state]]))]
      .map(tuple => {
          // Transform undefined options to empty sequences and optionally add partition size to accumulator
          val ((_, statGroup), (e, s)) = tuple
          val newE = e.getOrElse(Seq.empty[E])
          val newS = s.getOrElse(Seq.empty[V])
          subgraphPartitionSizesAccumulator.foreach(_.add((statGroup, 1)))
          (newE, newS)
        }
      )
      .cache() // RDD[(Iterable[event], Iterable[state])]

    log.info("Graph partitioning jobs done")
    partitionedRdd
  }

}
