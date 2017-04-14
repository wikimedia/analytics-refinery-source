package org.wikimedia.analytics.refinery.job.mediawikihistory.utils

import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{SparkSession, Row, SQLContext}

import scala.reflect.ClassTag

/**
  * WARNING !
  * To use this class you need to set the checkpoint directory of your spark context
  */

/**
  * [[RowKeyFormat]] trait to switch keys from
  * RDD to dataframe and back.
  */
trait RowKeyFormat[T] {
  val struct: StructType
  def toRow(k: T): Row
  def toKey(r: Row): T
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
  *
  * For internal reasons (generating unique IDs for vertices)
  * an implicit [[Ordering]] is needed for the key type [[T]].
  *
  * Also, since this class uses Spark (RDD and GraphFrames),
  * implicit [[ClassTag]] need to be defined for manipulated types (T, E, V).
  */
class SubgraphPartitioner[T, E <: Edge[T], V <: Vertex[T]](spark: SparkSession, rowKeyFormatter: RowKeyFormat[T])(
    implicit cmp: Ordering[T],
    cTagT: ClassTag[T],
    cTagE: ClassTag[E],
    cTagS: ClassTag[V]
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
      .distinct
      .sortBy(e => e)(cmp, cTagT) // Ensure consistency if zipping happens more than once because of RDD re-computation
      .zipWithUniqueId // RDD[(key, id)]
      .cache()

    val verticesDF = spark.createDataFrame(
      verticesRdd.map { case (key, id) => Row.fromTuple((id, rowKeyFormatter.toRow(key)))},
      StructType(Seq(StructField("id", LongType, nullable = false), StructField("key", rowKeyFormatter.struct, nullable = false)))
    ).cache()

    val edgesRdd = events
      .map(e => (e.fromKey, e.toKey)) // RDD[(fromKey, toKey)]
      .distinct
      .keyBy(_._1) // RDD[(fromKey, (fromKey, toKey))]
      .join(verticesRdd.keyBy(_._1)) // RDD[(fromKey, ((fromKey, toKey), (key-fromKey, id-fromKey)))]
      .map(e => (e._2._1._2, e._2._2._2)) // RDD[(toKey, id-fromKey))]
      .join(verticesRdd.keyBy(_._1)) //RDD[(toKey, (id-fromKey, (key-toKey, id-toKey)))]
      .map(e => (e._2._1, e._2._2._2)) // RDD[(id-fromKey, id-toKey)]

    val edgesDF = spark.createDataFrame(edgesRdd.map(Row.fromTuple(_)),
      StructType(Seq(StructField("src", LongType, nullable = false), StructField("dst", LongType, nullable = false)))
    ).cache()

    GraphFrame(verticesDF, edgesDF)

  }

  /**
    * Re-associate events to their related subgraphs,
    * ensuring any event is only ever associated to one subgraph
    */
  private def extractEventsGroups(
      events: RDD[E],
      connectedComponents: RDD[(T, Long)]
  ): RDD[(Long, E)] = {
    log.info("Translating connected components to partitioned events")

    val eventsWithGroupingIds = events
      .keyBy(_.fromKey) // RDD[(fromKey, event)]
      .join(connectedComponents) // RDD[(fromKey, (event, groupId-fromKey))]
      .map(_._2) // RDD[(event, groupId-fromKey)]
      .keyBy(_._1.toKey) // RDD[(toKey, (event, groupId-fromKey))]
      .join(connectedComponents) // RDD[(toKey, ((event, groupId-fromKey), groupId-toKey))]
      .map(e => (e._2._1._1, (e._2._1._2, e._2._2))) // RDD[(event, (groupId-fromKey, groupId-toKey))]
      .cache()

    // Check events relate to only one subgraph
    // This is true if groupId-fromKey == groupId-toKey for every event
    log.info("Checking partitioned events correctness")
    assert(eventsWithGroupingIds.filter(e => e._2._1 != e._2._2).count == 0)

    // We know groupId-fromKey == groupId-toKey so we only return one of them
    eventsWithGroupingIds.map(e => (e._2._1, e._1)) // RDD[(groupId, event)]
  }

  /**
    * Associate states to their related subgraphs (if any),
    * ensuring we have then same number of states before and after joining.
    */
  private def extractStatesGroups(
      states: RDD[V],
      connectedComponents: RDD[(T, Long)]
  ): RDD[(Long, V)] = {
    log.info("Translating connected components to partitioned states")

    val statesWithGroupingIds = states
      .keyBy(_.key) // RDD[(key, state)]
      .leftOuterJoin(connectedComponents) // RDD[(key, (state, Option[(groupId)]))]
      .sortByKey() // Ensure consistency if zipping happens more than once because of RDD re-computation
      .zipWithIndex() // RDD[((key, (state, Option[(groupId)])), fakeId)]
      .map { // Trick to prevent assigning all standalone states in same partition
        case ((k, (s, Some(g))), idx) => (g, s) // RDD[(groupId, state)]
        case ((k, (s, None)), idx) => (-idx, s) // RDD[(-fakeId, state)]
      }
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

    val eventsGroups = extractEventsGroups(events, connectedComponentsRdd) // RDD[(groupId, event)]
    val statesGroups = extractStatesGroups(states, connectedComponentsRdd) // RDD[(groupId, state)]

    val partitionedRdd = eventsGroups
      .groupByKey() // RDD[(groupId, Iterable[event])]
      .fullOuterJoin(statesGroups.groupByKey()) // RDD[(groupId, (Option[Iterable[event]], Option[Iterable[state]]))]
      .map { // Transform undefined options to empty sequences
        case (_, (e, s)) =>
          (e.getOrElse(Seq.empty[E]), s.getOrElse(Seq.empty[V]))
      }
      .cache() // RDD[(Iterable[event], Iterable[state])]

    log.info("Graph partitioning jobs done")
    partitionedRdd
  }

}
