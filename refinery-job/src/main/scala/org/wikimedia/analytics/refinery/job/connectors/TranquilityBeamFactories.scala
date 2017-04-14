package org.wikimedia.analytics.refinery.job.connectors

import com.metamx.common.Granularity
import com.metamx.tranquility.beam.{ClusteredBeamTuning, Beam}
import com.metamx.tranquility.druid.{SpecificDruidDimensions, DruidRollup, DruidBeams, DruidLocation}
import com.metamx.tranquility.spark.BeamFactory
import io.druid.data.input.impl.TimestampSpec
import io.druid.granularity.QueryGranularity
import io.druid.query.aggregation._
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.BoundedExponentialBackoffRetry
import org.joda.time.{DateTime, Period}


/**
  * This class defines configuration for a TranquilityBeamFactory.
  *
  * @param zookeeperHosts The comma-separated list of zookeeper hosts handling druid discovery data
  * @param zookeeperDruidDiscoveryPath The druid discovery path in zookeeper storage
  * @param zookeeperIndexingService The druid indexing service name in zookeeper
  * @param druidDatasource The druid datasource to index in
  * @param druidTimestamper A function extracting the timestamp in DateTime format out of each Map event
  * @param druidTimestampSpec The specification of the timestamp field in the event Map
  * @param druidDimensions The druid dimensions to index using the event Map keys
  * @param druidAggregators The druid aggregators to index using event map keys
  * @param druidQueryGranularity The druid query granularity to index
  * @param indexingSegmentGranularity The druid segment granularity to index
  * @param indexingWindowPeriod The window period to consider events to index - events late or forward
  *                             from this window will be discarded
  * @param indexingTaskPartitions The parallelization level of the indexing (total number of indexing tasks
  *                                 is indexingTaskPartitions * indexingTaskReplication)
  * @param indexingTaskReplication  The replication factor of the indexing (total number of indexing tasks
  *                                 is indexingTaskPartitions * indexingTaskReplication)
  */
case class TranquilityBeamConf(
                                // Zookeeper data to connect to Druid
                                zookeeperHosts: String,
                                zookeeperDruidDiscoveryPath: String,
                                zookeeperIndexingService: String,

                                // Druid datasource config
                                druidDatasource: String,
                                druidTimestamper: Map[String, Any] => DateTime,
                                druidTimestampSpec: DruidTimestampSpecWrapper,
                                druidDimensions: IndexedSeq[String],
                                druidAggregators: Seq[DruidAggregatorFactoryWrapper],
                                druidQueryGranularity: DruidQueryGranularityWrapper,

                                // Druid indexing config
                                indexingSegmentGranularity: DruidGranularityWrapper,
                                indexingWindowPeriod: Period,
                                indexingTaskPartitions: Int,
                                indexingTaskReplication: Int
                              ) extends Serializable

/**
  * Wrapper for druid aggregator factories, to prevent Spark serialization issues.
  * Only provide simple aggregators for now (missing filter, historgram and javascript).
  */
abstract class DruidAggregatorFactoryWrapper extends Serializable {
  def getDruidAggregatorFactory: AggregatorFactory
}

case class Count(name: String) extends DruidAggregatorFactoryWrapper {
  def getDruidAggregatorFactory: AggregatorFactory = new CountAggregatorFactory(name)
}

case class LongSum(name: String, fieldName: String) extends DruidAggregatorFactoryWrapper {
  def getDruidAggregatorFactory: AggregatorFactory = new LongSumAggregatorFactory(name, fieldName)
}
case class LongMax(name: String, fieldName: String) extends DruidAggregatorFactoryWrapper {
  def getDruidAggregatorFactory: AggregatorFactory = new LongMaxAggregatorFactory(name, fieldName)
}
case class LongMin(name: String, fieldName: String) extends DruidAggregatorFactoryWrapper {
  def getDruidAggregatorFactory: AggregatorFactory = new LongMinAggregatorFactory(name, fieldName)
}

case class DoubleSum(name: String, fieldName: String) extends DruidAggregatorFactoryWrapper {
  def getDruidAggregatorFactory: AggregatorFactory = new DoubleSumAggregatorFactory(name, fieldName)
}
case class DoubleMax(name: String, fieldName: String) extends DruidAggregatorFactoryWrapper {
  def getDruidAggregatorFactory: AggregatorFactory = new DoubleMaxAggregatorFactory(name, fieldName)
}
case class DoubleMin(name: String, fieldName: String) extends DruidAggregatorFactoryWrapper {
  def getDruidAggregatorFactory: AggregatorFactory = new DoubleMinAggregatorFactory(name, fieldName)
}

/**
  * Wrappers for Druid granularities and TimestampSpec to prevent Spark serialization issues
  */
case class DruidGranularityWrapper(value: String) extends Serializable {
  def getGranularity: Granularity = Granularity.valueOf(value)
  def isValid: Boolean = try { getGranularity; true} catch { case e: java.lang.IllegalArgumentException => false}
}
case class DruidQueryGranularityWrapper(value: String) extends Serializable {
  def getGranularity: QueryGranularity = QueryGranularity.fromString(value)
  def isValid: Boolean = try { getGranularity; true} catch { case e: java.lang.IllegalArgumentException => false}
}
case class DruidTimestampSpecWrapper(column: String, format: String, missingValue: DateTime) extends Serializable {
  def getTimestampSpec: TimestampSpec = new TimestampSpec(column, format, missingValue)
}


/**
  * This class is to be used to create tranquility beams indexing spark Map[String, Any] streams:
  *   import com.metamx.tranquility.spark.BeamRDD._
  *   stream.foreachRDD(_.propagate(new TranquilityBeamFactory(conf)))
  *
  * Warning: This class creates one beam per object of the class. If you want one beam per JVM
  * (recommended if you use a single type of BeamFactory accross multiple spark threads), use
  * TranquilitySingletonBeamFactory.
  *
  * @param conf The TranquilityBeamConf to be used to index
  */
class TranquilityBeamFactory(conf: TranquilityBeamConf) extends BeamFactory[Map[String, Any]] {
  // Return a singleton, so the same connection is shared across all tasks in the same JVM.

  private var BeamInstance: Option[Beam[Map[String, Any]]] = None

  protected def createBeam: Beam[Map[String, Any]] = {
    // Tranquility uses ZooKeeper (through Curator framework) for coordination.
    val curator = CuratorFrameworkFactory.newClient(
      conf.zookeeperHosts,
      new BoundedExponentialBackoffRetry(100, 3000, 5)
    )
    curator.start()

    val druidLocation: DruidLocation = DruidLocation.create(conf.zookeeperIndexingService, conf.druidDatasource)

    DruidBeams
      .builder(conf.druidTimestamper)
      .timestampSpec(conf.druidTimestampSpec.getTimestampSpec)
      .curator(curator)
      .discoveryPath(conf.zookeeperDruidDiscoveryPath)
      .location(druidLocation)
      .rollup(DruidRollup(
        SpecificDruidDimensions(conf.druidDimensions),
        conf.druidAggregators.map(_.getDruidAggregatorFactory),
        conf.druidQueryGranularity.getGranularity))
      .tuning(
        ClusteredBeamTuning(
          segmentGranularity = conf.indexingSegmentGranularity.getGranularity,
          windowPeriod = conf.indexingWindowPeriod,
          partitions = conf.indexingTaskPartitions,
          replicants = conf.indexingTaskReplication
        )
      )
      .buildBeam()
  }

  def makeBeam: Beam[Map[String, Any]] = {
    this.synchronized(
      if (this.BeamInstance.isEmpty) {
        this.BeamInstance = Some(this.createBeam)
      }
    )
    this.BeamInstance.get
  }
}

/**
  * This class is to be used to create a singleton tranquility beams indexing spark Map[String, Any] streams:
  *   stream.foreachRDD(_.propagate(new TranquilitySingletonBeamFactory(conf)))
  *
  * Warning: This class creates one beam per JVM. This works only if you have a single TranquilityBeam for the job.
  * If you want more singleton beams, you need to manage yourself.
  *
  * @param conf The TranquilityBeamConf to be used to index
  */
class TranquilitySingletonBeamFactory(conf: TranquilityBeamConf) extends TranquilityBeamFactory(conf) {
  override def makeBeam: Beam[Map[String, Any]] = {
    TranquilitySingletonBeamFactory.synchronized(
      if (TranquilitySingletonBeamFactory.BeamInstance.isEmpty) {
        TranquilitySingletonBeamFactory.BeamInstance = Some(this.createBeam)
      }
    )
    TranquilitySingletonBeamFactory.BeamInstance.get
  }
}

object TranquilitySingletonBeamFactory {
  private var BeamInstance: Option[Beam[Map[String, Any]]] = None
}
