package org.wikimedia.analytics.refinery.job.refine

import java.io.FileInputStream
import java.net.{InetAddress, Inet4Address, Inet6Address, UnknownHostException}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat, expr, lit, split, udf, when}
import org.springframework.security.web.util.matcher.IpAddressMatcher
import org.wikimedia.analytics.refinery.core.LogHelper
import org.wikimedia.analytics.refinery.spark.sql.PartitionedDataFrame
import org.yaml.snakeyaml.Yaml

/**
 * Adds the following derived fields to the Netflow data set:
 *
 *   - as_name_src:  Autonomous system name of the source IP.
 *                   Extracted from ip_src using MaxMind ISP database.
 *                   Type: String
 *                   Example: 'Red de Sakma en Barcelona'
 *
 *   - as_name_dst:  Autonomous system name of the destination IP.
 *                   Extracted from ip_dst using MaxMind ISP database.
 *                   Type: String
 *                   Example: 'New York University'
 *
 *   - parsed_comms: Parsed version of the BGP communities field.
 *                   Extracted from comms.
 *                   Type: Array[String]
 *                   Example: ['14907:0', '14907:2', '14907:3']
 *
 *   - net_cidr_src: Network prefix of the src IP in CIDR notation.
 *                   Extracted from net_src and mask_src.
 *                   Type: String
 *                   Example: '123.456.78.90/21'
 *
 *   - net_cidr_dst: Network prefix of the dst IP in CIDR notation.
 *                   Extracted from net_dst and mask_dst.
 *                   Type: String
 *                   Example: '2003:df6:e200::5:a/128'
 *
 *   - ip_version    Version of the IP protocol (IPv4, IPv6).
 *                   Extracted from ip_src.
 *                   Type: String
 *                   Example: 'IPv6'
 *
 *   - region:       Wikimedia region/site (esams, eqiad, ulsfo, eqsin, codfw).
 *                   Extracted from peer_ip_src using a network to region map.
 *                   Type: String
 *                   Example: 'eqiad'
 */
object augment_netflow extends LogHelper {

    // This file should contain a map of network prefixes and their
    // respective regions/sites (eqiad, esams, ...). It is expected
    // to be passed to the Refine job with the --files argument.
    val networkToRegionPath = "network_region_config.yaml"

    /**
     * Serializable wrapper for IpAddressMatcher.
     * IpAddressMatcher is not serializable, so we can not initialize it from
     * the driver and then use it in the worker. We have to initialize it in
     * the worker, but we want to initialize it only once, as opposed of every
     * time we have to match an IP.
     */
    case class SerializableIpAddressMatcher(
        networkPrefix: String
    ) {
        @transient
        private var matcher: IpAddressMatcher = null.asInstanceOf[IpAddressMatcher]

        def matches(ip: String): Boolean = {
            if (matcher == null) {
                matcher = new IpAddressMatcher(networkPrefix)
            }
            matcher.matches(ip)
        }
    }

    /**
     * Applies the transform function to the given PartitionedDataFrame depending on its table.
     * The netflow and network_flows_internal data is very similar, and adding some custom logic
     * at data augmentation step allows us not to add a new refine job.
     * Ideally this will at some point be stored in an API accessible way, allowing to remove
     * the hard-coded switch here.
     */
    def apply(partDf: PartitionedDataFrame): PartitionedDataFrame = {
        val networkToRegionMap = readNetworkToRegionMap(networkToRegionPath)
        val augmentedDf = partDf.partition.table match {
            case "netflow" => augmentNetflowDataFrame(partDf.df, networkToRegionMap)
            case "network_flows_internal" => augmentNetworkFlowsInternalDataFrame(partDf.df, networkToRegionMap)
            case _ => throw new IllegalArgumentException(s"Unexpected table in augment_netflow: ${partDf.partition.table}")
        }

        PartitionedDataFrame(augmentedDf, partDf.partition)
    }

    /**
     * Augments the given Netflow DataFrame with the new fields.
     */
    def augmentNetflowDataFrame(
        df: DataFrame,
        networkToRegionMap: Map[SerializableIpAddressMatcher, String]
    ): DataFrame = {
        // Declare UDFs to use in SparkSQL.
        val ipVersionUdf = udf(ipVersion _)
        val wikimediaRegionUdf = udf(wikimediaRegion(networkToRegionMap))
        df.sparkSession.sql(
            "CREATE OR REPLACE TEMPORARY FUNCTION get_isp_data AS " +
            "'org.wikimedia.analytics.refinery.hive.GetISPDataUDF'"
        )

        // SparkSQL statements for DataFrame transformation.
        df.
            withColumn("as_name_src",
                expr("get_isp_data(ip_src)['autonomous_system_organization']")
            ).
            withColumn("as_name_dst",
                expr("get_isp_data(ip_dst)['autonomous_system_organization']")
            ).
            withColumn("parsed_comms",
                when(col("comms") === "", Array[String]()).
                otherwise(split(col("comms"), "_"))
            ).
            withColumn("net_cidr_src",
                concat(col("net_src"), lit("/"), col("mask_src"))
            ).
            withColumn("net_cidr_dst",
                concat(col("net_dst"), lit("/"), col("mask_dst"))
            ).
            withColumn("ip_version",
                ipVersionUdf(col("ip_src"))
            ).
            withColumn("region",
                wikimediaRegionUdf(col("peer_ip_src"))
            )
    }

    /**
      * Augments the given network internal flow DataFrame with the new fields.
      */
    def augmentNetworkFlowsInternalDataFrame(
        df: DataFrame,
        networkToRegionMap: Map[SerializableIpAddressMatcher, String]
    ): DataFrame = {
        // Declare UDFs to use in SparkSQL.
        val ipVersionUdf = udf(ipVersion _)
        val wikimediaRegionUdf = udf(wikimediaRegion(networkToRegionMap))

        // SparkSQL statements for DataFrame transformation.
        df.
          withColumn("ip_version",
              ipVersionUdf(col("ip_src"))
          ).
          withColumn("region",
              wikimediaRegionUdf(col("peer_ip_src"))
          )
    }

    /**
     * Reads the network to region map from the config file into a map object.
     * The expected file format is:
     *     <regionNameA>:
     *         - <networkPrefix1>
     *         - <networkPrefix2>
     *     <regionNameB>:
     *         - <networkPrefix3>
     *         - <networkPrefix4>
     *     ...
     * Where <regionName> is like: eqiad, esams, ulsfo...
     * And <networkPrefix> is like: 123.456.78.90/21 or 12:345:67:890::/64
     * The output map format is:
     *     Map(
     *         <SerializableIpAddressMatcher1> -> <regionNameA>,
     *         <SerializableIpAddressMatcher2> -> <regionNameA>,
     *         <SerializableIpAddressMatcher3> -> <regionNameB>,
     *         <SerializableIpAddressMatcher4> -> <regionNameB>,
     *         ...
     *     )
     * SerializableIpAddressMatchers will be used to determine whether
     * given IPs belong to each region.
     */
    def readNetworkToRegionMap(
        file_path: String
    ): Map[SerializableIpAddressMatcher, String] = {
        type MapType = java.util.Map[String, Array[String]]
        type EntryType = java.util.Map.Entry[String, Array[String]]
        type NetworkList = java.util.ArrayList[String]

        val stream = new FileInputStream(file_path)
        // All the casting (asInstanceOf) is needed
        // to convert Java objects to Scala values.
        val mapFromFile = new Yaml().load(stream).asInstanceOf[MapType]
        mapFromFile.entrySet.toArray.flatMap{ case entry =>
                val region = entry.asInstanceOf[EntryType].getKey.asInstanceOf[String]
                val networks = entry.asInstanceOf[EntryType].getValue.asInstanceOf[NetworkList]
                networks.toArray.map{ case network =>
                    (SerializableIpAddressMatcher(network.asInstanceOf[String]), region)
                }
        }.toMap
    }

    /**
     * Returns the version of the given IP: either 'IPv6' or 'IPv4'.
     * If the IP can not be parsed, returns 'Unknown'.
     */
    def ipVersion(
        ip: String
    ): String = {
        val address = try {
            Some(InetAddress.getByName(ip))
        } catch {
            case e: UnknownHostException => None
        }
        address match {
            case Some(a) if (a.isInstanceOf[Inet4Address]) => "IPv4"
            case Some(a) if (a.isInstanceOf[Inet6Address]) => "IPv6"
            case _ => "Unknown"
        }
    }

    /**
     * Returns a function to be used as a SparkSQL UDF.
     * The parameter networkToRegionMap is thus added to the UDF's closure.
     * The returned function accepts an exporter IP (String) extracted from
     * a request made to Wikimedia servers, and returns the Wikimedia region
     * that the IP belongs to, using the networkToRegionMap.
     * For example: '91.198.174.226' => 'esams'.
     */
    def wikimediaRegion(
        networkToRegionMap: Map[SerializableIpAddressMatcher, String]
    ): (String) => String = {
        (ip) => {
            // This value will be Some(matcher), if a matcher is found;
            // Or None, if either no matcher is found or an Exception
            // is thrown (IP parsing error).
            val network = try {
                networkToRegionMap.keys.find{ case matcher =>
                    matcher.matches(ip)
                }
            } catch {
                case e: IllegalArgumentException => None
            }
            network match {
                case Some(k) => networkToRegionMap(k)
                case _ => "Unknown"
            }
        }
    }
}
