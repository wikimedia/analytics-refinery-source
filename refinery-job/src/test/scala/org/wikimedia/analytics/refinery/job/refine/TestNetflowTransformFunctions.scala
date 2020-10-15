package org.wikimedia.analytics.refinery.job.refine

import org.scalatest.{FlatSpec, Matchers}

class TestNetflowTransformFunctions extends FlatSpec with Matchers {

    it should "return the correct IP version" in {
        augment_netflow.ipVersion("12.34.56.78") should equal("IPv4")
        augment_netflow.ipVersion("2049:ed88:0:2f::") should equal("IPv6")
        augment_netflow.ipVersion("invalid IP") should equal("Unknown")
    }

    it should "return the correct wikimedia region" in {
        val matcher1 = augment_netflow.SerializableIpAddressMatcher("12.34.56.78/27")
        val matcher2 = augment_netflow.SerializableIpAddressMatcher("aa22:0:3500::10/55")
        val regionMap = Map(
            matcher1 -> "region1",
            matcher2 -> "region2"
        )
        val udf = augment_netflow.wikimediaRegion(regionMap)
        udf("12.34.56.79") should equal("region1")
        udf("12.34.0.0") should equal("Unknown")
        udf("aa22:0:3500::25") should equal("region2")
        udf("aa22:0:4700::30") should equal("Unknown")
    }

    it should "read the network to region map file correctly" in {
        val regionMap = augment_netflow.readNetworkToRegionMap(
            "src/test/resources/netflowRegionMap.yaml"
        )
        val matcher1 = augment_netflow.SerializableIpAddressMatcher("35.232.163.211/27")
        val matcher2 = augment_netflow.SerializableIpAddressMatcher("2034:0:456:cc00::/55")
        regionMap(matcher1) should equal("region1")
        regionMap(matcher2) should equal("region2")
    }
}
