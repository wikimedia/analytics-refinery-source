package org.wikimedia.analytics.refinery.tools

import java.util.Properties
import scala.util.Try

object ProjectInfo {
  private lazy val properties: Properties = {
    val props = new Properties()
    Try {
      val stream = getClass.getResourceAsStream("/META-INF/maven/org.wikimedia.analytics.refinery.tools/refinery-tools/pom.properties")
      if (stream != null) {
        props.load(stream)
        stream.close()
      }
    }
    props
  }

  def version: String = properties.getProperty("version", "unknown-version")
}
