package org.wikimedia.analytics.refinery.spark.utils

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import com.datastax.spark.connector.cql.{AuthConf, AuthConfFactory, DefaultAuthConfFactory, PasswordAuthConf}
import com.datastax.spark.connector.util.ConfigParameter
import org.apache.hadoop.conf.Configuration
import org.wikimedia.analytics.refinery.tools.LogHelper
import org.apache.commons.io.IOUtils


/**
 * Provides custom authentication configuration by implementing
 * [[com.datastax.spark.connector.cql.AuthConfFactory AuthConfFactory]].
 *
 * It reads password from a text file when a file is
 * passed as input to the 'spark.cassandra.auth.passwordfilepath'
 * option present in [[org.apache.spark.SparkConf SparkConf]]
 *
 * To use this custom code, the value of the
 * 'spark.cassandra.auth.conf.factory' option in
 * [[org.apache.spark.SparkConf SparkConf]] should be
 * set to the full name of this custom AuthConfFactory i.e.
 * org.wikimedia.analytics.refinery.spark.utils.WmfCassandraAuthConfFactory
 */
class WmfCassandraAuthConfFactory extends AuthConfFactory with LogHelper {
    val customReferenceSection = "Custom Authentication Parameters"
    val PasswordFilePathParam = ConfigParameter[Option[String]](
        //The name of the PasswordFilePathParam, "spark.cassandra.auth.passwordfilepath",
        //needs to be lowercase to allow parameter checks be successful.
        name = "spark.cassandra.auth.passwordfilepath",
        section = customReferenceSection,
        default = None,
        description = """password file path containing password for authentication""")

    /**
     * returns password inside a text password file as a string.
     * It takes the password file path as input.
     * It checks to confirm:
     * - if password file path exists.
     * - if password file path is a file.
     * @param passwordPath path of password file of string type
     * @return password in the file as string
     */
    def readPassword(passwordPath: String): String = {

        val hadoopConf: Configuration = new Configuration()
        val passwordFilePath = new Path(passwordPath)
        val fs: FileSystem = passwordFilePath.getFileSystem(hadoopConf)
        // perform necessary checks on the password file path.
        if (!fs.exists(passwordFilePath)){
            log.error(s"Password file does not exists at provided path ${passwordFilePath.toString}.")
        } else if (fs.isDirectory(passwordFilePath)){
            log.error(s" Provided path ${passwordFilePath.toString} is a folder. It should be a file.")
        }

        // read password from the file.
        try {
            val passwordStream = fs.open(passwordFilePath)
            val cassandraPassword = IOUtils.toString(passwordStream, "UTF-8").trim()
            cassandraPassword
        }
        catch {
            case e: Exception =>
                log.error(e.getMessage + s" There was a problem reading the password file from $passwordPath")
                sys.exit(1)
        }

    }

    override val properties = DefaultAuthConfFactory.properties ++ Set(
        PasswordFilePathParam.name
    )

    def authConf(sparkConf: SparkConf): AuthConf = {
        val credentials = {
            for (username <- sparkConf.getOption(DefaultAuthConfFactory.UserNameParam.name);
                 cassandraPasswordFile <- sparkConf.getOption(PasswordFilePathParam.name)) yield {
                val newCassandraPassword = readPassword(cassandraPasswordFile)
                (username, newCassandraPassword)
            }
        }

        credentials match {
            case Some((user, password)) => PasswordAuthConf(user: String, password)
            case None => DefaultAuthConfFactory.authConf(sparkConf)
        }
    }
}
