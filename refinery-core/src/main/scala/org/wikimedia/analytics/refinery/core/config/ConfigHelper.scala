package org.wikimedia.analytics.refinery.core.config

import scala.util.matching.Regex
import com.github.nscala_time.time.Imports._
import org.joda.time.format.{DateTimeFormatter, ISODateTimeFormat}
import profig._
import cats.syntax.either._
import io.circe.CursorOp.DownField
import io.circe.{Decoder, DecodingFailure}
import org.apache.hadoop.fs.Path

import scala.annotation.tailrec
import scala.language.experimental.macros
import scala.reflect.macros.blackbox

/**
  * Extend this trait to get automatic mapping from properties config file and args
  * properties to a case class.  It includes handy implicits for automatically mapping
  * from Strings to higher types, like Regexes, DateTimes, Maps, etc.
  *
  * If you need an implicit mapping to a type not defined here, you can define
  * one in scope of your caller.
  *
  * Usage:
  *
  * object MyApp extends ConfigHelper {
  *     case class Config(path: String, dt: DateTime)
  *
  *     val propertiesFile: String = "myapp.properties"
  *
  *     def main(args: Array[String]): Unit = {
  *         val config = configure[Config](Array(propertiesFile), args)
  *         // params will be a new Params instance loaded from configs found
  *         // in myapp.properties, with overrides from the CLI opts like
  *         // --path "my path override" --dt 2018-10-01T00:00:00
  *
  *         // Or, configure from --config_file args from args only.
  *         // myapp.properties will be loaded first, then remaining args will be merged over.
  *         // ... --config_file", "myapp.properties" --path "my path override" --dt 2018-10-01T00:00:00
  *         val config = configureArgs[Config](args)
  *     }
  *
  * This trait also includes methods to nicely format a help string, given a usage example and
  * a map of properties to doc strings:
  *
  *     val usage = "MyApp --path CUSTOM_PATH --dt CUSTOM_DT"
  *     val propertiesDoc = ListMap[String, String] = ListMap(
  *         "path" -> "Path to file",
  *         "dt"   -> "date time to use, yyyy-MM-dd'T'HH:mm:ss format"
  *     )
  *     ...
  *     if (args.contains('--help')) {
  *         println(help(usage, propertiesDoc))
  *         sys.exit(0)
  *     }
  *
  * It also includes a nice prettyPrint function if you want to log the final loaded configs:
  *     ...
  *     val config = configureArgs[Config](args)
  *     println("Loaded configuration:\n" + prettyPrint(config))
  *
  */
trait ConfigHelper {
    implicit val decodeOptionString: Decoder[Option[String]] = Decoder.decodeString.emap { s =>
        Either.catchNonFatal(Some(s)).leftMap(t =>
            throw new RuntimeException(s"Failed parsing '$s' into a string.", t)
        )
    }

    implicit val decodeSeqString: Decoder[Seq[String]] = Decoder.decodeString.emap { s =>
        Either.catchNonFatal(s.split(",").toSeq).leftMap(t =>
            throw new RuntimeException(s"Failed parsing '$s' into a seq string.", t)
        )
    }

    implicit val decodeInt: Decoder[Int] = Decoder.decodeString.emap { s =>
        Either.catchNonFatal(s.toInt).leftMap(t =>
            throw new RuntimeException(s"Failed parsing '$s' into a integer.", t)
        )
    }

    implicit val decodeOptionInt: Decoder[Option[Int]] = Decoder.decodeString.emap { s =>
        Either.catchNonFatal(Some(s.toInt)).leftMap(t =>
            throw new RuntimeException(s"Failed parsing '$s' into a integer.", t)
        )
    }

    implicit val decodeLong: Decoder[Long] = Decoder.decodeString.emap { s =>
        Either.catchNonFatal(s.toLong).leftMap(t =>
            throw new RuntimeException(s"Failed parsing '$s' into a long.", t)
        )
    }

    implicit val decodeOptionLong: Decoder[Option[Long]] = Decoder.decodeString.emap { s =>
        Either.catchNonFatal(Some(s.toLong)).leftMap(t =>
            throw new RuntimeException(s"Failed parsing '$s' into a long.", t)
        )
    }

    implicit val decodeDouble: Decoder[Double] = Decoder.decodeString.emap { s =>
        Either.catchNonFatal(s.toDouble).leftMap(t =>
            throw new RuntimeException(s"Failed parsing '$s' into a double.", t)
        )
    }

    implicit val decodeOptionDouble: Decoder[Option[Double]] = Decoder.decodeString.emap { s =>
        Either.catchNonFatal(Some(s.toDouble)).leftMap(t =>
            throw new RuntimeException(s"Failed parsing '$s' into a double.", t)
        )
    }

    // implicit conversion from k1:v1,k2:v2 string to a Map
    implicit val decodeMapString: Decoder[Map[String, String]] = Decoder.decodeString.emap { s =>
        /**
          * Converts a string of the form k1:v1,k2:v2 to a Map(k1 -> v1, k2 -> v2).
          */
        def stringToMap(str: String): Map[String, String] = {
            val kvPairs = str.split(",")
            kvPairs.toSeq.foldLeft[Map[String, String]](Map()) { (map, kvString) =>
                kvString.split(":") match {
                    case Array(key, value) => map ++ Map(key -> value)
                    case _ => throw new RuntimeException(
                        s"Failed parsing '$kvString' into a Map entry. Should be of the form key:value."
                    )
                }
            }
        }

        Either.catchNonFatal(stringToMap(s)).leftMap(t =>
             throw new RuntimeException(
                 s"Failed parsing '$s' into a Map. Must provide a comma separated list of key:value pairs.", t
             )
        )
    }

    // implicit conversion from string to Regex
    implicit val decodeRegex: Decoder[Regex] = Decoder.decodeString.emap { s =>
        Either.catchNonFatal(s.r).leftMap(t =>
            throw new RuntimeException(s"Failed parsing '$s' into a regex.", t)
        )
    }

    // implicit conversion from string to Option[Regex]
    implicit val decodeOptionRegex: Decoder[Option[Regex]] = Decoder.decodeString.emap { s =>
        Either.catchNonFatal(Some(s.r)).leftMap(t =>
            throw new RuntimeException(s"Failed parsing '$s' into a regex.", t)
        )
    }

    // implicit conversion from string to DateTimeFormatter
    implicit val decodeDateTimeFormatter: Decoder[DateTimeFormatter] = Decoder.decodeString.emap { s =>
        Either.catchNonFatal(DateTimeFormat.forPattern(s)).leftMap(t =>
            throw new RuntimeException(s"Failed parsing '$s' into a DateTimeFormatter", t)
        )
    }

    // implicit conversion from string to hadoop.fs.Path
    implicit def decodeHadoopFsPath: Decoder[Path] = Decoder.decodeString.emap {s =>
        Either.catchNonFatal(new Path(s)).leftMap(p =>
            throw new RuntimeException(s"Failed parsing '$s' into a hadoop Path", p)
        )
    }

    // Support implicit DateTime conversion from string to DateTime
    // The opt can either be given in integer hours ago, or
    // as a ISO-8601 formatted date time.
    private final val isoDateTimeFormatter = ISODateTimeFormat.dateTimeParser()
    implicit val decodeDateTime: Decoder[DateTime] = Decoder.decodeString.emap { s =>
        Either.catchNonFatal({
            {
                if (s.forall(Character.isDigit)) DateTime.now - s.toInt.hours
                else DateTime.parse(s, isoDateTimeFormatter)
            }.withZone(DateTimeZone.UTC)
        }).leftMap(t => throw new RuntimeException(
            s"Failed parsing '$s' into a DateTime. Must provide either an integer hours ago, " +
             "or a yyyy-MM-dd'T'HH:mm:ssZ formatted string."
        ))
    }


    /**
      * Returns a nicely formatted help message string.
      *
      * @param usage Free form usage heading and examples.  This will be at the
      *              beginning of your help message.
      * @param propertiesDoc Map of configuration property name to documentation.
      * @return
      */
    def help(usage: String, propertiesDoc: Map[String, String]): String = {
        usage.stripMargin + "\nConfiguration Properties:\n\n" + propertiesDoc
            // Do some prettifying of property docs
            .mapValues(_.stripMargin.replace("\n", "\n    ") + "\n")
            // tab the properties and their docs nicely.
            .map(t => s"  ${t._1}\n    ${t._2}").mkString("\n")
    }

    /**
      * Pretty prints a Scala value similar to its source representation.
      * Particularly useful for case classes.
      *
      * This is useful for printing out final ConfigHelper loaded configuration case classes.
      *
      * Taken from https://gist.github.com/carymrobbins/7b8ed52cd6ea186dbdf8
      *
      * @param a - The value to pretty print.
      * @param indentSize - Number of spaces for each indent.
      * @param maxElementWidth - Largest element size before wrapping.
      * @param depth - Initial depth to pretty print indents.
      * @return
      */
    def prettyPrint(a: Any, indentSize: Int = 2, maxElementWidth: Int = 30, depth: Int = 0): String = {
        val indent = " " * depth * indentSize
        val fieldIndent = indent + (" " * indentSize)
        val thisDepth = prettyPrint(_: Any, indentSize, maxElementWidth, depth)
        val nextDepth = prettyPrint(_: Any, indentSize, maxElementWidth, depth + 1)
        a match {
            // Make Strings look similar to their literal form.
            case s: String =>
                val replaceMap = Seq(
                    "\n" -> "\\n",
                    "\r" -> "\\r",
                    "\t" -> "\\t",
                    "\"" -> "\\\""
                )
                '"' + replaceMap.foldLeft(s) { case (acc, (c, r)) => acc.replace(c, r) } + '"'
            // For an empty Seq just use its normal String representation.
            case xs: Seq[_] if xs.isEmpty => xs.toString()
            case xs: Seq[_] =>
                // If the Seq is not too long, pretty print on one line.
                val resultOneLine = xs.map(nextDepth).toString()
                if (resultOneLine.length <= maxElementWidth) return resultOneLine
                // Otherwise, build it with newlines and proper field indents.
                val result = xs.map(x => s"\n$fieldIndent${nextDepth(x)}").toString()
                result.substring(0, result.length - 1) + "\n" + indent + ")"
            // Product should cover case classes.
            case p: Product =>
                val prefix = p.productPrefix
                // We'll use reflection to get the constructor arg names and values.
                val cls = p.getClass
                val fields = cls.getDeclaredFields.filterNot(_.isSynthetic).map(_.getName)
                val values = p.productIterator.toSeq
                // If we weren't able to match up fields/values, fall back to toString.
                if (fields.length != values.length) return p.toString
                fields.zip(values).toList match {
                    // If there are no fields, just use the normal String representation.
                    case Nil => p.toString
                    // If there is just one field, let's just print it as a wrapper.
                    case (_, value) :: Nil => s"$prefix(${thisDepth(value)})"
                    // If there is more than one field, build up the field names and values.
                    case kvps =>
                        val prettyFields = kvps.map { case (k, v) => s"$fieldIndent$k = ${nextDepth(v)}" }
                        // If the result is not too long, pretty print on one line.
                        val resultOneLine = s"$prefix(${prettyFields.mkString(", ")})"
                        if (resultOneLine.length <= maxElementWidth) return resultOneLine
                        // Otherwise, build it with newlines and proper field indents.
                        s"$prefix(\n${prettyFields.mkString(",\n")}\n$indent)"
                }
            // If we haven't specialized this type, just use its toString.
            case _ => a.toString
        }
    }



    // NOTE: It would be much nicer if we didn't have to have 2 different macros and 2 different
    // functions to support what would normally be done with default args.  But Macros can't use
    // default args, and we can't call a function that uses a macro from within the same
    // package/module that defines the macro, as macros require a second pass of compilation.
    // See also: https://docs.scala-lang.org/overviews/macros/overview.html

    /**
      * Load configs from files and args and convert matched configs into case class T
      *
      * @param files Array of config files to load
      * @param args  Array of CLI opt args, e.g. Array("--opt1", "myvalue", ...)
      * @tparam T    Should be a case class that matched configs should be loaded into.
      *              Config names should match the declared case class properties.
      * @return
      */
    def configure[T](files: Array[String], args: Array[String]): T = macro ConfigHelperMacros.configureImpl[T]


    /**
      * Load configs first from any config files found in --config_file args, and
      * then from the remaining args.
      *
      * @param args
      * @tparam T
      * @return
      */
    def configureArgs[T](args: Array[String]): T = macro ConfigHelperMacros.configureArgsImpl[T]

}

/**
 * Thrown when a required config is not provided.
 */
class ConfigHelperException(message: String) extends Exception(message)


/**
  * Contains a macro for loading config files and args into a case class. Without this macro
  * the case class would have to be defined in scope of the Profig.as[...] call.  This
  * macro allows us to hide away the Profig implementation of ConfigHelper, exposing only
  * the configure[T](files, args) interface.
  */
object ConfigHelperMacros {

    /**
      * configureArgsImpl will look for this in args to use for finding config file paths.
      */
    final val configFileOpt: String = "--config_file"


    /**
      * Macro implementation that allows us to generate an implicit mapping from
      * Profig configuration to T which should be a case class.  This should be used
      * as a macro only.  Profig will load configs, instantiate T with them, and then
      * clear globally loaded Profig configs.
      */
    def configureImpl[T]
        (c: blackbox.Context)
        (files: c.Expr[Array[String]], args: c.Expr[Array[String]])
        (implicit t: c.WeakTypeTag[T]): c.Expr[T] =
    {
        import c.universe._

        c.Expr[T](q"""
            import profig._
            ConfigHelperMacros.loadProfig($files, $args)
            val p = try {
                Profig.as[$t]
            } catch {
                case e: RuntimeException =>
                    // If this RuntimeException was caused by a missing required config, throw
                    // a ConfigHelperException, else just rethrow the original exception.
                    val missingConfig = ConfigHelperMacros.extractMissingConfig(e).getOrElse(throw e)
                    throw new ConfigHelperException(
                        "Failed loading configuration: " + missingConfig + " is required but was not provided"
                    )
            }
            Profig.clear()
            p
        """)
    }


    /**
      * Macro implementation that allows us to generate an implicit mapping from
      * Profig configuration to T which should be a case class.  This should be used
      * as a macro only.  Profig will load configs, instantiate T with them, and then
      * clear globally loaded Profig configs.  This implementation looks for the configFileOpt
      * in args and expects its value to be a config properties file.
      */
    def configureArgsImpl[T]
        (c: blackbox.Context)
        (args: c.Expr[Array[String]])
        (implicit t: c.WeakTypeTag[T]): c.Expr[T] =
    {
        import c.universe._

        c.Expr[T](q"""
            import profig._
            val (f, a) = ConfigHelperMacros.extractOpts(ConfigHelperMacros.configFileOpt)($args)
            ConfigHelperMacros.loadProfig(f, a)
            val p = try {
                Profig.as[$t]
            } catch {
                case e: RuntimeException =>
                    // If this RuntimeException was caused by a missing required config, throw
                    // a ConfigHelperException, else just rethrow the original exception.
                    val missingConfig = ConfigHelperMacros.extractMissingConfig(e).getOrElse(throw e)
                    throw new ConfigHelperException(
                        "Failed loading configuration: " + missingConfig + " is required but was not provided"
                    )
            }
            Profig.clear()
            p
         """
        )
    }


    /**
     * If the RuntimeException was caused by Profig / Circe loading configs into
      * a case class without required parameters, the RuntimeException will have been
      * caused by a Circe DecodingFailure.  This returns an Option of the name of the
      * missing required field.
      * @param e
      * @return
      */
    def extractMissingConfig(e: RuntimeException): Option[String] = {
        e.getCause match {
            case DecodingFailure(_, history) =>
                history match {
                    case List(DownField(missingConfig), _*) => Some(missingConfig)
                    case _ => None
                }
            case _ => None
        }
    }


    /**
      * Given an Array of config files, use Profig to read them all in, merging configs from files
      * on the right over ones on the left. Then merge any overrides in the args Array over configs
      * from files.
      *
      * @param files Array of config files. Their FileType will be inferred from the file extension.
      * @param args Array args string from CLI.
      */
    def loadProfig(files: Array[String], args: Array[String]): Unit = {
        // Load all config files in order given, with the right merging over the left.
        Profig.load(files.map(profigLookupPath(_)):_*)

        // Merge any args over configs read from config files.
        Profig.merge(args)
    }


    /**
      * Return a ProfigLookupPath suitable for passing to Profig.load.  FileType must be
      * FileType.Properties.  YAML, JSON, etc. not supported.
      *
      * @param file     path to properties file
      * @param loadType Either LoadType.Merge or LoadType.Defaults. Default: Merge.  (Defaults will
      *                 override anything that Profig has already loaded.)
      * @return
      */
    private def profigLookupPath(file: String, loadType: LoadType = LoadType.Merge): ProfigLookupPath = {
        // This should work with any FileType, but I had trouble in tests getting different
        // FileTypes to merge properly.  Not sure why.
        ProfigLookupPath(file, FileType.Properties, LoadType.Merge)
    }


    /**
      * Given a CLI opt to match, this will extract all of the values in CLI args of that opt,
      * and return a tuple of the matched values and the remaining args without the opt and value.
      * If the opt has a comma separated value, it is assumed this is a multi value, and will be split on commas.
      *
      * Example:
      *     val args = Array("--verbose", "true", "--config_file", "file.yaml", "--nonya", "--config_file=p.properties,c.conf")
      *
      *     val (configFiles, remainingArgs) = extractOpts("--config_file")(args)
      *
      * @param optName Name of the CLI opt to extract
      * @param args    CLI args
      * @param matchedValues
      * @param unmatchedArgs
      * @return
      */
    @tailrec
    def extractOpts(optName: String)(
        args         : Array[String],
        matchedValues: Array[String] = Array.empty,
        unmatchedArgs: Array[String] = Array.empty
    ): (Array[String], Array[String]) = {
        args match {
            // Empty array, nothing left to do, return collected results.
            case Array() =>
                (matchedValues, unmatchedArgs)

            // At least one element left in args, check it and recurse
            case Array(opt, _*) =>

                // Get a tuple for the recurse call arguments:
                // - the args to continue recursion,
                // - any matched optName values
                // - and the collected unmatched args
                val (recurseArgs, newMatchedValues, newUnmatchedArgs) = {
                    // If opt is $optName=, then the value is after the =
                    if (opt.startsWith(s"$optName=")) {
                        (
                            // We got the value from the first arg, so drop it and recurse on the rest
                            args.tail,
                            // Append the value of opt after the =
                            matchedValues ++ opt.split("=", 2)(1).split(","),
                            unmatchedArgs
                        )
                    }
                    // If opt is optName and there is at least one more arg, then the opt value
                    // will be the next arg.
                    else if (opt == optName && args.tail.length > 0) {
                        (
                            // We can lose the first to args; args(0) is optName, args(1) is the value
                            args.drop(2),
                            // append the value at args(1)
                            matchedValues ++ args(1).split(","),
                            unmatchedArgs
                        )
                    }
                    // Else opt didn't match optName at all, skip it.
                    else
                        (
                            args.tail,
                            matchedValues,
                            // opt is args.head, collect it as unmatched.
                            unmatchedArgs :+ opt
                        )
                }

                extractOpts(optName)(recurseArgs, newMatchedValues, newUnmatchedArgs)
        }
    }
}
