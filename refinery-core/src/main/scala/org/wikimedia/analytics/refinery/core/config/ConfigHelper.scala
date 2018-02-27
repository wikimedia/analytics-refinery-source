package org.wikimedia.analytics.refinery.core.config

import scala.util.matching.Regex
import com.github.nscala_time.time.Imports._
import org.joda.time.format.DateTimeFormatter

import profig._
import cats.syntax.either._
import io.circe.Decoder

import scala.language.experimental.macros
import scala.reflect.macros.blackbox

/**
  * Extend this trait to get automatic mapping from properties config file and args
  * properties to a case class.  It includes handy implicits for automatically mapping
  * from Strings to higher types, like Regexes, DateTimes, etc.
  *
  * If you need an implicit mapping to a type not defined here, you can define
  * one in scope of your caller.
  *
  * Usage:
  *
  * object MyApp extends ConfigHelper {
  *     case class Params(name: String, dt: DateTime)
  *
  *     val propertiesFile: String = "myapp.properties"
  *
  *     def main(args: Array[String]): Unit = {
  *         val params = configure[Params](Array(propertiesFile), args)
  *         // params will be a new Params instance loaded from configs found
  *         // in myapp.properties, with overrides from the CLI opts like
  *         // --name "my name override" --dt 2018-10-01T00:00:00
  *
  *         // Or, configure from --config_file args from args only.
  *         // myapp.properties will be loaded first, then remaining args will be merged over.
  *         // ... --config_file", "myapp.properties" --name "my name override" --dt 2018-10-01T00:00:00
  *         val params = configureArgs[Params](args)
  *     }
  *
  */
trait ConfigHelper {
    private val iso8601DateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss")

    // implicit conversion from string to Option[Regex]
    implicit val decodeOptionString: Decoder[Option[String]] = Decoder.decodeString.emap { s =>
        Either.catchNonFatal(Some(s)).leftMap(t =>
            throw new RuntimeException(s"Failed parsing '$s' into a string.", t)
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


    // implicit conversion from string to Boolean
    implicit val decodeBoolean: Decoder[Boolean] = Decoder.decodeString.emap { s =>
        val truthyMap: Map[String, Boolean] = Map(
            "true"  -> true,
            "yes"   -> true,
            "1"     -> true,
            "false" -> false,
            "no"    -> false,
            "0"     -> false
        )

        Either.catchNonFatal(truthyMap.getOrElse(
            s,
            throw new RuntimeException("Must be one of: " + truthyMap.keySet.mkString(", "))
        )).leftMap(t =>
            throw new RuntimeException(s"Failed parsing '$s' into a Boolean.", t)
        )
    }

    // implicit conversion from comma seperated string to Seq[String]
    implicit val decodeSeqString: Decoder[Seq[String]] = Decoder.decodeString.emap { s =>
        Either.catchNonFatal(s.split(",").toSeq).leftMap(t =>
            throw new RuntimeException(
                s"Failed parsing '$s' into a list. Must provide a comma separated list.", t
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

    // implicit conversionfrom string to DateTimeFormatter
    implicit val decodeDateTimeFormatter: Decoder[DateTimeFormatter] = Decoder.decodeString.emap { s =>
        Either.catchNonFatal(DateTimeFormat.forPattern(s)).leftMap(t =>
            throw new RuntimeException(s"Failed parsing '$s' into a DateTimeFormatter", t)
        )
    }

    // Support implicit DateTime conversion from string to DateTime
    // The opt can either be given in integer hours ago, or
    // as an ISO-8601 date time.
    implicit val decodeDateTime: Decoder[DateTime] = Decoder.decodeString.emap { s =>
        Either.catchNonFatal({
            if (s.forall(Character.isDigit)) DateTime.now - s.toInt.hours
            else DateTime.parse(s, iso8601DateFormatter)
        }).leftMap(t => throw new RuntimeException(
            s"Failed parsing '$s' into a DateTime. Must provide either an integer hours ago, " +
             "or an ISO-8601 formatted string."
        ))
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
            val p = Profig.as[$t]
            Profig.clear()
            p
        """)
    }


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
            val p = Profig.as[$t]
            Profig.clear()
            p
         """
        )
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
      *
      * @param matchedValues
      * @param unmatchedArgs
      * @return
      */
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
