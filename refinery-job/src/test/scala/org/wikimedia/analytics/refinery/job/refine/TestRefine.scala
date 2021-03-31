package org.wikimedia.analytics.refinery.job.refine

import org.scalatest.{FlatSpec, Matchers}

class TestRefine extends FlatSpec with Matchers {

    it should "succeed Refine.Config validation" in {
        new Refine.Config(
            input_database = Some("_INPUT_EXAMPLE_"),
            output_database = Some("_OUTPUT_EXAMPLE_"),
            output_path = Some("_OUTPUT_EXAMPLE_")
        )

        new Refine.Config(
            input_path = Some("_INPUT_EXAMPLE_"),
            input_path_regex = Some("_INPUT_REGEX_EXAMPLE_"),
            input_path_regex_capture_groups = Some(Seq("table", "_INPUT_PATH_REGEX_CAPTURE_GROUPS_EXAMPLE_")),
            output_database = Some("_OUTPUT_EXAMPLE_"),
            output_path = Some("_OUTPUT_EXAMPLE_")
        )

        assert(true, "Refine.Config should be valid")
    }

    it should "fail Refine.Config validation" in {
        // no input_path or input_database
        assertThrows[IllegalArgumentException] {
            new Refine.Config(
                output_database = Some("_OUTPUT_EXAMPLE_"),
                output_path = Some("_OUTPUT_EXAMPLE_")
            )
        }

        // no output_path
        assertThrows[IllegalArgumentException] {
            new Refine.Config(
                input_database = Some("_INPUT_EXAMPLE_"),
                output_database = Some("_OUTPUT_EXAMPLE_")
            )
        }

        // no output_database
        assertThrows[IllegalArgumentException] {
            new Refine.Config(
                input_database = Some("_INPUT_EXAMPLE_"),
                output_path = Some("_OUTPUT_EXAMPLE_")
            )
        }

        // input_path without input_path_regex
        assertThrows[IllegalArgumentException] {
            new Refine.Config(
                input_path = Some("_INPUT_EXAMPLE_"),
                input_path_regex_capture_groups = Some(Seq("table", "_INPUT_PATH_REGEX_CAPTURE_GROUPS_EXAMPLE_")),
                output_database = Some("_OUTPUT_EXAMPLE_"),
                output_path = Some("_OUTPUT_EXAMPLE_")
            )
        }

        // input_path without input_path_regex_capture_groups
        assertThrows[IllegalArgumentException] {
            new Refine.Config(
                input_path = Some("_INPUT_EXAMPLE_"),
                input_path_regex = Some("_INPUT_REGEX_EXAMPLE_"),
                output_database = Some("_OUTPUT_EXAMPLE_"),
                output_path = Some("_OUTPUT_EXAMPLE_")
            )
        }

        // input_path_regex_capture_groups without table in capture groups
        assertThrows[IllegalArgumentException] {
            new Refine.Config(
                input_path = Some("_INPUT_EXAMPLE_"),
                input_path_regex = Some("_INPUT_REGEX_EXAMPLE_"),
                input_path_regex_capture_groups = Some(Seq("_INPUT_PATH_REGEX_CAPTURE_GROUPS_EXAMPLE_")),
                output_database = Some("_OUTPUT_EXAMPLE_"),
                output_path = Some("_OUTPUT_EXAMPLE_")
            )
        }
    }

}