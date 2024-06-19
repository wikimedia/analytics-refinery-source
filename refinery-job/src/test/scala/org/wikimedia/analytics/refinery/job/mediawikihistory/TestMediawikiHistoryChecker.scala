package org.wikimedia.analytics.refinery.job.mediawikihistory

import com.amazon.deequ.analyzers.runners.EmptyStateException
import org.scalatest.FlatSpec
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, MapType, StringType, StructField, StructType}
import org.wikimedia.analytics.refinery.job.mediawikihistory.denormalized.DenormalizedHistoryChecker
import org.wikimedia.analytics.refinery.job.mediawikihistory.page.PageHistoryChecker
import org.wikimedia.analytics.refinery.job.mediawikihistory.reduced.ReducedHistoryChecker
import org.wikimedia.analytics.refinery.job.mediawikihistory.user.UserHistoryChecker

class TestMediawikiHistoryChecker extends FlatSpec with DataFrameSuiteBase {

    def growthSchema(col1: String): List[StructField] = List(
        StructField(col1, StringType, true),
        StructField("event_entity", StringType, true),
        StructField("event_type", StringType, true),
        StructField("growths", MapType(StringType, DoubleType, true), true)
    )
    val tolerance = 0.000001
    val mediawikiHistoryBasePath = "mediawiki_history_checker"
    val previousSnapshot = "2023-12"
    val newSnapshot = "2024-01"
    val wikisToCheck = 50
    val minEventsGrowthThreshold = -0.01
    val maxEventsGrowthThreshold = 1.0
    val wrongRowsRatioThreshold = 0.05

    def getGrowthDataFrame(data: Seq[Row], column1: String = "wiki_db") = {
        spark.createDataFrame(
            sc.parallelize(data),
            StructType(growthSchema(column1))
        )
    }

    it should "return user history growth error rows ratio" in {
        val userMetricsGrowthData = Seq(
            Row("ruwiki", "userHistory", "create",
                Map("growth_count_user_event" -> 0.004786113250798328,
                    "growth_distinct_user_id" -> 0.004786113250798328,
                    "growth_distinct_user_text" -> 0.004786113250798328,
                    "growth_count_user_group_bot" -> null,
                    "growth_count_user_anonymous" -> null,
                    "growth_count_user_self_created" -> 0.0035142079497027193)),
            Row("specieswiki", "userHistory", "alterblocks",
                Map("growth_count_user_event" -> 0.007456828885400314,
                    "growth_distinct_user_id" -> 0.005151688609044075,
                    "growth_distinct_user_text" -> 0.005151688609044075,
                    "growth_count_user_group_bot" -> 0.0,
                    "growth_count_user_anonymous" -> null,
                    "growth_count_user_self_created" -> 0.007100591715976331)),
            Row("bgwiki", "userHistory", "create",
                Map("growth_count_user_event" -> 0.004999746129975479,
                    "growth_distinct_user_id" -> 0.004999746129975479,
                    "growth_distinct_user_text" -> 0.004999746129975479,
                    "growth_count_user_group_bot" -> null,
                    "growth_count_user_anonymous" -> null,
                    "growth_count_user_self_created" -> 0.002870298909980142)),
            Row("zhwiki", "userHistory", "create",
                Map("growth_count_user_event" -> 0.004081501619029692,
                    "growth_distinct_user_id" -> 0.004081501619029692,
                    "growth_distinct_user_text" -> 0.004081501619029692,
                    "growth_count_user_group_bot" -> null,
                    "growth_count_user_anonymous" -> null,
                    "growth_count_user_self_created" -> 0.0020911789181026867)),
            Row("frwiki", "userHistory", "create",
                Map("growth_count_user_event" -> 0.004787007010385826,
                    "growth_distinct_user_id" -> 0.004787007010385826,
                    "growth_distinct_user_text" -> 0.004787007010385826,
                    "growth_count_user_group_bot" -> null,
                    "growth_count_user_anonymous" -> null,
                    "growth_count_user_self_created" -> 0.0035448992397922874)),
            Row("cswiki", "userHistory", "create",
                Map("growth_count_user_event" -> 0.005623430494492269,
                    "growth_distinct_user_id" -> 0.005623430494492269,
                    "growth_distinct_user_text" -> 0.005623430494492269,
                    "growth_count_user_group_bot" -> null,
                    "growth_count_user_anonymous" -> null,
                    "growth_count_user_self_created" -> 0.004103327094750165)),
            Row("nlwiki", "userHistory", "alterblocks",
                Map("growth_count_user_event" -> 5.619487405018447E-4,
                    "growth_distinct_user_id" -> 3.127388977691292E-4,
                    "growth_distinct_user_text" -> 3.127388977691292E-4,
                    "growth_count_user_group_bot" -> 0.0,
                    "growth_count_user_anonymous" -> null,
                    "growth_count_user_self_created" -> 3.6039282818271914E-4)),
            Row("enwikinews", "userHistory", "alterblocks",
                Map("growth_count_user_event" -> 7.309941520467836,
                    "growth_distinct_user_id" -> 5.230125523012552E-4,
                    "growth_distinct_user_text" -> 5.230125523012552E-4,
                    "growth_count_user_group_bot" -> 0.0,
                    "growth_count_user_anonymous" -> null,
                    "growth_count_user_self_created" -> 2.946375957572186E-4))
    )

        val userGrowthDataFrame = getGrowthDataFrame(userMetricsGrowthData)

        val errorRatio = new UserHistoryChecker(
            spark,
            mediawikiHistoryBasePath,
            previousSnapshot,
            newSnapshot,
            wikisToCheck,
            minEventsGrowthThreshold,
            maxEventsGrowthThreshold,
            wrongRowsRatioThreshold
        ).getUserGrowthErrorsRatio(userGrowthDataFrame)
        // check that difference between expected error ratio(0.125)
        // and actual error ratio is less than tolerance value
        assert(Math.abs(0.125 - errorRatio) < tolerance)
    }

    it should "return page history growth error rows ratio" in {
        val pageMetricsGrowthData = Seq(
            Row("enwiktionary", "pageHistory", "move",
                Map("growth_count_page_event" -> 0.003452452080675634,
                    "growth_distinct_all_page_id" -> 0.0032849722635040685,
                    "growth_distinct_page_id" -> 0.0033623910336239102,
                    "growth_distinct_page_artificial_id" -> 2.918004085205719E-4,
                    "growth_distinct_page_title" -> 0.003451459119843479,
                    "growth_distinct_page_namespace" -> -0.008823529411764705,
                    "variability_count_page_redirect" -> 0.0036910846110164677)),
            Row("enwiktionary", "pageHistory", "create-page",
                Map("growth_count_page_event" -> 0.017895590625484945,
                    "growth_distinct_all_page_id" -> 0.017895590625484945,
                    "growth_distinct_page_id" -> 0.017895590625484945,
                    "growth_distinct_page_artificial_id" -> null,
                    "growth_distinct_page_title" -> 0.017781961984882185,
                    "growth_distinct_page_namespace" -> -0.00702702702702703,
                    "variability_count_page_redirect" -> 0.01063614262560778)),
            Row("incubatorwiki", "pageHistory", "delete",
                Map("growth_count_page_event" -> 0.13653031863377688,
                    "growth_distinct_all_page_id" -> 0.1366457879889204,
                    "growth_distinct_page_id" -> 0.14396889256563578,
                    "growth_distinct_page_artificial_id" -> 0.0,
                    "growth_distinct_page_title" -> 0.14061155443433085,
                    "growth_distinct_page_namespace" -> 0.0,
                    "variability_count_page_redirect" -> 0.0)),
            Row("arwiki", "pageHistory", "restore",
                Map("growth_count_page_event" -> 0.002393112505472056,
                    "growth_distinct_all_page_id" -> 0.0021717491629716767,
                    "growth_distinct_page_id" -> 0.0022271714922048997,
                    "growth_distinct_page_artificial_id" -> 0.0,
                    "growth_distinct_page_title" -> 0.002078421640402035,
                    "growth_distinct_page_namespace" -> 0.0,
                    "variability_count_page_redirect" -> -6.883496816382723E-4)),
            Row("idwiki", "pageHistory", "delete",
                Map("growth_count_page_event" -> 0.00546218685584638,
                    "growth_distinct_all_page_id" -> 0.005465025025309823,
                    "growth_distinct_page_id" -> 0.006500291210704847,
                    "growth_distinct_page_artificial_id" -> 0.0010487415101877746,
                    "growth_distinct_page_title" -> 0.0045731194727462255,
                    "growth_distinct_page_namespace" -> 0.0,
                    "variability_count_page_redirect" -> 0.0)),
        )
        val pageGrowthDataFrame = getGrowthDataFrame(pageMetricsGrowthData)

        val errorRatio = new PageHistoryChecker(
            spark,
            mediawikiHistoryBasePath,
            previousSnapshot,
            newSnapshot,
            wikisToCheck,
            minEventsGrowthThreshold,
            maxEventsGrowthThreshold,
            wrongRowsRatioThreshold
        ).getPageGrowthErrorsRatio(pageGrowthDataFrame)

        // check that difference between expected error ratio(0.0)
        // and actual error ratio is less than tolerance value
        assert(Math.abs(0.0 - errorRatio) < tolerance)
    }

    it should "compute denormalized history's growth error rows ratio" in {
        val denormMetricsGrowthData = Seq(
            Row("eswiki", "page", "restore",
                Map("growth_count_denorm_event" -> 0.0018677708267698817,
                    "growth_distinct_user_id" -> 0.0,
                    "growth_distinct_user_text" -> 0.0,
                    "growth_count_user_group_bot" -> 0.0,
                    "growth_count_user_anonymous" -> null,
                    "growth_count_user_self_created"-> 0.0010220768601798855,
                    "growth_distinct_page_id" -> 0.0018789530791012783,
                    "growth_distinct_page_title" -> 0.0017394916530363215,
                    "growth_distinct_page_namespace" -> 0.0,
                    "variability_count_page_redirect" -> 0.0034982508745627187,
                    "growth_count_revision_deleted" -> null,
                    "growth_count_revision_reverted" -> null,
                    "growth_count_revision_revert" -> null)),
            Row("specieswiki", "revision", "create",
                Map("growth_count_denorm_event" -> 0.003533629363818918,
                    "growth_distinct_user_id" -> 0.003933957203314061,
                    "growth_distinct_user_text" -> 0.0032554847841472045,
                    "growth_count_user_group_bot" -> 9.47238558991814E-5,
                    "growth_count_user_anonymous" -> 0.003449268830128205,
                    "growth_count_user_self_created" -> 0.0016321947939521824,
                    "growth_distinct_page_id" -> 0.005918050281384821,
                    "growth_distinct_page_title"-> 0.006373250872562429,
                    "growth_distinct_page_namespace" -> 0.0,
                    "variability_count_page_redirect" -> 0.008204879347284533,
                    "growth_count_revision_deleted" -> 0.003767382399749537,
                    "growth_count_revision_reverted" -> 0.003571059660306639,
                    "growth_count_revision_revert" -> 0.00425129601076149)),
            Row("cawiki", "user", "create",
                Map("growth_count_denorm_event" -> 0.006023865249587538,
                    "growth_distinct_user_id" -> 0.006139963303870506,
                    "growth_distinct_user_text" -> 0.00614415227039499,
                    "growth_count_user_group_bot" -> null,
                    "growth_count_user_anonymous" -> null,
                    "growth_count_user_self_created" -> 0.002924666257842351,
                    "growth_distinct_page_id" -> null,
                    "growth_distinct_page_title" -> null,
                    "growth_distinct_page_namespace" -> null,
                    "variability_count_page_redirect" -> null,
                    "growth_count_revision_deleted" -> null,
                    "growth_count_revision_reverted" -> null,
                    "growth_count_revision_revert" -> null)),
            Row("frwikisource", "user", "create",
                Map("growth_count_denorm_event" -> 0.004978860703159769,
                    "growth_distinct_user_id" -> 0.0050589026072805745,
                    "growth_distinct_user_text" -> 0.005057624333264859,
                    "growth_count_user_group_bot" -> null,
                    "growth_count_user_anonymous" -> null,
                    "growth_count_user_self_created" -> 0.0035392829900839054,
                    "growth_distinct_page_id" -> null,
                    "growth_distinct_page_title" -> null,
                    "growth_distinct_page_namespace" -> null,
                    "variability_count_page_redirect" -> null,
                    "growth_count_revision_deleted" -> null,
                    "growth_count_revision_reverted" -> null,
                    "growth_count_revision_revert" -> null)),
            Row("nlwiki", "page", "merge",
                Map("growth_count_denorm_event" -> 0.0,
                    "growth_distinct_user_id" -> 0.0,
                    "growth_distinct_user_text" -> 0.0,
                    "growth_count_user_group_bot" -> null,
                    "growth_count_user_anonymous" -> null,
                    "growth_count_user_self_created" -> 0.0,
                    "growth_distinct_page_id" -> 0.0,
                    "growth_distinct_page_title" -> 0.0,
                    "growth_distinct_page_namespace" -> 0.0,
                    "variability_count_page_redirect" -> 0.0,
                    "growth_count_revision_deleted" -> null,
                    "growth_count_revision_reverted" -> null,
                    "growth_count_revision_revert" -> null)),
            Row("simplewiki", "revision", "create",
                Map("growth_count_denorm_event" -> 2.006266473120503885,
                    "growth_distinct_user_id" -> 0.008971045744102978,
                    "growth_distinct_user_text" -> 0.008165368344824227,
                    "growth_count_user_group_bot" -> 0.0015554380814732749,
                    "growth_count_user_anonymous" -> 0.010718745235082976,
                    "growth_count_user_self_created" -> 0.004529595976820161,
                    "growth_distinct_page_id" -> 0.00705649876616279,
                    "growth_distinct_page_title" -> 0.006943945333016359,
                    "growth_distinct_page_namespace" -> 0.0,
                    "variability_count_page_redirect" -> 0.0056615332615079115,
                    "growth_count_revision_deleted" -> 0.009759881569207995,
                    "growth_count_revision_reverted" -> 0.006977155628967673,
                    "growth_count_revision_revert" -> 0.006793411801107285)),
            Row("enwiktionary", "page", "create-page",
                Map("growth_count_denorm_event" -> 0.017895590625484945,
                    "growth_distinct_user_id" -> 0.016802200430519016,
                    "growth_distinct_user_text" -> 0.014443860854106904,
                    "growth_count_user_group_bot" -> 0.009109543272020834,
                    "growth_count_user_anonymous" -> 0.010105668397730234,
                    "growth_count_user_self_created" -> 0.015042832458160113,
                    "growth_distinct_page_id" -> 0.017895590625484945,
                    "growth_distinct_page_title" -> 0.017781961984882185,
                    "growth_distinct_page_namespace" -> -0.02702702702702703,
                    "variability_count_page_redirect" -> 0.01063614262560778,
                    "growth_count_revision_deleted" -> null,
                    "growth_count_revision_reverted" -> null,
                    "growth_count_revision_revert" -> null)),
            Row("enwiktionary", "page", "move",
                Map("growth_count_denorm_event" -> 0.003452452080675634,
                    "growth_distinct_user_id" -> 0.008005569091541943,
                    "growth_distinct_user_text" -> 0.008124576844955992,
                    "growth_count_user_group_bot" -> 0.002877857786890308,
                    "growth_count_user_anonymous" -> -4.947452062187973E-4,
                    "growth_count_user_self_created" -> 0.005662757076301362,
                    "growth_distinct_page_id" -> 0.0033623910336239102,
                    "growth_distinct_page_title" -> 0.003451459119843479,
                    "growth_distinct_page_namespace" -> -0.058823529411764705,
                    "variability_count_page_redirect" -> 0.0036910846110164677,
                    "growth_count_revision_deleted" -> null,
                    "growth_count_revision_reverted" -> null,
                    "growth_count_revision_revert"-> null))
       )
        val denormGrowthDataFrame = getGrowthDataFrame(denormMetricsGrowthData)

        val errorRatio = new DenormalizedHistoryChecker(
            spark,
            mediawikiHistoryBasePath,
            previousSnapshot,
            newSnapshot,
            wikisToCheck,
            minEventsGrowthThreshold,
            maxEventsGrowthThreshold,
            wrongRowsRatioThreshold
        ).getDenormGrowthErrorsRatio(denormGrowthDataFrame)
        // check that difference between expected error ratio(0.375)
        // and actual error ratio is less than tolerance value
        assert(Math.abs(0.375 - errorRatio) < tolerance)
    }

    it should "compute reduced history's growth error rows ratio" in {
        val reducedMetricsGrowthData = Seq(
            Row("ro.wikipedia", "page", "restore",
                Map("growth_count_reduced_event" -> 5.899705014749262E-4,
                    "growth_distinct_user_text" -> 0.0,
                    "growth_count_user_group_bot" -> null,
                    "growth_count_user_name_bot" -> null,
                    "growth_count_user_anonymous" -> null,
                    "growth_count_user_user" -> 5.899705014749262E-4,
                    "growth_count_user_self_created" -> null,
                    "growth_distinct_page_title" -> 6.038647342995169E-4,
                    "growth_distinct_page_namespace" -> -0.0625,
                    "growth_count_page_content" -> -0.0019646365422396855,
                    "growth_count_page_non_content" -> 0.004431314623338257,
                    "variability_count_page_redirect" -> 0.0,
                    "growth_count_revision_deleted" -> null,
                    "growth_count_revision_reverted" -> null,
                    "growth_count_revision_revert" -> null,
                    "growth_count_revisions" -> null,
                    "variability_sum_text_bytes_diff" -> 0.0,
                    "growth_sum_text_bytes_diff_abs" -> 0.0)),
            Row("hu.wikipedia", "user", "monthly_digest",
                Map ("growth_count_reduced_event" -> 0.004805941365346434,
                    "growth_distinct_user_text" -> null,
                    "growth_count_user_group_bot" -> -0.041679923639834554,
                    "growth_count_user_name_bot" -> 0.02942003185608545,
                    "growth_count_user_anonymous" -> 0.005018472723822401,
                    "growth_count_user_user" -> 0.004502319036640661,
                    "growth_count_user_self_created" -> null,
                    "growth_distinct_page_title" -> null,
                    "growth_distinct_page_namespace" -> null,
                    "growth_count_page_content" -> 0.004971086220369888,
                    "growth_count_page_non_content" -> 0.0038087694594383164,
                    "variability_count_page_redirect" -> null,
                    "growth_count_revision_deleted" -> null,
                    "growth_count_revision_reverted" -> null,
                    "growth_count_revision_revert" -> null,
                    "growth_count_revisions" -> 0.003852856663635977,
                    "variability_sum_text_bytes_diff" -> 0.004760724887286105,
                    "growth_sum_text_bytes_diff_abs" -> 0.004415231793070093)),
            Row("fr.wikipedia", "page", "merge",
                Map ("growth_count_reduced_event" -> 0.0,
                    "growth_distinct_user_text" -> 0.0,
                    "growth_count_user_group_bot" -> null,
                    "growth_count_user_name_bot" -> null,
                    "growth_count_user_anonymous" -> null,
                    "growth_count_user_user" -> 0.0,
                    "growth_count_user_self_created" -> null,
                    "growth_distinct_page_title" -> 0.0,
                    "growth_distinct_page_namespace" -> -0.16666666666666666,
                    "growth_count_page_content" -> 0.09090909090909091,
                    "growth_count_page_non_content" -> -0.2,
                    "variability_count_page_redirect" -> -0.1,
                    "growth_count_revision_deleted" -> null,
                    "growth_count_revision_reverted" -> null,
                    "growth_count_revision_revert" -> null,
                    "growth_count_revisions" -> null,
                    "variability_sum_text_bytes_diff" -> 0.0,
                    "growth_sum_text_bytes_diff_abs" -> 0.0)),
            Row("en.wikisource", "page", "restore",
                Map("growth_count_reduced_event" -> 8.378718056137411E-4,
                    "growth_distinct_user_text" -> 0.0,
                    "growth_count_user_group_bot" -> null,
                    "growth_count_user_name_bot" -> 0.0,
                    "growth_count_user_anonymous" -> null,
                    "growth_count_user_user" -> 8.382229673093043E-4,
                    "growth_count_user_self_created" -> null,
                    "growth_distinct_page_title" -> 8.442380751371887E-4,
                    "growth_distinct_page_namespace" -> -0.045454545454545456,
                    "growth_count_page_content" -> 0.0014844136566056407,
                    "growth_count_page_non_content" -> -0.00273224043715847,
                    "variability_count_page_redirect" -> 0.005050505050505051,
                    "growth_count_revision_deleted" -> null,
                    "growth_count_revision_reverted" -> null,
                    "growth_count_revision_revert" -> null,
                    "growth_count_revisions" -> null,
                    "variability_sum_text_bytes_diff" -> 0.0,
                    "growth_sum_text_bytes_diff_abs" -> 0.0)),
            Row("en.wiktionary", "page", "move",
                Map("growth_count_reduced_event" -> 0.0036555224499869444,
                    "growth_distinct_user_text" -> 0.008295625942684766,
                    "growth_count_user_group_bot" -> 0.0020364968696642005,
                    "growth_count_user_name_bot" -> 0.0,
                    "growth_count_user_anonymous" -> -6.234629905441446E-4,
                    "growth_count_user_user" -> 0.007581993451499022,
                    "growth_count_user_self_created" -> null,
                    "growth_distinct_page_title" -> 0.003473013908341748,
                    "growth_distinct_page_namespace" -> -0.07142857142857142,
                    "growth_count_page_content" -> 0.0034643946426203423,
                    "growth_count_page_non_content" -> 0.0038006305019475085,
                    "variability_count_page_redirect" -> 0.0036910846110164677,
                    "growth_count_revision_deleted" -> null,
                    "growth_count_revision_reverted" -> null,
                    "growth_count_revision_revert" -> null,
                    "growth_count_revisions" -> null,
                    "variability_sum_text_bytes_diff" -> 0.0,
                    "growth_sum_text_bytes_diff_abs" -> 0.0)),
            Row("hu.wikipedia", "user", "daily_digest",
              Map("growth_count_reduced_event" -> 0.004598035683026582,
                  "growth_distinct_user_text" -> null,
                  "growth_count_user_group_bot" -> -0.043971216477429624,
                  "growth_count_user_name_bot" -> 0.024395323238032902,
                  "growth_count_user_anonymous" -> 0.004925159514313847,
                  "growth_count_user_user" -> 0.004548028782177564,
                  "growth_count_user_self_created" -> null,
                  "growth_distinct_page_title" -> null,
                  "growth_distinct_page_namespace" -> null,
                  "growth_count_page_content" -> 0.004795757438876508,
                  "growth_count_page_non_content" -> 0.003777343503750867,
                  "variability_count_page_redirect" -> null,
                  "growth_count_revision_deleted" -> null,
                  "growth_count_revision_reverted" -> null,
                  "growth_count_revision_revert" -> null,
                  "growth_count_revisions" -> 0.003852856663635977,
                  "variability_sum_text_bytes_diff" -> 0.004760724887286105,
                  "growth_sum_text_bytes_diff_abs" -> 0.004415231793070093))
        )
        val reducedGrowthDataFrame = getGrowthDataFrame(reducedMetricsGrowthData, "project")

        val errorRatio = new ReducedHistoryChecker(
            spark,
            mediawikiHistoryBasePath,
            previousSnapshot,
            newSnapshot,
            wikisToCheck,
            minEventsGrowthThreshold,
            maxEventsGrowthThreshold,
            wrongRowsRatioThreshold
        ).getReducedGrowthErrorsRatio(reducedGrowthDataFrame)
        println(errorRatio)
        // check that difference between expected error ratio(1.0)
        // and actual error ratio is less than tolerance value
        assert(Math.abs(1.0 - errorRatio) < tolerance)
    }

    it should "Give a EmptyStateException when the column is null" in {
        val nullData = Seq(
            Row("cswiki", "userHistory", "create",
                Map("growth_count_user_event" -> null,
                    "growth_distinct_user_id" -> null,
                    "growth_distinct_user_text" -> null,
                    "growth_count_user_group_bot" -> null,
                    "growth_count_user_anonymous" -> null,
                    "growth_count_user_self_created" -> null)),
            Row("nlwiki", "userHistory", "alterblocks",
                Map("growth_count_user_event" -> null,
                    "growth_distinct_user_id" -> null,
                    "growth_distinct_user_text" -> null,
                    "growth_count_user_group_bot" -> null,
                    "growth_count_user_anonymous" -> null,
                    "growth_count_user_self_created" -> 3.6039282818271914E-4)),
            Row("enwikinews", "userHistory", "alterblocks",
                Map("growth_count_user_event" -> null,
                    "growth_distinct_user_id" -> 5.230125523012552E-4,
                    "growth_distinct_user_text" -> 5.230125523012552E-4,
                    "growth_count_user_group_bot" -> 0.0,
                    "growth_count_user_anonymous" -> null,
                    "growth_count_user_self_created" -> 2.946375957572186E-4))
        )

        val nullDataFrame = getGrowthDataFrame(nullData)
        assertThrows[java.lang.RuntimeException](
            new UserHistoryChecker(
                spark,
                mediawikiHistoryBasePath,
                previousSnapshot,
                newSnapshot,
                wikisToCheck,
                minEventsGrowthThreshold,
                maxEventsGrowthThreshold,
                wrongRowsRatioThreshold
            ).getUserGrowthErrorsRatio(nullDataFrame))
        }

}
