package org.wikimedia.analytics.refinery.job.mediawikihistory

case class MediawikiHistoryCheckerConfig(
  // Thresholds for error reporting - compliant / non-compliant rows thresholds
  userWrongRowsRatioThreshold: Double = 0.05d,
  reducedWrongRowsRatioThreshold: Double = 0.05d,
  denormalizedWrongRowsRatioThreshold: Double = 0.05d,
  pageWrongRowsRatioThreshold: Double = 0.05d,
  // ReducedHistoryChecker section
  growth_count_reduced_event_min: Double = -0.01d,
  growth_count_reduced_event_max: Double = 1.0d,
  growth_distinct_user_text_min: Double = -0.01d,
  growth_distinct_user_text_max: Double = 1.0d,
  growth_count_user_group_bot_min: Double = -0.01d,
  growth_count_user_group_bot_max: Double = 1.0d,
  growth_count_user_name_bot_min: Double = -0.01d,
  growth_count_user_name_bot_max: Double = 1.0d,
  growth_count_user_anonymous_min: Double = -0.01d,
  growth_count_user_anonymous_max: Double = 1.0d,
  growth_count_user_user_min: Double = -0.01d,
  growth_count_user_user_max: Double = 1.0d,
  growth_count_user_self_created_min: Double = -0.01d,
  growth_count_user_self_created_max: Double = 1.0d,
  growth_count_revisions_min: Double = -0.01d,
  growth_count_revisions_max: Double = 1.0d,
  growth_sum_text_bytes_diff_abs_min: Double = -0.01d,
  growth_sum_text_bytes_diff_abs_max: Double = 1.0d,
  // Page values with digests
  growth_distinct_page_title_min: Double = -0.01d,
  growth_distinct_page_title_max: Double = 1.0d,
  growth_distinct_page_namespace_min: Double = -0.01d,
  growth_distinct_page_namespace_max: Double = 1.0d,
  growth_count_page_content_min: Double = -0.01d,
  growth_count_page_content_max: Double = 1.0d,
  growth_count_page_non_content_min: Double = -0.01d,
  growth_count_page_non_content_max: Double = 1.0d,
  growth_count_revision_deleted_min: Double = -0.01d,
  growth_count_revision_deleted_max: Double = 1.0d,
  // DenormalizedHistoryChecker section
  growth_count_denorm_event_min: Double = -0.01d,
  growth_count_denorm_event_max: Double = 1.0d,
  growth_distinct_page_id_min: Double = -0.01d,
  growth_distinct_page_id_max: Double = 1.0d,
  // UserHistoryChecker section
  growth_count_user_event_min: Double = -0.01d,
  growth_count_user_event_max: Double = 1.0d,
  growth_distinct_user_id_min: Double = -0.01d,
  growth_distinct_user_id_max: Double = 1.0d,
  growth_count_user_temporary_min: Double = -0.01d,
  growth_count_user_temporary_max: Double = 1.0d,
  growth_count_user_permanent_min: Double = -0.01d,
  growth_count_user_permanent_max: Double = 1.0d,
  // PageHistoryChecker section
  growth_count_page_event_min: Double = -0.01d,
  growth_count_page_event_max: Double = 1.0d,
  growth_distinct_all_page_id_min: Double = -0.01d,
  growth_distinct_all_page_id_max: Double = 1.0d,
  // Since we measure variability, we set the lower accepted threshold limit to
  // Variability not growth --> between -max and max
  variability_count_page_redirect_max: Double = 1.0d,
  variability_sum_text_bytes_diff_max: Double = 1.0d
) { }
