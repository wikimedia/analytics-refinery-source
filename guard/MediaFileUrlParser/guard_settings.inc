echo_guard_input() {
    local hive_query="
SELECT
    CONCAT('https://', uri_host, uri_path)
FROM
    wmf.webrequest TABLESAMPLE(BUCKET 1 OUT OF 64)
WHERE
    uri_host = 'upload.wikimedia.org' AND
    uri_path NOT RLIKE '(monitoring/backend|robots.txt|crossdomain.xml|.{1000,})$' AND
    (http_status = 200 or http_status = 206) AND
    webrequest_source='upload' AND $(date +'year=%Y AND month=%m AND day=%d AND hour=0');
"


    beeline --silent=true --fastConnect=true  --showHeader=false -e "$hive_query" || true
}

set_guard_arguments() {
    add_guard_argument "--failure-limit-total=50"
    add_guard_argument "--failure-limit-per-kind=20"
}
