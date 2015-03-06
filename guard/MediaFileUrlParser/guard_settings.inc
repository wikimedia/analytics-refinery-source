echo_guard_input() {
    local TSV_FILE_ABS="/a/squid/archive/sampled/sampled-1000.tsv.log-$(date +'%Y%m%d').gz"

    if [ ! -f "$TSV_FILE_ABS" ]
    then
        error "The input TSV file '$TSV_FILE_ABS' does not exist"
    fi

    zcat /a/squid/archive/sampled/sampled-1000.tsv.log-$(date +'%Y%m%d').gz \
        | cut -f 6,9 \
        | cut -f 2- -d / \
        | grep '^\(200\|206\)' \
        | cut -f 2 \
        | grep '^https\?://upload.wikimedia.org/' \
        | cut -f 1 -d ? \
        | grep -v 'https\?://upload.wikimedia.org/\(\|monitoring/backend\|robots.txt\|crossdomain.xml\|.\{1000,\}\)$' \
        || true

}

set_guard_arguments() {
    add_guard_argument "--failure-limit-total=50"
    add_guard_argument "--failure-limit-per-kind=20"
}
