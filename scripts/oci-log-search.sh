#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF'
Query btfind custom logs through OCI Logging Search.

Logging Search requires a log-stream path rooted at a compartment:
  "<compartment-ocid>/<log-group>/<log>"
A bare log-group or log OCID is not a valid scope and returns HTTP 404
NotAuthorizedOrNotFound. This wrapper always builds the compartment-rooted
path, so it cannot reproduce that operator error.

Usage: oci-log-search.sh --service crawler|web [OPTIONS]

Options:
  -s, --service SERVICE  crawler or web (required)
  -e, --event EVENT      event filter (default: crawler_status or web_status)
  -m, --since-minutes N  lookback window ending now (default: 60)
      --since-hours N    lookback window in hours (same window setting as
                         --since-minutes; the last window flag wins)
      --count            return only the number of matching records
      --per-day          return matching counts grouped by UTC day
      --recent N         return the N most recent matching records (default mode)
  -n, --limit N          maximum rows in recent mode (default: 5)
  -h, --help             show this help

Environment:
  OCI_COMPARTMENT_OCID  required; tenancy or compartment OCID that contains
                        the log group
  BTFIND_LOG_GROUP      log group name or OCID (default: btfind-production)
  BTFIND_CRAWLER_LOG    crawler log name or OCID (default: btfind-crawler)
  BTFIND_WEB_LOG        web log name or OCID (default: btfind-web)
  OCI_REGION            default: ap-osaka-1

Output is the OCI CLI JSON response on stdout; OCI errors go to stderr and
produce a non-zero exit status. A single query covers at most 14 days; use
consecutive windows for older history because custom-log retention is 30 days.
EOF
}

die() {
    printf 'oci-log-search: %s\n' "$*" >&2
    exit 2
}

validate_component() {
    local value=$1
    local expected_ocid_type=$2
    local label=$3
    [[ -n $value ]] || die "$label must not be empty"
    case $value in
        "ocid1.$expected_ocid_type".*) return 0 ;;
        ocid1.*) die "$label is an OCID of the wrong type" ;;
    esac
    case $value in
        *[!A-Za-z0-9._-]*) die "$label contains characters not valid in a log-stream path" ;;
    esac
}

service=
event=
since_minutes=60
mode=recent
limit=5

while (($# > 0)); do
    case $1 in
        -s | --service)
            (($# >= 2)) || die "$1 requires a value"
            service=$2
            shift 2
            ;;
        -e | --event)
            (($# >= 2)) || die "$1 requires a value"
            event=$2
            shift 2
            ;;
        -m | --since-minutes)
            (($# >= 2)) || die "$1 requires a value"
            [[ $2 =~ ^[0-9]+$ ]] || die "$1 must be a non-negative integer"
            since_minutes=$((10#$2))
            shift 2
            ;;
        --since-hours)
            (($# >= 2)) || die "$1 requires a value"
            [[ $2 =~ ^[0-9]+$ ]] || die "$1 must be a non-negative integer"
            since_minutes=$((10#$2 * 60))
            shift 2
            ;;
        --count)
            mode=count
            shift
            ;;
        --per-day)
            mode=per_day
            shift
            ;;
        --recent)
            (($# >= 2)) || die "$1 requires a value"
            [[ $2 =~ ^[0-9]+$ ]] || die "$1 must be a positive integer"
            limit=$((10#$2))
            ((limit > 0)) || die "$1 must be a positive integer"
            mode=recent
            shift 2
            ;;
        -n | --limit)
            (($# >= 2)) || die "$1 requires a value"
            [[ $2 =~ ^[0-9]+$ ]] || die "$1 must be a positive integer"
            limit=$((10#$2))
            ((limit > 0)) || die "$1 must be a positive integer"
            shift 2
            ;;
        -h | --help)
            usage
            exit 0
            ;;
        *)
            die "unknown argument: $1 (try --help)"
            ;;
    esac
done

((since_minutes > 0)) || die "the lookback window must be at least one minute"
((since_minutes <= 14 * 24 * 60)) || die "the lookback window cannot exceed 14 days"

case ${service-} in
    crawler)
        log_stream_component=${BTFIND_CRAWLER_LOG:-btfind-crawler}
        log_env_var=BTFIND_CRAWLER_LOG
        event=${event:-crawler_status}
        ;;
    web)
        log_stream_component=${BTFIND_WEB_LOG:-btfind-web}
        log_env_var=BTFIND_WEB_LOG
        event=${event:-web_status}
        ;;
    '')
        die "--service crawler|web is required (try --help)"
        ;;
    *)
        die "--service must be crawler or web"
        ;;
esac

compartment_ocid=${OCI_COMPARTMENT_OCID:-}
log_group_component=${BTFIND_LOG_GROUP:-btfind-production}
region=${OCI_REGION:-ap-osaka-1}

[[ -n $compartment_ocid ]] || die "set OCI_COMPARTMENT_OCID to the log group's compartment or tenancy OCID"
case $compartment_ocid in
    ocid1.compartment.* | ocid1.tenancy.*) ;;
    *) die "OCI_COMPARTMENT_OCID is not a compartment or tenancy OCID" ;;
esac
case $event in
    *\'*) die "the event filter must not contain a single quote" ;;
esac
validate_component "$log_group_component" loggroup BTFIND_LOG_GROUP
validate_component "$log_stream_component" log "$log_env_var"

now=$(date -u +%s)
start=$((now - since_minutes * 60))
scope="$compartment_ocid/$log_group_component/$log_stream_component"

case $mode in
    recent)
        query="search \"$scope\" | where data.fields.event = '$event' | sort by datetime desc"
        ;;
    count)
        query="search \"$scope\" | where data.fields.event = '$event' | count"
        limit=1
        ;;
    per_day)
        query="search \"$scope\" | where data.fields.event = '$event' | summarize count() as count by rounddown(datetime, '1d') as day"
        limit=100
        ;;
esac

exec oci logging-search search-logs \
    --region "$region" \
    --search-query "$query" \
    --time-start "$start" \
    --time-end "$now" \
    --limit "$limit" \
    --output json
