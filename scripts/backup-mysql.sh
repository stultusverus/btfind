#!/usr/bin/env bash
set -euo pipefail
umask 077
: "${MYSQL_DEFAULTS_FILE:?Set the path to a protected MySQL client option file}"
: "${OCI_BACKUP_BUCKET:?Set the private backup bucket}"
: "${OCI_BACKUP_NAMESPACE:?Set the Object Storage namespace}"
test -f "$MYSQL_DEFAULTS_FILE"
backup_dir=$(mktemp -d /tmp/btfind-backup.XXXXXXXX)
backup_file="$backup_dir/btfind.sql.gz"
cleanup() {
    rm -f -- "$backup_file"
    rmdir -- "$backup_dir"
}
trap cleanup EXIT
mysqldump --defaults-extra-file="$MYSQL_DEFAULTS_FILE" \
    --single-transaction --quick --hex-blob --no-tablespaces \
    --set-gtid-purged=OFF --column-statistics=0 --skip-add-locks btfind |
    gzip > "$backup_file"
gzip -t "$backup_file"
backup_key="btfind/$(date -u +%Y-%m-%dT%H-%M-%SZ).sql.gz"
oci os object put --auth instance_principal --namespace-name "$OCI_BACKUP_NAMESPACE" \
    --bucket-name "$OCI_BACKUP_BUCKET" --name "$backup_key" --file "$backup_file"
printf 'Uploaded %s\n' "$backup_key"
