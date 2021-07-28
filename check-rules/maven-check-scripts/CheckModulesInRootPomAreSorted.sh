#!/usr/bin/env bash
set -o nounset; set -o errexit; set -o pipefail; set -o errtrace; set -o functrace


xpath -e "project/modules/module/text()" pom.xml 2>/dev/null > current-list
cat current-list | sort -h > sorted-list
DIFF="$(diff current-list sorted-list || true)"
if [ "${DIFF}" != "" ]; then
	echo "%ERROR_TEXT__UNSORTED_MODULES%:"
    echo "${DIFF}"
fi
