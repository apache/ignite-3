#!/usr/bin/env bash
set -o nounset; set -o errexit; set -o pipefail; set -o errtrace; set -o functrace


find . -name "pom.xml" |  \
  grep -v parent | \
  while read -r pom; do
    for xpath in "project/dependencies/dependency/version" \
                 "project/build/plugins/plugin/version" \
                 "project/profiles/profile/build/plugins/plugin/version"; do
		if xpath -e "${xpath}" ${pom} 2>&1 | \
          grep -qE "^Found"; then
            echo "%ERROR_TEXT__VERSION_IN_NON_PARENT_POM%: ${pom}"
		fi
    done
done
