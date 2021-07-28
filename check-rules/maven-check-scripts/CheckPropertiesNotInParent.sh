#!/usr/bin/env bash
set -o nounset; set -o errexit; set -o pipefail; set -o errtrace; set -o functrace


find . -name "pom.xml" | \
  grep -v parent | \
  while read -r pom; do
      if grep '<properties>' "${pom}"; then
          echo "%ERROR_TEXT__PROPERTIES_SECTION%: ${pom}"
      fi
done
