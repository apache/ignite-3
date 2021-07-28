#!/usr/bin/env bash
set -o nounset; set -o errexit; set -o pipefail; set -o errtrace; set -o functrace


POMS=$(find . -name pom.xml | grep -v parent)
for xpath in "project/dependencyManagement/dependencies/dependency/artifactId/text()" \
             "project/build/pluginManagement/plugins/plugin/artifactId/text()"; do
	xpath -e "${xpath}" parent/pom.xml 2>&1 | \
      grep -vE '(NODE|Found)' | \
      while read -r declaration; do
        FOUND=false
        for pom in ${POMS}; do
        	if grep -E "<artifactId>${declaration}</artifactId>" "${pom}"; then
            	FOUND=true
                continue 2
            fi
        done
        for parent_xpath in "project/build/plugins" \
                            "project/dependencies"; do
            if xpath -e "${parent_xpath}" parent/pom.xml 2>&1 | \
              grep -E "<" | \
              grep -E "<artifactId>${declaration}</artifactId>"; then
              	FOUND=true
                continue 2
            fi
        done
        if [ "${FOUND}" == "false" ]; then
            echo "${declaration}" >> %FILE_UNUSED_PROPERTIES%
            echo "%ERROR_TEXT__UNUSED_DECLARATION%: ${declaration}"
        fi
    done
done
