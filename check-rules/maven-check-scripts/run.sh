#!/usr/bin/env bash
set -o nounset; set -o errexit; set -o pipefail; set -o errtrace; set -o functrace


DIR__MAVEN_CHECK_SCRIPTS="check-rules/maven-check-scripts"
for script in CheckDependencyAndPluginVersionsNotInParent.sh \
              CheckModulesInRootPomAreSorted.sh \
              CheckPropertiesNotInParent.sh \
              CheckUnusedDependenciesAndPluginsInParent.sh \
              CheckUnusedProperties.sh; do
    echo " * Executing ${script}..."
    bash ${DIR__MAVEN_CHECK_SCRIPTS}/${script}
done

rm -rf current-list
       sorted-list
