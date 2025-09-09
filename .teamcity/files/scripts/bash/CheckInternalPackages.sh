set +x
ERROR_TEXT__INTERNAL_PACKAGES="%ERROR_TEXT__INTERNAL_PACKAGES%"
set -x


PACKAGES="$(grep "org.apache.ignite" "target/site/apidocs/index.html" | \
              sed -r 's|.*html">(.*)</a.*|\1|' | \
              grep internal || true)"
if [ "${PACKAGES}" != "" ]; then
	echo "${ERROR_TEXT__INTERNAL_PACKAGES}"
    for package in ${PACKAGES}; do
    	echo "    ${package}"
    done
fi
