PACKAGES="$(cat target/site/apidocs/index.html | grep org.apache.ignite | sed -r 's|.*html">(.*)</a.*|\1|' | grep internal || true)"
if [ "${PACKAGES}" != "" ]; then
	echo "[ERROR] Internal packages detected"
    for package in ${PACKAGES}; do
    	echo "    ${package}"
    done
fi