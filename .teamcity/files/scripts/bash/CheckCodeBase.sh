TRIGGER=%teamcity.build.branch%

if echo $TRIGGER | grep pull; then
  PR=${TRIGGER#*/}
  SOURCE=$(curl -s https://api.github.com/repos/apache/ignite-3/pulls/$PR | jq -r .head.ref)
else
  SOURCE=$TRIGGER
fi
echo $SOURCE

EXCLUDE="%ISSUES_EXCLUDE_LIST%"
EXCLUDE_LIST=( $EXCLUDE )

MATCHES=$(grep -iIER --exclude-dir={.git,.idea} '.' -e ".*${SOURCE}.*" | \
    grep -v '@see' | \
    grep -v '<MUTED>' | \
    grep -v -f <(printf "%s\n" "${EXCLUDE_LIST[@]:-__no_excludes__}"))

if [ -n "$MATCHES" ]; then
    echo
    echo "Ticket $SOURCE is still mentioned in one or more TODOs in the code:"
    echo
    echo "$MATCHES"
    echo
    echo "=========================================================================="
    echo "If this mention is intentional and should be excluded, you can:"
    echo
    echo "  1. Add an inline suppression by appending '// <MUTED>' (or '/* <MUTED> */')"
    echo "     to the line with the mention, e.g.:"
    echo "     // TODO: fix this later $SOURCE  // <MUTED>"
    echo
    echo "  2. Add the ticket to ISSUES_EXCLUDE_LIST in the TC job"
    echo "     Example: IGNITE-15799 IGNITE-18899"
    echo
    echo "  3. Use a Javadoc '@see' reference — those are always ignored."
    echo "=========================================================================="
    echo
    exit 1
fi
