BRANCH_NAME=%teamcity.build.branch%

if [ "$BRANCH_NAME" != "main" ]; then
    if grep -IER --exclude-dir={.git,.idea} '.' -e ".*${BRANCH_NAME}.*"; then
        echo
        echo "Code base contains mention ticket!"
        echo
        exit 1
    fi
fi
