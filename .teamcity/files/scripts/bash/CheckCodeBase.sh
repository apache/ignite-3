TRIGGER=%teamcity.build.branch%

if echo $TRIGGER | grep pull; then
  PR=${TRIGGER#*/}
  SOURCE=$(curl -s https://api.github.com/repos/apache/ignite-3/pulls/$PR | jq -r .head.ref)
else
  SOURCE=$TRIGGER
fi
echo $SOURCE

if grep -IER --exclude-dir={.git,.idea} '.' -e ".*${SOURCE}.*"; then
    echo
    echo "Code base contains mention ticket!"
    echo
fi