echo "Killing processes starters"
for processPort in {3344..3345} \
                   {10000..10050} \
                   {10800..10850}; do
    for PID in $(lsof -t -i:${processPort}); do
        echo -n "    Killing  PID ${PID}"
        processInfo="$(ps -L "$PID")"
        kill -9 "${PID}"
        echo "        Killed process info: ${processInfo}"
    done
done

while read string; do
    pid="$(cut -d" " -f1 <<< "${string}")"
    process_name="$(cut -d" " -f5- <<< "${string}")"
    echo "Kill $process_name with $pid PID"
    kill -9 ${pid}
done < <(ps a | grep '[b]in/java' | grep -v '/opt/java/openjdk/bin/java' | tr -s ' ' || true)

# Force correct permissions
sudo chown -R teamcity:teamcity .