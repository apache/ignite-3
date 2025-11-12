# Pre-clean info
echo "JPS (before): "
sudo %env.JAVA_HOME%/bin/jps | while read -r process; do
    echo "    ${process}"
done
echo

echo "Killing processes starters by name"
for processName in MainWithArgsInFile \
                   IgniteNodeRunner \
                   PlatformTestNodeRunner \
                   CommandLineStartup \
                   GradleDaemon; do
    for PID in $(%env.JAVA_HOME%/bin/jps | grep ${processName} | awk '{ print $1 }'); do
        echo -n "    Killing '${processName}' process with PID '${PID}'... "
        processInfo="$(ps aux -p "$PID")"
        sudo kill -9 "${PID}" && echo "[OK]" || {
            echo "[ERROR] Unable to kill process ${PID}"
            exit 1
        }
        echo "        Killed process info: ${processInfo}"
    done
done
echo

echo "Killing processes starters by port"
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

# Post-clean info
echo "JPS (after): "
sudo %env.JAVA_HOME%/bin/jps | while read -r process; do
    echo "    ${process}"
done
echo

# Force correct permissions
sudo chown -R teamcity:teamcity .