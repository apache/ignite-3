/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cli.builtins.node;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.ignite.cli.IgniteCLIException;
import org.apache.ignite.cli.builtins.module.ModuleRegistry;
import org.apache.ignite.cli.ui.Spinner;
import org.jline.terminal.Terminal;

/**
 * Manager of local Ignite nodes.
 */
@Singleton
public class NodeManager {
    /** Entry point of core Ignite artifact for running new node. */
    private static final String MAIN_CLASS = "org.apache.ignite.app.IgniteCliRunner";

    /** Timeout for successful node start. */
    private static final Duration NODE_START_TIMEOUT = Duration.ofSeconds(30);

    /** Interval for polling node logs to identify successful start. */
    private static final Duration LOG_FILE_POLL_INTERVAL = Duration.ofMillis(500);

    /** Module registry. **/
    private final ModuleRegistry moduleRegistry;

    /**
     * Creates node manager.
     *
     * @param moduleRegistry Module registry.
     * @param terminal System terminal instance.
     */
    @Inject
    public NodeManager(ModuleRegistry moduleRegistry, Terminal terminal) {
        this.moduleRegistry = moduleRegistry;
    }

    /**
     * Starts new Ignite node and check if it was successfully started.
     * It has very naive implementation of successful run check -
     * just waiting for appropriate message in the node logs.
     *
     * @param nodeName Node name.
     * @param logDir Log dir for receiving node state.
     * @param pidsDir Dir where pid files of running nodes will be stored.
     * @param srvCfg Config for Ignite node
     * @param out PrintWriter for user messages.
     * @return Information about successfully started node
     */
    public RunningNode start(String nodeName, Path logDir, Path pidsDir, Path srvCfg, PrintWriter out) {
        if (getRunningNodes(logDir, pidsDir).stream().anyMatch(n -> n.name.equals(nodeName)))
            throw new IgniteCLIException("Node with nodeName " + nodeName + " is already exist");

        try {
            Path logFile = logFile(logDir, nodeName);
            if (Files.exists(logFile))
                Files.delete(logFile);

            Files.createFile(logFile);

            var cmdArgs = new ArrayList<String>();

            cmdArgs.add("java");
            cmdArgs.add("-cp");
            cmdArgs.add(classpath());
            cmdArgs.add(MAIN_CLASS);

            if (srvCfg != null) {
                cmdArgs.add("--config");
                cmdArgs.add(srvCfg.toAbsolutePath().toString());
            }

            cmdArgs.add(nodeName);

            ProcessBuilder pb = new ProcessBuilder(
                cmdArgs
            )
                .redirectError(logFile.toFile())
                .redirectOutput(logFile.toFile());

            Process p = pb.start();

            try (var spinner = new Spinner(out, "Starting a new Ignite node")) {

                if (!waitForStart("Apache Ignite started successfully!", logFile, NODE_START_TIMEOUT, spinner)) {
                    p.destroyForcibly();

                    throw new IgniteCLIException("Node wasn't started during timeout period "
                        + NODE_START_TIMEOUT.toMillis() + "ms");
                }
            }
            catch (InterruptedException | IOException e) {
                throw new IgniteCLIException("Waiting for node start was failed", e);
            }

            createPidFile(nodeName, p.pid(), pidsDir);

            return new RunningNode(p.pid(), nodeName, logFile);
        }
        catch (IOException e) {
            throw new IgniteCLIException("Can't load classpath", e);
        }
    }

    /**
     * Waits for node start by checking node logs in cycle.
     *
     * @param started Mark string that node was started.
     * @param file Node's log file
     * @param timeout Timeout for waiting
     * @return true if node was successfully started, false otherwise.
     * @throws IOException If can't read the log file
     * @throws InterruptedException If waiting was interrupted.
     */
    private static boolean waitForStart(
        String started,
        Path file,
        Duration timeout,
        Spinner spinner
    ) throws IOException, InterruptedException {
        var start = System.currentTimeMillis();

        while ((System.currentTimeMillis() - start) < timeout.toMillis()) {
            spinner.spin();
            LockSupport.parkNanos(LOG_FILE_POLL_INTERVAL.toNanos());

            var content = Files.readString(file);

            if (content.contains(started))
                return true;
            else if (content.contains("Exception"))
                throw new IgniteCLIException("Can't start the node. Read logs for details: " + file);
        }

        return false;
    }

    /**
     * @return Actual classpath according to current installed modules.
     * @throws IOException If couldn't read the module registry file.
     */
    public String classpath() throws IOException {
        return moduleRegistry.listInstalled().modules.stream()
            .flatMap(m -> m.artifacts.stream())
            .map(m -> m.toAbsolutePath().toString())
            .collect(Collectors.joining(System.getProperty("path.separator")));
    }

    /**
     * @return Actual classpath items list according to current installed modules.
     * @throws IOException If couldn't read the module registry file.
     */
    public List<String> classpathItems() throws IOException {
        return moduleRegistry.listInstalled().modules.stream()
            .flatMap(m -> m.artifacts.stream())
            .map(m -> m.getFileName().toString())
            .collect(Collectors.toList());
    }

    /**
     * Creates pid file for Ignite node.
     *
     * @param nodeName Node name.
     * @param pid Pid
     * @param pidsDir Dir for storing pid files.
     */
    public void createPidFile(String nodeName, long pid, Path pidsDir) {
        if (!Files.exists(pidsDir)) {
            if (!pidsDir.toFile().mkdirs())
                throw new IgniteCLIException("Can't create directory for storing the process pids: " + pidsDir);
        }

        Path pidPath = pidsDir.resolve(nodeName + "_" + System.currentTimeMillis() + ".pid");

        try (FileWriter fileWriter = new FileWriter(pidPath.toFile())) {
            fileWriter.write(String.valueOf(pid));
        }
        catch (IOException e) {
            throw new IgniteCLIException("Can't write pid file " + pidPath);
        }
    }

    /**
     * @param logDir Ignite installation work dir.
     * @param pidsDir Dir with nodes pids.
     * @return List of running nodes.
     */
    public List<RunningNode> getRunningNodes(Path logDir, Path pidsDir) {
        if (Files.exists(pidsDir)) {
            try (Stream<Path> files = Files.find(pidsDir, 1, (f, attrs) -> f.getFileName().toString().endsWith(".pid"))) {
                return files
                    .map(f -> {
                        long pid;

                        try {
                            pid = Long.parseLong(Files.readAllLines(f).get(0));

                            if (!ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false))
                                return Optional.<RunningNode>empty();
                        }
                        catch (IOException e) {
                            throw new IgniteCLIException("Can't parse pid file " + f);
                        }

                        String filename = f.getFileName().toString();

                        if (filename.lastIndexOf('_') == -1)
                            return Optional.<RunningNode>empty();
                        else {
                            String nodeName = filename.substring(0, filename.lastIndexOf('_'));

                            return Optional.of(new RunningNode(pid, nodeName, logFile(logDir, nodeName)));
                        }

                    })
                    .filter(Optional::isPresent)
                    .map(Optional::get).collect(Collectors.toList());
            }
            catch (IOException e) {
                throw new IgniteCLIException("Can't find directory with pid files for running nodes " + pidsDir);
            }
        }
        else
            return Collections.emptyList();
    }

    /**
     * Stops the node by name and waits for success.
     *
     * @param nodeName Node name.
     * @param pidsDir Dir with running nodes pids.
     * @return true if stopped, false otherwise.
     */
    public boolean stopWait(String nodeName, Path pidsDir) {
        if (Files.exists(pidsDir)) {
            try {
                List<Path> files = Files.find(pidsDir, 1,
                    (f, attrs) ->
                        f.getFileName().toString().startsWith(nodeName + "_")).collect(Collectors.toList());

                if (!files.isEmpty()) {
                    return files.stream().map(f -> {
                        try {
                            long pid = Long.parseLong(Files.readAllLines(f).get(0));

                            boolean res = stopWait(pid);

                            Files.delete(f);

                            return res;
                        }
                        catch (IOException e) {
                            throw new IgniteCLIException("Can't read pid file " + f);
                        }
                    }).reduce((a, b) -> a && b).orElse(false);
                }
                else
                    throw new IgniteCLIException("Can't find node with name" + nodeName);
            }
            catch (IOException e) {
                throw new IgniteCLIException("Can't open directory with pid files " + pidsDir);
            }
        }
        else
            return false;
    }

    /**
     * Stops the process and waits for success.
     *
     * @param pid Pid of proccess to stop.
     * @return true if process was stopped, false otherwise.
     */
    private boolean stopWait(long pid) {
        return ProcessHandle
            .of(pid)
            .map(ProcessHandle::destroy)
            .orElse(false);
    }

    /**
     * @param logDir Ignite log dir.
     * @param nodeName Node name.
     * @return Path of node log file.
     */
    private static Path logFile(Path logDir, String nodeName) {
          return logDir.resolve(nodeName + ".log");
    }

    /**
     * Simple structure with information about running node.
     */
    public static class RunningNode {

        /** Pid. */
        public final long pid;

        /** Consistent id. */
        public final String name;

        /** Path to log file. */
        public final Path logFile;

        /**
         * Creates info about running node.
         *
         * @param pid Pid.
         * @param name Consistent id.
         * @param logFile Log file.
         */
        public RunningNode(long pid, String name, Path logFile) {
            this.pid = pid;
            this.name = name;
            this.logFile = logFile;
        }
    }
}
