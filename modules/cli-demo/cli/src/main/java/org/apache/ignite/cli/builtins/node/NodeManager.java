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
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchService;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.ignite.cli.builtins.module.ModuleStorage;
import org.apache.ignite.cli.IgniteCLIException;

@Singleton
public class NodeManager {

    private static final String MAIN_CLASS = "org.apache.ignite.app.IgniteRunner";
    private static final Duration NODE_START_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration LOG_FILE_POLL_INTERVAL = Duration.ofMillis(50);

    private final ModuleStorage moduleStorage;

    @Inject
    public NodeManager(ModuleStorage moduleStorage) {
        this.moduleStorage = moduleStorage;
    }

    public RunningNode start(String consistentId,
        Path workDir, Path pidsDir, Path serverConfig) {

        if (getRunningNodes(workDir, pidsDir).stream().anyMatch(n -> n.consistentId.equals(consistentId)))
            throw new IgniteCLIException("Node with consistentId " + consistentId + " is already exist");
        try {
            Path logFile = logFile(workDir, consistentId);
            if (Files.exists(logFile))
                Files.delete(logFile);

            Files.createFile(logFile);

            var commandArgs = Arrays.asList(
                "java", "-cp", classpath(), MAIN_CLASS,
                "--config", serverConfig.toAbsolutePath().toString()
            );

            ProcessBuilder pb = new ProcessBuilder(
                commandArgs
            )
                .redirectError(logFile.toFile())
                .redirectOutput(logFile.toFile());
            Process p = pb.start();
            try {
                if (!waitForStart("Ignite application started successfully", logFile, NODE_START_TIMEOUT)) {
                    p.destroyForcibly();
                    throw new IgniteCLIException("Node wasn't started during timeout period "
                        + NODE_START_TIMEOUT.toMillis() + "ms");
                }
            }
            catch (InterruptedException|IOException e) {
                throw new IgniteCLIException("Waiting for node start was failed", e);
            }
            createPidFile(consistentId, p.pid(), pidsDir);
            return new RunningNode(p.pid(), consistentId, logFile);
        }
        catch (IOException e) {
            throw new IgniteCLIException("Can't load classpath", e);
        }
    }

    // TODO: We need more robust way of checking if node successfully run
    private static boolean waitForStart(String started, Path file, Duration timeout) throws IOException, InterruptedException {
       try(WatchService watchService = FileSystems.getDefault().newWatchService()) {
           file.getParent().register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);
           var beginTime = System.currentTimeMillis();
           while ((System.currentTimeMillis() - beginTime) < timeout.toMillis()) {
               var key = watchService.poll(LOG_FILE_POLL_INTERVAL.toMillis(), TimeUnit.MILLISECONDS);
               if (key != null) {
                   for (WatchEvent<?> event : key.pollEvents()) {
                       if (event.kind().equals(StandardWatchEventKinds.ENTRY_MODIFY) &&
                           (((WatchEvent<Path>)event).context().getFileName()).equals(file.getFileName())) {
                           var content = Files.readString(file);
                           if (content.contains(started))
                               return true;
                           else if (content.contains("Exception"))
                               throw new IgniteCLIException("Can't start the node. Read logs for details: " + file);
                       }
                   }
                   key.reset();
               }
           }
           return false;
       }
    }

    public String classpath() throws IOException {
        return moduleStorage.listInstalled().modules.stream()
            .flatMap(m -> m.artifacts.stream())
            .map(m -> m.toAbsolutePath().toString())
            .collect(Collectors.joining(":"));
    }

    public void createPidFile(String consistentId, long pid,Path pidsDir) {
        if (!Files.exists(pidsDir)) {
            if (!pidsDir.toFile().mkdirs())
                throw new IgniteCLIException("Can't create directory for storing the process pids: " + pidsDir);
        }

        Path pidPath = pidsDir.resolve(consistentId + "_" + System.currentTimeMillis() + ".pid");

        try (FileWriter fileWriter = new FileWriter(pidPath.toFile())) {
            fileWriter.write(String.valueOf(pid));
        }
        catch (IOException e) {
            throw new IgniteCLIException("Can't write pid file " + pidPath);
        }
    }

    public List<RunningNode> getRunningNodes(Path worksDir, Path pidsDir) {
        if (Files.exists(pidsDir)) {
            try (Stream<Path> files = Files.find(pidsDir, 1, (f, attrs) ->  f.getFileName().toString().endsWith(".pid"))) {
                    return files
                        .map(f -> {
                            long pid = 0;
                            try {
                                pid = Long.parseLong(Files.readAllLines(f).get(0));
                                if (!ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false))
                                    return Optional.<RunningNode>empty();
                            }
                            catch (IOException e) {
                                throw new IgniteCLIException("Can't parse pid file " + f);
                            }
                            String filename = f.getFileName().toString();
                            if (filename.lastIndexOf("_") == -1)
                                return Optional.<RunningNode>empty();
                            else {
                                String consistentId = filename.substring(0, filename.lastIndexOf("_"));
                                return Optional.of(new RunningNode(pid, consistentId, logFile(worksDir, consistentId)));
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

    public boolean stopWait(String consistentId, Path pidsDir) {
        if (Files.exists(pidsDir)) {
            try {
                List<Path> files = Files.find(pidsDir, 1,
                    (f, attrs) ->
                        f.getFileName().toString().startsWith(consistentId + "_")).collect(Collectors.toList());
                if (files.size() > 0) {
                    return files.stream().map(f -> {
                        try {
                            long pid = Long.parseLong(Files.readAllLines(f).get(0));
                            boolean result = stopWait(pid);
                            Files.delete(f);
                            return result;
                        }
                        catch (IOException e) {
                            throw new IgniteCLIException("Can't read pid file " + f);
                        }
                    }).reduce((a, b) -> a && b).orElse(false);
                }
                else
                    throw new IgniteCLIException("Can't find node with consistent id " + consistentId);
            }
            catch (IOException e) {
                throw new IgniteCLIException("Can't open directory with pid files " + pidsDir);
            }
        }
        else
            return false;
    }

    private boolean stopWait(long pid) {
        return ProcessHandle
            .of(pid)
            .map(ProcessHandle::destroy)
            .orElse(false);
    }
    
    private static Path logFile(Path workDir, String consistentId) {
          return workDir.resolve(consistentId + ".log");
    }

    public static class RunningNode {

        public final long pid;
        public final String consistentId;
        public final Path logFile;

        public RunningNode(long pid, String consistentId, Path logFile) {
            this.pid = pid;
            this.consistentId = consistentId;
            this.logFile = logFile;
        }
    }
}
