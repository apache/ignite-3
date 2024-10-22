/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.disaster.system;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.util.ArrayUtils.concat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.apache.ignite.internal.cli.Main;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.jetbrains.annotations.Nullable;

/**
 * Used to run system disaster recovery CLI commands.
 */
@SuppressWarnings("UseOfProcessBuilder")
class SystemDisasterRecoveryClient {
    private static final IgniteLogger LOG = Loggers.forClass(SystemDisasterRecoveryClient.class);

    void initiateClusterReset(
            String httpHost,
            int httpPort,
            @Nullable Integer metastorageReplicationFactor,
            String... newCmgNodeNames
    ) throws InterruptedException {
        LOG.info(
                "Initiating cluster reset via {}:{}, new CMG {}, metastorage replication Factor {}",
                httpHost,
                httpPort,
                List.of(newCmgNodeNames),
                metastorageReplicationFactor
        );

        String[] args = {Main.class.getName(), "recovery", "cluster", "reset", "--url", "http://" + httpHost + ":" + httpPort};

        if (newCmgNodeNames.length > 0) {
            args = concat(args, "--cluster-management-group", String.join(",", newCmgNodeNames));
        }

        if (metastorageReplicationFactor != null) {
            args = concat(args, "--metastorage-replication-factor", String.valueOf(metastorageReplicationFactor));
        }

        executeWithSameJavaBinaryAndClasspath(args);
    }

    private static void executeWithSameJavaBinaryAndClasspath(String... args) throws InterruptedException {
        String javaBinaryPath = ProcessHandle.current().info().command().orElseThrow();
        String javaClassPath = System.getProperty("java.class.path");

        LOG.info("Java binary is {}, classpath is {}", javaBinaryPath, javaClassPath);

        String[] fullArgs = Stream.concat(Stream.of(javaBinaryPath, "-cp", javaClassPath), Arrays.stream(args))
                .toArray(String[]::new);

        //noinspection UseOfProcessBuilder
        ProcessBuilder processBuilder = new ProcessBuilder(fullArgs);
        executeProcessFrom(processBuilder);
    }

    private static void executeProcessFrom(ProcessBuilder processBuilder) throws InterruptedException {
        try {
            Process process = processBuilder.start();

            if (!process.waitFor(30, SECONDS)) {
                throw new RuntimeException("Process did not finish in time, stdout so far '" + stdoutString(process, false)
                        + "', stderr so far '" + stderrString(process, false) + "'");
            }
            if (process.exitValue() != 0) {
                throw new RuntimeException("Return code " + process.exitValue()
                        + ", stdout: " + stdoutString(process) + ", stderr: " + stderrString(process));
            }

            LOG.info("stdout is '{}'", stdoutString(process));
            LOG.info("stderr is '{}'", stderrString(process));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String stdoutString(Process process) {
        return stdoutString(process, true);
    }

    private static String stdoutString(Process process, boolean readFully) {
        try (InputStream stdout = process.getInputStream()) {
            return new String(streamContent(stdout, readFully), UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String stderrString(Process process) {
        return stderrString(process, true);
    }

    private static String stderrString(Process process, boolean readFully) {
        try (InputStream stderr = process.getErrorStream()) {
            return new String(streamContent(stderr, readFully), UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static byte[] streamContent(InputStream is, boolean readFully) throws IOException {
        if (readFully) {
            return is.readAllBytes();
        } else {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();

            while (is.available() > 0) {
                baos.write(is.read());
            }

            return baos.toByteArray();
        }
    }

    void initiateMigration(String oldHttpHost, int oldHttpPort, String newHttpHost, int newHttpPort) throws InterruptedException {
        LOG.info("Initiating migration, old {}:{}, new {}:{}", oldHttpHost, oldHttpPort, newHttpHost, newHttpPort);

        executeWithSameJavaBinaryAndClasspath(
                Main.class.getName(),
                "recovery", "cluster", "migrate",
                "--old-cluster-url", "http://" + oldHttpHost + ":" + oldHttpPort,
                "--new-cluster-url", "http://" + newHttpHost + ":" + newHttpPort
        );
    }
}
