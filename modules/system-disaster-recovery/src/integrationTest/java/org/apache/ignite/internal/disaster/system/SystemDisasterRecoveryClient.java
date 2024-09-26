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

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import org.apache.ignite.internal.cli.Main;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;

/**
 * Used to run system disaster recovery CLI commands.
 */
@SuppressWarnings("UseOfProcessBuilder")
class SystemDisasterRecoveryClient {
    private static final IgniteLogger LOG = Loggers.forClass(SystemDisasterRecoveryClient.class);

    void initiateCmgRepairVia(String httpHost, int httpPort, String... newCmgNodeNames) throws InterruptedException {
        LOG.info("Initiating CMG repair via {}:{}, new CMG {}", httpHost, httpPort, List.of(newCmgNodeNames));

        String javaBinaryPath = ProcessHandle.current().info().command().orElseThrow();
        String javaClassPath = System.getProperty("java.class.path");

        LOG.info("Java binary is {}, classpath is {}", javaBinaryPath, javaClassPath);

        //noinspection UseOfProcessBuilder
        ProcessBuilder processBuilder = new ProcessBuilder(
                javaBinaryPath,
                "-cp", javaClassPath,
                Main.class.getName(),
                "recovery", "cluster", "reset",
                "--url", "http://" + httpHost + ":" + httpPort,
                "--cluster-management-group", String.join(",", newCmgNodeNames)
        );
        executeProcessFrom(processBuilder);
    }

    private static void executeProcessFrom(ProcessBuilder processBuilder) throws InterruptedException {
        try {
            Process process = processBuilder.start();

            if (!process.waitFor(10, SECONDS)) {
                throw new RuntimeException("Process did not finish in 10 seconds");
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
        try (InputStream stdout = process.getInputStream()) {
            return new String(stdout.readAllBytes(), UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String stderrString(Process process) {
        try (InputStream stderr = process.getErrorStream()) {
            return new String(stderr.readAllBytes(), UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
