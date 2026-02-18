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

package org.apache.ignite.example.code.deployment;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.ignite.example.util.DeployComputeUnit;

public class AbstractDeploymentUnitExample {

    // Root path of the project
    private static final Path projectRoot = Paths.get("").toAbsolutePath();

    // Pre-built JAR from deploymentUnitJar task (built at compile time)
    private static final Path DEFAULT_JAR_PATH =
            projectRoot.resolve("examples/java/build/libs/deploymentunit-example-1.0.0.jar");

    private static String jarPathAsString = "";
    private static Path jarPath = DEFAULT_JAR_PATH;
    private static boolean runFromIDE = true;

    public static String getJarPathAsString() {
        return jarPathAsString;
    }

    public static Path getJarPath() {
        return jarPath;
    }

    public static boolean isRunFromIDE() {
        return runFromIDE;
    }
    /**
     * Processes the deployment unit.
     *
     * @param args Arguments passed to the deployment process.
     * @throws IOException if any error occurs.
     */
    public static void processDeploymentUnit(String[] args) throws IOException {
        DeployComputeUnit.DeploymentArgs deploymentArgs = DeployComputeUnit.processArguments(args);

        runFromIDE = deploymentArgs.runFromIDE();
        String newJarPathStr = deploymentArgs.jarPath();

        // Use isBlank() instead of trim().isEmpty() to avoid creating a new String
        if (newJarPathStr != null && !newJarPathStr.isBlank()) {
            jarPathAsString = newJarPathStr;
            jarPath = Path.of(newJarPathStr);
        }

        // JAR is pre-built at compile time via deploymentUnitJar task
        // No runtime JAR building needed - just verify it exists
        if (!Files.exists(jarPath)) {
            throw new IllegalStateException(
                "Deployment unit JAR not found at: " + jarPath + "\n" +
                "Please build the project first: ./gradlew :ignite-examples:build"
            );
        }
    }
}
