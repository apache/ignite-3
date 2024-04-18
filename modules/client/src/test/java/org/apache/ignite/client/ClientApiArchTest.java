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

package org.apache.ignite.client;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Checks client API imports.
 */
public class ClientApiArchTest {
    @Test
    public void testClientApiImportsArePublic() throws IOException {
        Path clientApiDir = Path.of(
                getRepoRoot(),
                "modules",
                "client",
                "src",
                "main",
                "java",
                "org",
                "apache",
                "ignite",
                "client");

        var fileCount = new AtomicInteger();

        try (Stream<Path> walk = Files.walk(clientApiDir)) {
            walk.forEach(f -> {
                if (Files.isDirectory(f)) {
                    return;
                }

                fileCount.incrementAndGet();
                assertPublicImports(
                        f,
                        "internal.client.IgniteClientConfigurationImpl;",
                        "internal.client.TcpIgniteClient;",
                        "internal.client.ClientUtils.sync;",
                        "internal.client.SslConfigurationBuilder;");
            });
        }

        assertThat("No files found in " + clientApiDir, fileCount.get(), greaterThan(0));
    }

    private static void assertPublicImports(Path path, String... excludes) {
        if (path.endsWith("package-info.java")) {
            return;
        }

        List<String> code;
        try {
            code = Files.readAllLines(path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        assertThat("Code is too short in " + path, code.size(), greaterThan(30));
        int lineNumber = 0;

        for (var line : code) {
            lineNumber++;

            if (line.startsWith("import ")) {
                boolean excluded = false;

                for (var exclude : excludes) {
                    if (line.contains(exclude)) {
                        excluded = true;
                        break;
                    }
                }

                if (excluded) {
                    continue;
                }

                if (line.contains(".internal.") || line.contains(".impl.")) {
                    Assertions.fail("Import is not public in " + path + ":" + lineNumber + " (" + line + ")");
                }

                if (line.contains("*")) {
                    Assertions.fail("Wildcard import is not allowed in " + path + ":" + lineNumber + " (" + line + ")");
                }
            }
        }
    }

    private static String getRepoRoot() {
        var currentDir = System.getProperty("user.dir");
        var path = Path.of(currentDir);

        while (path != null) {
            if (Files.exists(path.resolve("gradlew.bat"))) {
                return path.toString();
            }

            path = path.getParent();
        }

        throw new IllegalStateException("Can't find parent project directory from " + currentDir);
    }
}
