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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Checks client API imports.
 */
public class ClientApiArchTest {
    @Test
    public void testClientApiImportsArePublic() {
        assert false : "TODO";
    }

    @Test
    public void testClientConfigurationImportsArePublic() throws IOException {
        // Read IgniteClientConfiguration.java and check that all imports are public.
        // Get repo root directory
        var rootDir = getRepoRoot();
        var clientConfigPath = Path.of(
                rootDir,
                "modules",
                "client",
                "src",
                "main",
                "java",
                "org",
                "apache",
                "ignite",
                "client",
                "IgniteClientConfiguration.java");

        var code = Files.readAllLines(clientConfigPath);
        assertThat(code.size(), greaterThan(50));

        for (var line : code) {
            if (line.startsWith("import ")) {
                if (line.contains(".internal.") || line.contains(".impl.")) {
                    Assertions.fail("Import is not public: " + line);
                }
            }
        }
    }

    private static String getRepoRoot() {
        var currentDir = System.getProperty("user.dir");
        var path = Path.of(currentDir);

        while (path != null) {
            if (Files.exists(path.resolve(".git"))) {
                return path.toString();
            }

            path = path.getParent();
        }

        throw new IllegalStateException("Can't find parent .git directory from " + currentDir);
    }
}
