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

package org.apache.ignite.internal.configuration.storage;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.configuration.NodeBootstrapConfiguration;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for the {@link LocalFileConfigurationStorage}.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class LocalFileConfigurationStorageTest extends ConfigurationStorageTest {

    @WorkDirectory
    private Path tmpDir;

    @Override
    public ConfigurationStorage getStorage() {
        Path configFile = getConfigFile();
        return new LocalFileConfigurationStorage(NodeBootstrapConfiguration.directFile(configFile));
    }

    @Test
    void testHocon() throws IOException {
        // All of this is needed because write expects serializable values and only concrete classes are serializable
        HashMap<String, ArrayList<String>> map = new HashMap<>(Map.of("list", new ArrayList<>(List.of("val1", "val2"))));
        var data = Map.of("foo1", "bar1", "foo2", "bar2", "map", map);

        CompletableFuture<Boolean> future = storage.write(data, 0);
        assertThat(future.join(), is(true));

        String contents = Files.readString(getConfigFile());
        assertThat(contents, is("foo1=bar1\n"
                + "foo2=bar2\n"
                + "map {\n"
                + "    list=[\n"
                + "        val1,\n"
                + "        val2\n"
                + "    ]\n"
                + "}\n"));
    }

    private Path getConfigFile() {
        return tmpDir.resolve(NodeBootstrapConfiguration.DEFAULT_CONFIG_NAME);
    }
}
