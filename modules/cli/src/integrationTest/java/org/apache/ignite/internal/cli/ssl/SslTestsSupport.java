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

package org.apache.ignite.internal.cli.ssl;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.util.Objects;

class SslTestsSupport {
    static String getResourcePath(String resource) {
        try {
            URL url = CliSslNotInitializedIntegrationTestBase.class.getClassLoader().getResource(resource);
            Objects.requireNonNull(url, "Resource " + resource + " not found.");
            Path path = Path.of(url.toURI()); // Properly extract file system path from the "file:" URL
            return path.toString().replace("\\", "\\\\"); // Escape backslashes for the config parser
        } catch (URISyntaxException e) {
            throw new RuntimeException(e); // Shouldn't happen since URL is obtained from the class loader
        }
    }
}
