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

package org.apache.ignite.internal.configuration.validation;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;

import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;

class ConfigurationDuplicatesValidatorTest extends BaseIgniteAbstractTest {
    private static final String DUPLICATE_CONFIG = "ignite {\n"
            + "  network: {\n"
            + "    port: 456,\n"
            + "    nodeFinder.netClusterNodes: [ \"localhost\" ]\n"
            + "  },\n"
            + "  network: {}\n"
            + "  network.nodeFinder.netClusterNodes: [ \"localhost\" ],\n"
            + "  storage.profiles: {\n"
            + "        test.engine: test, \n"
            + "        default_aipersist.engine: aipersist, \n"
            + "        default_aipersist.sizeBytes: 123, \n"
            + "        test.engine: aimem \n"
            + "  },\n"
            + "  storage {\n"
            + "      profiles = [\n"
            + "          {\n"
            + "              name = persistent,\n"
            + "              name = persistent\n"
            + "          }\n"
            + "      ]\n"
            + "  },\n"
            + "  clientConnector.port: 123,\n"
            + "  rest {\n"
            + "    port: 123,\n"
            + "    port: 456\n"
            + "  },\n"
            + "  compute { threadPoolSize: 1 },\n"
            + "  compute { threadPoolSize: 2 }\n"
            + "  failureHandler.dumpThreadsOnFailure: false\n"
            + "}";

    private static final String OK_CONFIG = "ignite {\n"
            + "  network: {\n"
            + "    port: 456,\n"
            + "    nodeFinder.netClusterNodes: [ {} ]\n"
            + "  },\n"
            + "  storage {\n"
            + "      profiles = [\n"
            + "          {\n"
            + "              name = persistent,\n"
            + "              engine = aipersist\n"
            + "          }\n"
            + "          {\n"
            + "              name = rocksdb-example,\n"
            + "              engine = rocksdb\n"
            + "          }\n"
            + "          {\n"
            + "              name = in-memory,\n"
            + "              engine = aimem\n"
            + "          }\n"
            + "      ]\n"
            + "  },\n"
            + "  clientConnector.port: 123,\n"
            + "  rest {\n"
            + "    port: 123\n"
            + "  },\n"
            + "  failureHandler.dumpThreadsOnFailure: false\n"
            + "}";

    @Test
    void testValidationSucceededWithNoDuplicates() {
        assertThat(ConfigurationDuplicatesValidator.validate(OK_CONFIG), empty());
    }

    @Test
    void testValidationFailedWithDuplicates() {
        assertThat(ConfigurationDuplicatesValidator.validate(DUPLICATE_CONFIG),
                containsInAnyOrder(
                        new ValidationIssue("ignite.network", "Duplicated key"),
                        new ValidationIssue("ignite.network.nodeFinder.netClusterNodes", "Duplicated key"),
                        new ValidationIssue("ignite.storage.profiles.[0].name", "Duplicated key"),
                        new ValidationIssue("ignite.storage.profiles", "Duplicated key"),
                        new ValidationIssue("ignite.storage.profiles.test.engine", "Duplicated key"),
                        new ValidationIssue("ignite.rest.port", "Duplicated key"),
                        new ValidationIssue("ignite.compute", "Duplicated key"),
                        new ValidationIssue("ignite.compute.threadPoolSize", "Duplicated key")
                )
        );
    }
}
