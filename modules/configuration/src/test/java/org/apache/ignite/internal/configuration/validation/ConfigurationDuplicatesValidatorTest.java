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
    private static final String NL = System.lineSeparator();

    private static final String DUPLICATE_CONFIG = "ignite {" + NL
            + "  network: {" + NL
            + "    port: 456," + NL
            + "    nodeFinder.netClusterNodes: [ \"localhost\" ]" + NL
            + "  }," + NL
            + "  network: {}" + NL
            + "  network.nodeFinder.netClusterNodes: [ \"localhost\" ]," + NL
            + "  storage.profiles: {" + NL
            + "        test.engine: test," + NL
            + "        default_aipersist.engine: aipersist," + NL
            + "        default_aipersist.sizeBytes: 123," + NL
            + "        test.engine: aimem" + NL
            + "  }," + NL
            + "  storage {" + NL
            + "      profiles = [" + NL
            + "          {" + NL
            + "              name = persistent," + NL
            + "              name = persistent" + NL
            + "          }" + NL
            + "      ]" + NL
            + "  }," + NL
            + "  clientConnector.port: 123," + NL
            + "  rest {" + NL
            + "    port: 123," + NL
            + "    port: 456" + NL
            + "  }," + NL
            + "  compute { threadPoolSize: 1 }," + NL
            + "  compute { threadPoolSize: 2 }" + NL
            + "  failureHandler.handler.type: noop," + NL
            + "  failureHandler.dumpThreadsOnFailure: false" + NL
            + "}";

    private static final String OK_CONFIG = "ignite {" + NL
            + "  network: {" + NL
            + "    port: 456," + NL
            + "    nodeFinder.netClusterNodes: [ {} ]" + NL
            + "  }," + NL
            + "  storage {" + NL
            + "      profiles = [" + NL
            + "          {" + NL
            + "              name = persistent," + NL
            + "              engine = aipersist" + NL
            + "          }" + NL
            + "          {" + NL
            + "              name = rocksdb-example," + NL
            + "              engine = rocksdb" + NL
            + "          }" + NL
            + "          {" + NL
            + "              name = in-memory," + NL
            + "              engine = aimem" + NL
            + "          }" + NL
            + "      ]" + NL
            + "  }," + NL
            + "  clientConnector.port: 123," + NL
            + "  rest {" + NL
            + "    port: 123" + NL
            + "  }," + NL
            + "  failureHandler.handler.type: noop," + NL
            + "  failureHandler.dumpThreadsOnFailure: false" + NL
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
