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

package org.apache.ignite.internal.rest;

import static org.apache.ignite.internal.rest.PathAvailability.available;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.junit.jupiter.api.Test;

/**
 * Test suite for {@link RestManager}.
 */
public class RestManagerTest {
    private static final String[] AVAILABLE_ON_START = {
            "cluster/v1",
            "node/v1/test",
            "cluster/v2/test"
    };

    private static final String[] UNAVAILABLE_ON_START = {
            "cluster/v2",
            "node/v1",
            "node",
            "cluster/v2/method"
    };

    @Test
    public void pathAvailabilityTest() {
        RestManager restManager = new RestManager(AVAILABLE_ON_START);

        for (String availablePath : AVAILABLE_ON_START) {
            assertThat(restManager.pathAvailability(availablePath), is(available()));
        }

        for (String unavailablePath : UNAVAILABLE_ON_START) {
            assertThat(restManager.pathAvailability(unavailablePath).isAvailable(), is(false));
        }

        restManager.setState(RestState.INITIALIZATION);

        for (String availablePath : AVAILABLE_ON_START) {
            assertThat(restManager.pathAvailability(availablePath).isAvailable(), is(false));
        }

        for (String unavailablePath : UNAVAILABLE_ON_START) {
            assertThat(restManager.pathAvailability(unavailablePath).isAvailable(), is(false));
        }

        restManager.setState(RestState.INITIALIZED);

        for (String availablePath : AVAILABLE_ON_START) {
            assertThat(restManager.pathAvailability(availablePath), is(available()));
        }

        for (String unavailablePath : UNAVAILABLE_ON_START) {
            assertThat(restManager.pathAvailability(unavailablePath), is(available()));
        }
    }
}
