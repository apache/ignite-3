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

package org.apache.ignite.internal.network;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.ignite.internal.network.messages.AllTypesMessage;
import org.apache.ignite.internal.tostring.IgniteToStringExclude;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.junit.jupiter.api.Test;

/**
 * Test suite for the network annotation processor that uses the {@link AllTypesMessage} to test support of all possible types.
 */
public class AllTypesMessageTest {
    /**
     * Tests the {@link #equals} contract on the generated messages.
     */
    @Test
    public void testEquals() {
        AllTypesMessage msg = AllTypesMessageGenerator.generate(0, true);

        assertNotEquals(null, msg);
        assertEquals(msg, msg);

        AllTypesMessage msg2 = AllTypesMessageGenerator.generate(1, true);

        assertNotEquals(msg, msg2);
        assertNotEquals(msg2, msg);
    }

    /**
     * Tests the {@link #hashCode} contract on the generated messages.
     */
    @Test
    public void testHashCode() {
        AllTypesMessage msg = AllTypesMessageGenerator.generate(0, true);

        assertEquals(msg.hashCode(), msg.hashCode());

        for (int i = 1; i <= 100; ++i) {
            AllTypesMessage msg2 = AllTypesMessageGenerator.generate(i, true);

            if (msg2.hashCode() != msg.hashCode()) {
                return;
            }
        }

        fail("All generated messages had the same hash code");
    }

    /**
     * Tests that {@link IgniteToStringInclude} and {@link IgniteToStringExclude} are processed correctly.
     */
    @Test
    public void testIgniteToStringAnnotations() {
        AllTypesMessage msg = AllTypesMessageGenerator.generate(0, false);

        assertThat(msg.toString(), containsString("strQ"));
        assertThat(msg.toString(), not(containsString("excludedString")));
        assertThat(msg.toString(), not(containsString("sensitiveString")));
    }
}
