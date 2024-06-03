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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/**
 * Test suite for {@link ChannelType}.
 */
public class ChannelTypeTest extends BaseNetworkTest {
    private static short OFFSET = Short.MAX_VALUE - 100;

    @Test
    public void testRegister() {
        ChannelType testRegister = ChannelType.register(OFFSET, "TestRegister");

        assertThat(testRegister, is(ChannelType.getChannel(OFFSET)));
        OFFSET++;
    }

    @Test
    public void testRegisterAlready() {
        ChannelType.register(OFFSET, "TestRegister1");

        assertThrows(ChannelTypeAlreadyExist.class, () -> ChannelType.register(OFFSET, "TestRegister2"));

        OFFSET++;
    }

    @Test
    public void testRegisterSame() {
        ChannelType.register(OFFSET, "TestRegister1");

        assertThrows(ChannelTypeAlreadyExist.class, () -> ChannelType.register(OFFSET, "TestRegister1"));

        OFFSET++;
    }

    @Test
    public void testGetNotRegistered() {
        ChannelType channel = ChannelType.getChannel(OFFSET);

        assertThat(channel, nullValue());
    }

    @Test
    public void testRegisterNegativeIndex() {
        assertThrows(IllegalArgumentException.class, () -> ChannelType.register((short) -1, "test"));
    }
}
