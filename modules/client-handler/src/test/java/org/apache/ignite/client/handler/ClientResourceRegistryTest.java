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

package org.apache.ignite.client.handler;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.ErrorGroups.Client;
import org.apache.ignite.lang.IgniteException;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link org.apache.ignite.client.handler.ClientResourceRegistry}.
 */
public class ClientResourceRegistryTest {
    @Test
    public void testPutGetRemove() throws IgniteInternalCheckedException {
        var reg = new ClientResourceRegistry();
        ClientResource resource = new ClientResource(1, null);

        var id = reg.put(resource);
        var returned = reg.get(id);
        var removed = reg.remove(id);

        assertSame(resource, returned);
        assertSame(resource, removed);

        var ex = assertThrows(IgniteException.class, () -> reg.get(id));
        assertThat(ex.getMessage(), containsString("Failed to find resource with id: 1"));
        assertEquals(Client.RESOURCE_NOT_FOUND_ERR, ex.code());
    }

    @Test
    public void testCloseReleasesAllResourcesAndBlocksUsage() throws IgniteInternalCheckedException {
        var reg = new ClientResourceRegistry();
        var closed = new AtomicLong();

        reg.put(new ClientResource(1, closed::incrementAndGet));
        reg.put(new ClientResource(2, closed::incrementAndGet));

        reg.close();

        assertEquals(2, closed.get());

        String expected = "Resource registry is closed.";
        var ex = assertThrows(IgniteInternalCheckedException.class, () -> reg.put(new ClientResource(1, null)));
        assertThat(ex.getMessage(), containsString(expected));

        ex = assertThrows(IgniteInternalCheckedException.class, () -> reg.get(0));
        assertThat(ex.getMessage(), containsString(expected));

        ex = assertThrows(IgniteInternalCheckedException.class, () -> reg.remove(0));
        assertThat(ex.getMessage(), containsString(expected));
    }
}
