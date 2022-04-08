/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.ignite.client.handler.ClientResource;
import org.apache.ignite.client.handler.ClientResourceRegistry;
import org.apache.ignite.lang.IgniteException;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link org.apache.ignite.client.handler.ClientResourceRegistry}.
 */
public class ClientResourceRegistryTest {
    @Test
    public void testPutGetRemove() {
        var reg = new ClientResourceRegistry();
        ClientResource resource = new ClientResource(1, null);

        var id = reg.put(resource);
        var returned = reg.get(id);
        var removed = reg.remove(id);

        assertSame(resource, returned);
        assertSame(resource, removed);
        assertThrows(IgniteException.class, () -> reg.get(id));
    }

    @Test
    public void testCloseReleasesAllResourcesAndBlocksUsage() {
        // TODO
        var reg = new ClientResourceRegistry();
        var closed = new AtomicLong();

        reg.put(new ClientResource(1, closed::incrementAndGet));
        reg.put(new ClientResource(2, closed::incrementAndGet));

        reg.close();

        assertEquals(2, closed.get());
        assertThrows(IgniteException.class, () -> reg.get(0));
        assertThrows(IgniteException.class, () -> reg.put(new ClientResource(1, null)));
        assertThrows(IgniteException.class, () -> reg.remove(0));
    }
}
