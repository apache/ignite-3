/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class EndpointTest {

    @Test
    public void testToStringReset() {
        final Endpoint ep = new Endpoint("192.168.1.1", 8080);
        assertEquals("192.168.1.1:8080", ep.toString());
        assertEquals("192.168.1.1:8080", ep.toString());
    }

    @Test
    public void testCopy() {
        final Endpoint ep = new Endpoint("192.168.1.1", 8080);
        assertEquals("192.168.1.1:8080", ep.toString());
        assertEquals("192.168.1.1:8080", ep.toString());
        assertEquals(ep, ep.copy());
    }

    @Test
    public void testEqualsHashCode() {
        final Endpoint ep1 = new Endpoint("192.168.1.1", 8080);
        final Endpoint ep2 = new Endpoint("192.168.1.1", 8080);
        assertEquals(ep1, ep2);
        assertEquals(ep1.hashCode(), ep2.hashCode());
    }
}
