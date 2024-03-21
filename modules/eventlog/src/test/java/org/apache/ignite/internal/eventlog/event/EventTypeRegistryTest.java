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

package org.apache.ignite.internal.eventlog.event;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import org.apache.ignite.internal.eventlog.event.exception.NotUniqueEventTypeException;
import org.junit.jupiter.api.Test;

class EventTypeRegistryTest {

    @Test
    void register() {
        assertFalse(EventTypeRegistry.contains("register_test"));
        EventTypeRegistry.register("register_test");
        assertTrue(EventTypeRegistry.contains("register_test"));
    }

    @Test
    void registerTwice() {
        assertFalse(EventTypeRegistry.contains("register_twice_test"));
        EventTypeRegistry.register("register_twice_test");
        assertTrue(EventTypeRegistry.contains("register_twice_test"));

        assertThrows(
                NotUniqueEventTypeException.class,
                () -> EventTypeRegistry.register("register_twice_test"),
                "Event type `register_twice_test` is already registered. Please, use another name."
        );
    }

    @Test
    void registerMany() {
        assertFalse(EventTypeRegistry.contains("register_many_test1"));
        assertFalse(EventTypeRegistry.contains("register_many_test2"));

        EventTypeRegistry.register(Set.of("register_many_test1", "register_many_test2"));

        assertTrue(EventTypeRegistry.contains("register_many_test1"));
        assertTrue(EventTypeRegistry.contains("register_many_test2"));
    }
}
