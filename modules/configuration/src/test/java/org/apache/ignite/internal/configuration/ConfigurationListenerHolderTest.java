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

package org.apache.ignite.internal.configuration;

import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Iterator;
import java.util.List;
import java.util.Spliterators;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Class to test the {@link ConfigurationListenerHolder}.
 */
public class ConfigurationListenerHolderTest {
    private ConfigurationListenerHolder<? super Object> holder;

    @BeforeEach
    void beforeEach() {
        holder = new ConfigurationListenerHolder<>();
    }

    @Test
    void testAddListeners() {
        holder.addListener(1, 0);
        holder.addListener(2, 0);
        holder.addListener(1, 0);
        holder.addListener(3, 0);

        assertEquals(
                List.of(1, 2, 1, 3),
                collect(holder.listeners(1))
        );
    }

    @Test
    void testRemoveListeners() {
        holder.addListener(1, 0);
        holder.addListener(2, 0);
        holder.addListener(1, 0);

        holder.removeListener(1);

        assertEquals(
                List.of(2, 1),
                collect(holder.listeners(1))
        );
    }

    @Test
    void testListeners() {
        assertTrue(collect(holder.listeners(-1)).isEmpty());
        assertTrue(collect(holder.listeners(0)).isEmpty());
        assertTrue(collect(holder.listeners(1)).isEmpty());

        holder.addListener(1, 0);
        holder.addListener(2, 0);
        holder.addListener(1, 0);
        holder.addListener(3, 0);

        holder.addListener(4, 1);
        holder.addListener(5, 1);

        holder.addListener(7, 2);
        holder.addListener(8, 2);

        assertTrue(collect(holder.listeners(-1)).isEmpty());

        assertEquals(
                List.of(1, 2, 1, 3),
                collect(holder.listeners(1))
        );

        assertEquals(
                List.of(1, 2, 1, 3, 4, 5),
                collect(holder.listeners(2))
        );

        assertEquals(
                List.of(1, 2, 1, 3, 4, 5, 7, 8),
                collect(holder.listeners(3))
        );

        assertEquals(
                List.of(1, 2, 1, 3, 4, 5, 7, 8),
                collect(holder.listeners(4))
        );
    }

    @Test
    void testClear() {
        holder.addListener(1, 0);
        holder.addListener(2, 0);

        holder.clear();

        assertTrue(collect(holder.listeners(1)).isEmpty());
    }

    private List<?> collect(Iterator<?> iterator) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false).collect(toList());
    }
}
