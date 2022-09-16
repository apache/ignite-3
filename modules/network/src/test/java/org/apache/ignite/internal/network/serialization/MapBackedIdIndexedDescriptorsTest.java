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

package org.apache.ignite.internal.network.serialization;

import static java.util.Collections.emptyMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class MapBackedIdIndexedDescriptorsTest {
    private final ClassDescriptorRegistry unrelatedRegistry = new ClassDescriptorRegistry();

    @Test
    void retrievesKnownDescriptorByClass() {
        ClassDescriptor descriptor = unrelatedRegistry.getRequiredDescriptor(String.class);
        var descriptors = new MapBackedIdIndexedDescriptors(int2ObjectMap(Map.of(descriptor.descriptorId(), descriptor)));

        assertThat(descriptors.getDescriptor(descriptor.descriptorId()), is(descriptor));
    }

    private Int2ObjectMap<ClassDescriptor> int2ObjectMap(Map<Integer, ClassDescriptor> map) {
        Int2ObjectMap<ClassDescriptor> result = new Int2ObjectOpenHashMap<>();
        result.putAll(map);
        return result;
    }

    @Test
    void doesNotFindAnythingByClassWhenMapDoesNotContainTheClassDescriptor() {
        var descriptors = new MapBackedIdIndexedDescriptors(int2ObjectMap(emptyMap()));

        assertThat(descriptors.getDescriptor(42), is(nullValue()));
    }

    @Test
    void throwsWhenQueriedAboutUnknownDescriptorByClass() {
        var descriptors = new MapBackedIdIndexedDescriptors(int2ObjectMap(emptyMap()));

        Throwable thrownEx = assertThrows(IllegalStateException.class, () -> descriptors.getRequiredDescriptor(42));
        assertThat(thrownEx.getMessage(), is("Did not find a descriptor with ID=42"));
    }
}
