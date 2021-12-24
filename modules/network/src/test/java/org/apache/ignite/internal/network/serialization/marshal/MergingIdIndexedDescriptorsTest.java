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

package org.apache.ignite.internal.network.serialization.marshal;

import static java.util.Collections.emptyMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.util.Map;
import org.apache.ignite.internal.network.serialization.ClassDescriptor;
import org.apache.ignite.internal.network.serialization.MapBackedIdIndexedDescriptors;
import org.junit.jupiter.api.Test;

class MergingIdIndexedDescriptorsTest {
    private final ClassDescriptor descriptor = mock(ClassDescriptor.class);

    @Test
    void retrievesByIdWhenOnlyFirstBackendHasTheDescriptor() {
        var back1 = new MapBackedIdIndexedDescriptors(Map.of(42, descriptor));
        var back2 = new MapBackedIdIndexedDescriptors(emptyMap());

        var merger = new MergingIdIndexedDescriptors(back1, back2);

        assertThat(merger.getDescriptor(42), is(descriptor));
    }

    @Test
    void retrievesByIdWhenOnlySecondBackendHasTheDescriptor() {
        var back1 = new MapBackedIdIndexedDescriptors(emptyMap());
        var back2 = new MapBackedIdIndexedDescriptors(Map.of(42, descriptor));

        var merger = new MergingIdIndexedDescriptors(back1, back2);

        assertThat(merger.getDescriptor(42), is(descriptor));
    }

    @Test
    void doesNotFindAnythingWhenNoBackendContainsTheId() {
        var back1 = new MapBackedIdIndexedDescriptors(emptyMap());
        var back2 = new MapBackedIdIndexedDescriptors(emptyMap());

        var merger = new MergingIdIndexedDescriptors(back1, back2);

        assertThat(merger.getDescriptor(42), is(nullValue()));
    }

    @Test
    void throwsWhenLookinUpMandatoryDescriptorThatIsNotThere() {
        var back1 = new MapBackedIdIndexedDescriptors(emptyMap());
        var back2 = new MapBackedIdIndexedDescriptors(emptyMap());

        var merger = new MergingIdIndexedDescriptors(back1, back2);

        assertThrows(IllegalStateException.class, () -> merger.getRequiredDescriptor(42));
    }
}
