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

package org.apache.ignite.internal.network.serialization;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import java.util.List;
import org.junit.jupiter.api.Test;

class ClassDescriptorMergerTest {
    @Test
    void mergesMatchingFields() {
        List<FieldDescriptor> localFields = List.of(
                fieldDescriptor("test1")
        );
        List<FieldDescriptor> remoteFields = List.of(
                fieldDescriptor("test1")
        );

        List<MergedField> mergedFields = ClassDescriptorMerger.mergeFields(localFields, remoteFields);

        assertThat(mergedFields, hasSize(1));
        assertThat(mergedFields.get(0).hasLocal(), is(true));
        assertThat(mergedFields.get(0).hasRemote(), is(true));
    }

    private FieldDescriptor fieldDescriptor(String name) {
        return FieldDescriptor.local(name, int.class, 1, false, FieldHost.class);
    }

    @Test
    void mergesWhenLocallyMissingFieldIsInTheBeginning() {
        List<FieldDescriptor> localFields = List.of(
                fieldDescriptor("test2")
        );
        List<FieldDescriptor> remoteFields = List.of(
                fieldDescriptor("test1"),
                fieldDescriptor("test2")
        );

        List<MergedField> mergedFields = ClassDescriptorMerger.mergeFields(localFields, remoteFields);

        assertThat(mergedFields, hasSize(2));

        assertThat(mergedFields.get(0).name(), is("test1"));
        assertThat(mergedFields.get(0).hasLocal(), is(false));
        assertThat(mergedFields.get(0).hasRemote(), is(true));

        assertThat(mergedFields.get(1).name(), is("test2"));
        assertThat(mergedFields.get(1).hasLocal(), is(true));
        assertThat(mergedFields.get(1).hasRemote(), is(true));
    }

    @Test
    void mergesWhenLocallyMissingFieldIsInTheEnd() {
        List<FieldDescriptor> localFields = List.of(
                fieldDescriptor("test1")
        );
        List<FieldDescriptor> remoteFields = List.of(
                fieldDescriptor("test1"),
                fieldDescriptor("test2")
        );

        List<MergedField> mergedFields = ClassDescriptorMerger.mergeFields(localFields, remoteFields);

        assertThat(mergedFields, hasSize(2));

        assertThat(mergedFields.get(0).name(), is("test1"));
        assertThat(mergedFields.get(0).hasLocal(), is(true));
        assertThat(mergedFields.get(0).hasRemote(), is(true));

        assertThat(mergedFields.get(1).name(), is("test2"));
        assertThat(mergedFields.get(1).hasLocal(), is(false));
        assertThat(mergedFields.get(1).hasRemote(), is(true));
    }

    @Test
    void mergesWhenRemotelyMissingFieldIsInTheBeginning() {
        List<FieldDescriptor> localFields = List.of(
                fieldDescriptor("test1"),
                fieldDescriptor("test2")
        );
        List<FieldDescriptor> remoteFields = List.of(
                fieldDescriptor("test2")
        );

        List<MergedField> mergedFields = ClassDescriptorMerger.mergeFields(localFields, remoteFields);

        assertThat(mergedFields, hasSize(2));

        assertThat(mergedFields.get(0).name(), is("test1"));
        assertThat(mergedFields.get(0).hasLocal(), is(true));
        assertThat(mergedFields.get(0).hasRemote(), is(false));

        assertThat(mergedFields.get(1).name(), is("test2"));
        assertThat(mergedFields.get(1).hasLocal(), is(true));
        assertThat(mergedFields.get(1).hasRemote(), is(true));
    }

    @Test
    void mergesWhenRemotelyMissingFieldIsInTheEnd() {
        List<FieldDescriptor> localFields = List.of(
                fieldDescriptor("test1"),
                fieldDescriptor("test2")
        );
        List<FieldDescriptor> remoteFields = List.of(
                fieldDescriptor("test1")
        );

        List<MergedField> mergedFields = ClassDescriptorMerger.mergeFields(localFields, remoteFields);

        assertThat(mergedFields, hasSize(2));

        assertThat(mergedFields.get(0).name(), is("test1"));
        assertThat(mergedFields.get(0).hasLocal(), is(true));
        assertThat(mergedFields.get(0).hasRemote(), is(true));

        assertThat(mergedFields.get(1).name(), is("test2"));
        assertThat(mergedFields.get(1).hasLocal(), is(true));
        assertThat(mergedFields.get(1).hasRemote(), is(false));
    }

    private static class FieldHost {
        @SuppressWarnings("unused")
        private int test1;
        @SuppressWarnings("unused")
        private int test2;
    }
}
