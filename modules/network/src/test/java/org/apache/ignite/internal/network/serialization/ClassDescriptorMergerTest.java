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

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;

class ClassDescriptorMergerTest extends BaseIgniteAbstractTest {
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

    @Test
    void mergesIdenticalLineages() {
        List<ClassDescriptor> local = descriptorsForClasses("A", "B");
        List<ClassDescriptor> remote = List.copyOf(local);

        List<MergedLayer> mergedLineage = ClassDescriptorMerger.mergeLineages(local, remote);

        assertThat(mergedLineage, hasSize(2));

        assertMergedLayerIsFull(mergedLineage.get(0), "A");
        assertMergedLayerIsFull(mergedLineage.get(1), "B");
    }

    private List<ClassDescriptor> descriptorsForClasses(String... classNames) {
        return Arrays.stream(classNames)
                .map(this::descriptorForClass)
                .collect(toUnmodifiableList());
    }

    private ClassDescriptor descriptorForClass(String className) {
        ClassDescriptor descriptor = mock(ClassDescriptor.class);

        when(descriptor.className()).thenReturn(className);

        return descriptor;
    }

    private void assertMergedLayerIsFull(MergedLayer mergedLayer, String expectedClassName) {
        assertThat(mergedLayer.hasLocal(), is(true));
        assertThat(mergedLayer.local().className(), is(expectedClassName));
        assertThat(mergedLayer.hasRemote(), is(true));
        assertThat(mergedLayer.remote().className(), is(expectedClassName));
    }

    @Test
    void mergesLocalLineageExcessInTheBeginningToLocalOnlyPrefix() {
        List<ClassDescriptor> local = descriptorsForClasses("A", "B");
        List<ClassDescriptor> remote = descriptorsForClasses("B");

        List<MergedLayer> mergedLineage = ClassDescriptorMerger.mergeLineages(local, remote);

        assertThat(mergedLineage, hasSize(2));

        assertMergedLayerIsLocalOnly(mergedLineage.get(0), "A");
        assertMergedLayerIsFull(mergedLineage.get(1), "B");
    }

    private void assertMergedLayerIsLocalOnly(MergedLayer mergedLayer, String expectedClassName) {
        assertThat(mergedLayer.hasLocal(), is(true));
        assertThat(mergedLayer.local().className(), is(expectedClassName));
        assertThat(mergedLayer.hasRemote(), is(false));
        NullPointerException ex = assertThrows(NullPointerException.class, mergedLayer::remote);
        assertThat(ex.getMessage(), is("remoteLayer is null"));
    }

    @Test
    void mergesLocalLineageExcessInTheEndToLocalOnlyPostfix() {
        List<ClassDescriptor> local = descriptorsForClasses("A", "B");
        List<ClassDescriptor> remote = descriptorsForClasses("A");

        List<MergedLayer> mergedLineage = ClassDescriptorMerger.mergeLineages(local, remote);

        assertThat(mergedLineage, hasSize(2));

        assertMergedLayerIsFull(mergedLineage.get(0), "A");
        assertMergedLayerIsLocalOnly(mergedLineage.get(1), "B");
    }

    @Test
    void mergesRemoteLineageExcessInTheBeginningToRemoteOnlyPrefix() {
        List<ClassDescriptor> local = descriptorsForClasses("B");
        List<ClassDescriptor> remote = descriptorsForClasses("A", "B");

        List<MergedLayer> mergedLineage = ClassDescriptorMerger.mergeLineages(local, remote);

        assertThat(mergedLineage, hasSize(2));

        assertMergedLayerIsRemoteOnly(mergedLineage.get(0), "A");
        assertMergedLayerIsFull(mergedLineage.get(1), "B");
    }

    private void assertMergedLayerIsRemoteOnly(MergedLayer mergedLayer, String expectedClassName) {
        assertThat(mergedLayer.hasRemote(), is(true));
        assertThat(mergedLayer.remote().className(), is(expectedClassName));
        assertThat(mergedLayer.hasLocal(), is(false));
        NullPointerException ex = assertThrows(NullPointerException.class, mergedLayer::local);
        assertThat(ex.getMessage(), is("localLayer is null"));
    }

    @Test
    void mergesRemoteLineageExcessInTheEndToRemoteOnlyPostfix() {
        List<ClassDescriptor> local = descriptorsForClasses("A");
        List<ClassDescriptor> remote = descriptorsForClasses("A", "B");

        List<MergedLayer> mergedLineage = ClassDescriptorMerger.mergeLineages(local, remote);

        assertThat(mergedLineage, hasSize(2));

        assertMergedLayerIsFull(mergedLineage.get(0), "A");
        assertMergedLayerIsRemoteOnly(mergedLineage.get(1), "B");
    }

    @Test
    void mergesLocalLineageExcessInTheMiddleToLocalOnlyMiddle() {
        List<ClassDescriptor> local = descriptorsForClasses("A", "B", "C");
        List<ClassDescriptor> remote = descriptorsForClasses("A", "C");

        List<MergedLayer> mergedLineage = ClassDescriptorMerger.mergeLineages(local, remote);

        assertThat(mergedLineage, hasSize(3));

        assertMergedLayerIsFull(mergedLineage.get(0), "A");
        assertMergedLayerIsLocalOnly(mergedLineage.get(1), "B");
        assertMergedLayerIsFull(mergedLineage.get(2), "C");
    }

    @Test
    void mergesRemoteLineageExcessInTheMiddleToRemoteOnlyMiddle() {
        List<ClassDescriptor> local = descriptorsForClasses("A", "C");
        List<ClassDescriptor> remote = descriptorsForClasses("A", "B", "C");

        List<MergedLayer> mergedLineage = ClassDescriptorMerger.mergeLineages(local, remote);

        assertThat(mergedLineage, hasSize(3));

        assertMergedLayerIsFull(mergedLineage.get(0), "A");
        assertMergedLayerIsRemoteOnly(mergedLineage.get(1), "B");
        assertMergedLayerIsFull(mergedLineage.get(2), "C");
    }

    @Test
    void mergesBothLineagesExcessInTheBeginningToCorrespondingPrefix() {
        List<ClassDescriptor> local = descriptorsForClasses("A", "B");
        List<ClassDescriptor> remote = descriptorsForClasses("X", "Y", "B");

        List<MergedLayer> mergedLineage = ClassDescriptorMerger.mergeLineages(local, remote);

        assertThat(mergedLineage, hasSize(4));

        assertMergedLayerIsLocalOnly(mergedLineage.get(0), "A");
        assertMergedLayerIsRemoteOnly(mergedLineage.get(1), "X");
        assertMergedLayerIsRemoteOnly(mergedLineage.get(2), "Y");
        assertMergedLayerIsFull(mergedLineage.get(3), "B");
    }

    @Test
    void mergesBothLineagesExcessInTheEndToCorrespondingPostfix() {
        List<ClassDescriptor> local = descriptorsForClasses("A", "B");
        List<ClassDescriptor> remote = descriptorsForClasses("A", "X", "Y");

        List<MergedLayer> mergedLineage = ClassDescriptorMerger.mergeLineages(local, remote);

        assertThat(mergedLineage, hasSize(4));

        assertMergedLayerIsFull(mergedLineage.get(0), "A");
        assertMergedLayerIsLocalOnly(mergedLineage.get(1), "B");
        assertMergedLayerIsRemoteOnly(mergedLineage.get(2), "X");
        assertMergedLayerIsRemoteOnly(mergedLineage.get(3), "Y");
    }

    @Test
    void mergesBothLineagesExcessInTheMiddleToCorrespondingPostfix() {
        List<ClassDescriptor> local = descriptorsForClasses("A", "B", "C");
        List<ClassDescriptor> remote = descriptorsForClasses("A", "X", "Y", "C");

        List<MergedLayer> mergedLineage = ClassDescriptorMerger.mergeLineages(local, remote);

        assertThat(mergedLineage, hasSize(5));

        assertMergedLayerIsFull(mergedLineage.get(0), "A");
        assertMergedLayerIsLocalOnly(mergedLineage.get(1), "B");
        assertMergedLayerIsRemoteOnly(mergedLineage.get(2), "X");
        assertMergedLayerIsRemoteOnly(mergedLineage.get(3), "Y");
        assertMergedLayerIsFull(mergedLineage.get(4), "C");
    }

    private static class FieldHost {
        @SuppressWarnings("unused")
        private int test1;
        @SuppressWarnings("unused")
        private int test2;
    }
}
