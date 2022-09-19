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

import static java.util.stream.Collectors.toSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Merges {@link ClassDescriptor} components.
 */
class ClassDescriptorMerger {
    static List<MergedField> mergeFields(List<FieldDescriptor> localFields, List<FieldDescriptor> remoteFields) {
        List<MergedField> mergedFields = new ArrayList<>();

        int remoteFieldCount = remoteFields.size();
        int localFieldCount = localFields.size();
        int remoteFieldIndex = 0;
        int localFieldIndex = 0;
        // fields are sorted by name in each ClassDescriptor
        while (remoteFieldIndex < remoteFieldCount && localFieldIndex < localFieldCount) {
            FieldDescriptor remoteField = remoteFields.get(remoteFieldIndex);
            FieldDescriptor localField = localFields.get(localFieldIndex);

            final MergedField mergedField;
            int remoteToLocal = remoteField.name().compareTo(localField.name());
            if (remoteToLocal > 0) {
                // there is no remote field, but there is local field
                mergedField = MergedField.localOnly(localField);
            } else if (remoteToLocal < 0) {
                // there is only a remote field
                mergedField = MergedField.remoteOnly(remoteField);
            } else {
                mergedField = new MergedField(localField, remoteField);
            }

            mergedFields.add(mergedField);

            if (remoteToLocal >= 0) {
                localFieldIndex++;
            }
            if (remoteToLocal <= 0) {
                remoteFieldIndex++;
            }
        }

        for (; remoteFieldIndex < remoteFieldCount; remoteFieldIndex++) {
            FieldDescriptor remoteField = remoteFields.get(remoteFieldIndex);
            mergedFields.add(MergedField.remoteOnly(remoteField));
        }
        for (; localFieldIndex < localFieldCount; localFieldIndex++) {
            FieldDescriptor localField = localFields.get(localFieldIndex);
            mergedFields.add(MergedField.localOnly(localField));
        }

        return List.copyOf(mergedFields);
    }

    static List<MergedLayer> mergeLineages(List<ClassDescriptor> localLineage, List<ClassDescriptor> remoteLineage) {
        Set<String> commonClassNames = classNames(localLineage);
        commonClassNames.retainAll(classNames(remoteLineage));

        Predicate<ClassDescriptor> isCommon = layer -> commonClassNames.contains(layer.className());

        List<MergedLayer> result = new ArrayList<>();

        int localIndex = 0;
        int remoteIndex = 0;

        while (localIndex < localLineage.size() && remoteIndex < remoteLineage.size()) {
            while (localIndex < localLineage.size() && !isCommon.test(localLineage.get(localIndex))) {
                result.add(MergedLayer.localOnly(localLineage.get(localIndex)));
                localIndex++;
            }
            while (remoteIndex < remoteLineage.size() && !isCommon.test(remoteLineage.get(remoteIndex))) {
                result.add(MergedLayer.remoteOnly(remoteLineage.get(remoteIndex)));
                remoteIndex++;
            }

            if (localIndex >= localLineage.size() || remoteIndex >= remoteLineage.size()) {
                break;
            }

            // in both lists, we are standing on descriptors with same class name
            ClassDescriptor localLayer = localLineage.get(localIndex);
            ClassDescriptor remoteLayer = remoteLineage.get(remoteIndex);
            result.add(new MergedLayer(localLayer, remoteLayer));

            localIndex++;
            remoteIndex++;
        }

        // tails
        while (localIndex < localLineage.size()) {
            result.add(MergedLayer.localOnly(localLineage.get(localIndex)));
            localIndex++;
        }
        while (remoteIndex < remoteLineage.size()) {
            result.add(MergedLayer.remoteOnly(remoteLineage.get(remoteIndex)));
            remoteIndex++;
        }

        return List.copyOf(result);
    }

    private static Set<String> classNames(List<ClassDescriptor> localLineage) {
        return localLineage.stream()
                .map(ClassDescriptor::className)
                .collect(toSet());
    }
}
