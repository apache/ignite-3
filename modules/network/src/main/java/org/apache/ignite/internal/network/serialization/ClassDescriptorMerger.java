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

import java.util.ArrayList;
import java.util.List;

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
}
