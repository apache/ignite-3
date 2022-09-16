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

import java.util.Objects;
import org.jetbrains.annotations.Nullable;

/**
 * Contains information about a field, both from local and remote classes. Any of the parts (local/remote) might be absent.
 */
public class MergedField {
    private final FieldDescriptor localField;
    private final FieldDescriptor remoteField;

    /**
     * Creates a {@link MergedField} for the case when there is only local part.
     *
     * @param localField local part
     * @return merged field
     */
    public static MergedField localOnly(FieldDescriptor localField) {
        return new MergedField(localField, null);
    }

    /**
     * Creates a {@link MergedField} for the case when there is only remote part.
     *
     * @param remoteField remote part
     * @return merged field
     */
    public static MergedField remoteOnly(FieldDescriptor remoteField) {
        return new MergedField(null, remoteField);
    }

    /**
     * Creates a new {@link MergedField}.
     *
     * @param localField  local part
     * @param remoteField remote part
     */
    public MergedField(@Nullable FieldDescriptor localField, @Nullable FieldDescriptor remoteField) {
        assert localField != null || remoteField != null : "Both descriptors are null";
        assert localField == null || remoteField == null || localField.name().equals(remoteField.name())
                : "Field descriptors with different names: " + localField.name() + " and " + remoteField.name();

        this.localField = localField;
        this.remoteField = remoteField;
    }

    /**
     * Whether there is a remote part.
     *
     * @return whether there is a remote part
     */
    public boolean hasRemote() {
        return remoteField != null;
    }

    /**
     * Returns remote part if it exists.
     *
     * @return remote part
     * @throws NullPointerException if there is no remote part.
     */
    public FieldDescriptor remote() {
        return Objects.requireNonNull(remoteField, "remoteField is null");
    }

    /**
     * Whether there is a local part.
     *
     * @return whether there is a local part
     */
    public boolean hasLocal() {
        return localField != null;
    }

    /**
     * Returns local part if it exists.
     *
     * @return local part
     * @throws NullPointerException if there is no local part.
     */
    public FieldDescriptor local() {
        return Objects.requireNonNull(localField, "localField is null");
    }

    /**
     * Returns field name.
     *
     * @return field name
     */
    public String name() {
        return localField != null ? localField.name() : remoteField.name();
    }

    /**
     * Whether local and remote parts are different.
     *
     * @return whether local and remote parts are different
     */
    public boolean typesAreDifferent() {
        return local().localClass() != remote().localClass();
    }

    /**
     * Whether local and remote parts are compatible (that is, a value of remote type can be loselessly converted to the local type).
     *
     * @return whether local and remote parts are different
     */
    public boolean typesAreCompatible() {
        return local().localClass().isAssignableFrom(remote().localClass());
    }

    /**
     * Converts the given value to the local type.
     *
     * @param fieldValue value to convert
     * @return converted value
     */
    public Object convertToLocalType(Object fieldValue) {
        return local().localClass().cast(fieldValue);
    }
}
