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

/**
 * A representation of a declared type.
 */
public interface DeclaredType {
    /**
     * Returns type descriptor id.
     *
     * @return type descriptor id.
     */
    int typeDescriptorId();

    /**
     * Returns {@code true} if the contents of the type slot can only have (at runtime) instances serialized as its declared type
     * (and not subtypes or other types coming from write replacement),
     * so the serialization type is known upfront. This is also true for enums, even though technically their values might
     * have subtypes; but we serialize them using their names, so we still treat the type as known upfront.
     */
    boolean isSerializationTypeKnownUpfront();
}
