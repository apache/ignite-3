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

/**
 * Handles situations when the schema that was used to serialize an object (remote schema) is different from the
 * schema used to deserialize the object (local schema).
 *
 * <p>Typically, a handler might either ignore the discrepancy (in which case the corresponding field will be skipped
 * and nothing will be assigned), or an exception can be thrown to stop the deserialization, or some recovery could be
 * performed.
 *
 * <p>Note that the handlers are designed to work with declaring classes, not with the concrete classes representing
 * the whole class lineage.
 */
public interface SchemaMismatchHandler<T> {
    /**
     * Called when a field is encountered in the serialized form (so the field was present on the remote side), but
     * in the local counterpart this field does not exist.
     *
     * @param instance   object being filled during deserialization
     * @param fieldName  name of the field that exists on the remote side but does not exist locally
     * @param fieldValue the field value that was read from the serialized representation
     * @throws SchemaMismatchException thrown if the handler wills to stop the deserialization with an error
     */
    default void onFieldIgnored(T instance, String fieldName, Object fieldValue) throws SchemaMismatchException {
        // no-op
    }

    /**
     * Called when a field is present locally, but not remotely, so it does not get any value from the serialized
     * representation.
     *
     * @param instance   object being filled during deserialization
     * @param fieldName  name of the field that exists locally but does not exist on the remote side
     * @throws SchemaMismatchException thrown if the handler wills to stop the deserialization with an error
     */
    default void onFieldMissed(T instance, String fieldName) throws SchemaMismatchException {
        // no-op
    }

    /**
     * Called when a field is present both locally and remotely, but its type is different (and not compatible).
     * A type is compatible if the local class is assignable from the remote class.
     *
     * @param instance   object being filled during deserialization
     * @param fieldName  name of the field
     * @param remoteType remote declared type of the field
     * @param fieldValue the field value that was read from the serialized representation
     * @throws SchemaMismatchException thrown if the handler wills to stop the deserialization with an error
     */
    default void onFieldTypeChanged(T instance, String fieldName, Class<?> remoteType, Object fieldValue) throws SchemaMismatchException {
        throw new SchemaMismatchException(fieldName + " type changed, serialized as " + remoteType.getName() + ", value " + fieldValue);
    }
}
