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

package org.apache.ignite.internal.network.serialization.marshal;

import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;

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
@SuppressWarnings({"RedundantThrows", "unused"})
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
        // if field type has changed in an inconvertible way, we cannot do anything with it, let's throw an exception
        throw new SchemaMismatchException(fieldName + " type changed, serialized as " + remoteType.getName() + ", value " + fieldValue
                + " of type " + fieldName.getClass().getName());
    }

    /**
     * Called when a remote class implements {@link java.io.Externalizable}, but local class does not.
     *
     * @param instance      the object that was constructed, but not yet filled
     * @param externalData  externalized data that represents the object (it was written
     *                      using {@link java.io.Externalizable#writeExternal(ObjectOutput)}
     * @throws SchemaMismatchException thrown if the handler wills to stop the deserialization with an error
     */
    default void onExternalizableIgnored(T instance, ObjectInput externalData) throws SchemaMismatchException {
        // task-specific handling is required, so by default just throw an exception
        throw new SchemaMismatchException("Class " + instance.getClass().getName()
                + " was serialized as an Externalizable remotely, but locally it is not an Externalizable");
    }

    /**
     * Called when a remote class does not implement {@link java.io.Externalizable}, but local class does. The method is called after
     * all read fields are assigned to the instance.
     *
     * @param instance the instance that has already been filled
     * @throws SchemaMismatchException thrown if the handler wills to stop the deserialization with an error
     */
    default void onExternalizableMissed(T instance) throws SchemaMismatchException {
        // the instance is actually filled with field data, so it seems that it's not too dangerous to ignore this by default
    }

    /**
     * Called when a local class does not have an active readResolve() method (that is, it is neither {@link java.io.Serializable} nor
     * {@link java.io.Externalizable} OR it has no readResolve() method at all), but the remote class does have it active
     * (that is, the remote class is {@link java.io.Serializable} or {@link java.io.Externalizable} AND has readResolve()).
     * The invocation happens after the instance has been filled.
     *
     * @param instance instance that is being deserialized
     * @throws SchemaMismatchException thrown if the handler wills to stop the deserialization with an error
     */
    default void onReadResolveDisappeared(T instance) throws SchemaMismatchException {
        // no-op
    }

    /**
     * Called when a remote class does not have an active readResolve() method (that is, it is neither {@link java.io.Serializable} nor
     * {@link java.io.Externalizable} OR it has no readResolve() method at all), but the local class does have it active
     * (that is, the local class is {@link java.io.Serializable} or {@link java.io.Externalizable} AND has readResolve()).
     * The invocation happens after the instance has been filled, but before its readResolve() is applied.
     * The result of this call defines whether readResolve() should be applied.
     *
     * @param instance instance that is being deserialized
     * @return if {@code true}, then readResolve() will be applied by the deserializer, otherwise it will not
     * @throws SchemaMismatchException thrown if the handler wills to stop the deserialization with an error
     */
    default boolean onReadResolveAppeared(T instance) throws SchemaMismatchException {
        // readResolve() is here, so just allow it to be invoked by default; it does not look dangerous
        return true;
    }

    /**
     * Called when a remote class has an active writeObject() method (that is, the method exists and the class implements
     * {@link java.io.Serializable}, but local class does not have an active readObject() method (that is, the local class
     * is not {@link java.io.Serializable} or it does not contain readObject() method).
     *
     * @param instance      the object that was constructed, but the current layer is not yet filled
     * @param objectData    data written by writeObject() remotely
     * @throws SchemaMismatchException thrown if the handler wills to stop the deserialization with an error
     */
    default void onReadObjectIgnored(T instance, ObjectInputStream objectData) throws SchemaMismatchException {
        // task-specific handling is required, so by default just throw an exception
        throw new SchemaMismatchException("Class " + instance.getClass().getName()
                + " was serialized with writeObject() remotely, but locally it has no readObject()");
    }

    /**
     * Called when a local class has an active readObject() method (that is, the method exists and the class implements
     * {@link java.io.Serializable}, but remote class does not have an active writeObject() method (that is, the remote class
     * is not {@link java.io.Serializable} or it does not contain writeObject() method).
     *
     * @param instance the instance layers of which (including the current one) are already filled
     * @throws SchemaMismatchException thrown if the handler wills to stop the deserialization with an error
     */
    default void onReadObjectMissed(T instance) throws SchemaMismatchException {
        // the instance is actually filled with field data, so it seems that it's not too dangerous to ignore this by default
    }
}
