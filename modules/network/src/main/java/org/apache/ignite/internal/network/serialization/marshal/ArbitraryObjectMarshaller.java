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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Field;
import java.util.function.BiFunction;
import org.apache.ignite.internal.network.serialization.ClassDescriptor;
import org.apache.ignite.internal.network.serialization.ClassIndexedDescriptors;
import org.apache.ignite.internal.network.serialization.FieldDescriptor;

/**
 * (Un)marshals arbitrary objects (that is, objects that are not built-in nor serializable/externalizable).
 */
class ArbitraryObjectMarshaller {
    private final TypedValueWriter valueWriter;
    private final ValueReader<Object> valueReader;

    private final Instantiation instantiation;

    ArbitraryObjectMarshaller(ClassIndexedDescriptors descriptors, TypedValueWriter valueWriter, ValueReader<Object> valueReader) {
        this.valueWriter = valueWriter;
        this.valueReader = valueReader;

        instantiation = new BestEffortInstantiation(
                new NoArgConstructorInstantiation(),
                new SerializableInstantiation(descriptors),
                new UnsafeInstantiation()
        );
    }

    void writeArbitraryObject(Object object, ClassDescriptor descriptor, DataOutput output, MarshallingContext context)
            throws MarshalException, IOException {
        context.addUsedDescriptor(descriptor);

        for (FieldDescriptor fieldDescriptor : descriptor.fields()) {
            writeField(object, fieldDescriptor, output, context);
        }
    }

    private void writeField(Object object, FieldDescriptor fieldDescriptor, DataOutput output, MarshallingContext context)
            throws MarshalException, IOException {
        Object fieldValue = getFieldValue(object, fieldDescriptor);

        valueWriter.write(fieldValue, fieldDescriptor.clazz(), output, context);
    }

    private Object getFieldValue(Object object, FieldDescriptor fieldDescriptor) throws MarshalException {
        VarHandle varHandle = varHandleFrom(fieldDescriptor, MarshalException::new);
        return varHandle.get(object);
    }

    private <X extends Exception> VarHandle varHandleFrom(
            FieldDescriptor fieldDescriptor,
            BiFunction<String, Exception, X> exceptionConverter
    ) throws X {
        try {
            Field field = findField(fieldDescriptor);
            Lookup lookup = MethodHandles.privateLookupIn(fieldDescriptor.declaringClass(), MethodHandles.lookup());
            return lookup.unreflectVarHandle(field);
        } catch (ReflectiveOperationException e) {
            throw exceptionConverter.apply("Cannot get a field VarHandle", e);
        }
    }

    private Field findField(FieldDescriptor fieldDescriptor) throws NoSuchFieldException {
        return fieldDescriptor.declaringClass().getDeclaredField(fieldDescriptor.name());
    }

    Object preInstantiateArbitraryObject(ClassDescriptor descriptor) throws UnmarshalException {
        try {
            return instantiation.newInstance(descriptor.clazz());
        } catch (InstantiationException e) {
            throw new UnmarshalException("Cannot instantiate " + descriptor.clazz(), e);
        }
    }

    void fillArbitraryObjectFrom(DataInput input, Object object, ClassDescriptor descriptor, UnmarshallingContext context)
            throws IOException, UnmarshalException {
        for (FieldDescriptor fieldDescriptor : descriptor.fields()) {
            Object fieldValue = valueReader.read(input, context);
            setFieldValue(object, fieldDescriptor, fieldValue);
        }
    }

    private void setFieldValue(Object target, FieldDescriptor fieldDescriptor, Object value) throws UnmarshalException {
        // Using Field directly and not a VarHandle because VarHandle does not to write a value to a final field.

        try {
            Field field = findField(fieldDescriptor);
            field.setAccessible(true);
            field.set(target, value);
        } catch (ReflectiveOperationException e) {
            throw new UnmarshalException("Cannot set field value", e);
        }
    }
}
