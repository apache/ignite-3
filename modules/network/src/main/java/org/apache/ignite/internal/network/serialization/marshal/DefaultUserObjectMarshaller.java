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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Method;
import java.util.List;
import org.apache.ignite.internal.network.serialization.ClassDescriptor;
import org.apache.ignite.internal.network.serialization.ClassDescriptorFactory;
import org.apache.ignite.internal.network.serialization.ClassDescriptorFactoryContext;

/**
 * Default implementation of {@link UserObjectMarshaller}.
 */
public class DefaultUserObjectMarshaller implements UserObjectMarshaller {
    private final ClassDescriptorFactoryContext descriptorRegistry;
    private final ClassDescriptorFactory descriptorFactory;

    public DefaultUserObjectMarshaller(ClassDescriptorFactoryContext descriptorRegistry, ClassDescriptorFactory descriptorFactory) {
        this.descriptorRegistry = descriptorRegistry;
        this.descriptorFactory = descriptorFactory;
    }

    /** {@inheritDoc} */
    @Override
    public MarshalledObject marshal(Object object) throws MarshalException {
        ClassDescriptor descriptor = getOrCreateDescriptor(object);

        var baos = new ByteArrayOutputStream();

        final List<ClassDescriptor> usedDescriptors;
        try (var dos = new DataOutputStream(baos)) {
            writeDescriptorId(descriptor, dos);

            usedDescriptors = writeObject(object, descriptor, dos);
        } catch (IOException e) {
            throw new MarshalException("Cannot marshal", e);
        }

        return new MarshalledObject(baos.toByteArray(), usedDescriptors);
    }

    private ClassDescriptor getOrCreateDescriptor(Object object) {
        ClassDescriptor descriptor = descriptorRegistry.getDescriptor(object.getClass());
        if (descriptor == null) {
            descriptor = descriptorFactory.create(object.getClass());
        }
        return descriptor;
    }

    private void writeDescriptorId(ClassDescriptor descriptor, DataOutputStream dos) throws IOException {
        dos.writeInt(descriptor.descriptorId());
    }

    private List<ClassDescriptor> writeObject(Object object, ClassDescriptor descriptor, DataOutputStream dos)
            throws IOException, MarshalException {
        final Object objectToWrite;
        if ((descriptor.isSerializable() || descriptor.isExternalizable()) && descriptor.hasWriteReplace()) {
            // TODO: IGNITE-16155 what if non-Externalizable is returned?
            objectToWrite = applyWriteReplace(object);
        } else {
            objectToWrite = object;
        }

        if (descriptor.isExternalizable()) {
            return writeExternalizable((Externalizable) objectToWrite, descriptor, dos);
        } else {
            throw new UnsupportedOperationException("Not supported yet");
        }
    }

    private Object applyWriteReplace(Object object) throws MarshalException {
        Method writeReplaceMethod;
        try {
            writeReplaceMethod = object.getClass().getDeclaredMethod("writeReplace");
        } catch (NoSuchMethodException e) {
            throw new MarshalException("writeReplace() was not found on " + object.getClass()
                    + " even though the descriptor says the class has the method", e);
        }

        writeReplaceMethod.setAccessible(true);

        try {
            return writeReplaceMethod.invoke(object);
        } catch (ReflectiveOperationException e) {
            throw new MarshalException("writeReplace() invocation failed on " + object, e);
        }

        // TODO: IGNITE-16155 what if null is returned?
    }

    private List<ClassDescriptor> writeExternalizable(
            Externalizable externalizable,
            ClassDescriptor descriptor,
            DataOutputStream dos
    ) throws IOException {
        byte[] externalizableBytes = externalize(externalizable);

        dos.writeInt(externalizableBytes.length);
        dos.write(externalizableBytes);

        return List.of(descriptor);
    }

    private byte[] externalize(Externalizable externalizable) throws IOException {
        var baos = new ByteArrayOutputStream();
        try (var oos = new ObjectOutputStream(baos)) {
            externalizable.writeExternal(oos);
        }

        return baos.toByteArray();
    }

    /** {@inheritDoc} */
    @Override
    public <T> T unmarshal(byte[] bytes) throws UnmarshalException {
        try (var dis = new DataInputStream(new ByteArrayInputStream(bytes))) {
            int descriptorId = readDescriptorId(dis);
            ClassDescriptor descriptor = descriptorRegistry.getRequiredDescriptor(descriptorId);

            if (descriptor.isExternalizable()) {
                Object readObject = readExternalizable(descriptor, dis);
                Object resultObject;
                if (descriptor.hasReadResolve()) {
                    resultObject = applyReadResolve(readObject);
                } else {
                    resultObject = readObject;
                }
                @SuppressWarnings("unchecked") T castResult = (T) resultObject;
                return castResult;
            } else {
                throw new UnsupportedOperationException("Not supported yet");
            }
        } catch (IOException | ClassNotFoundException e) {
            throw new UnmarshalException("Cannot unmarshal", e);
        }
    }

    private int readDescriptorId(DataInputStream dis) throws IOException {
        return dis.readInt();
    }

    private <T extends Externalizable> T readExternalizable(ClassDescriptor descriptor, DataInputStream dis)
            throws IOException, ClassNotFoundException, UnmarshalException {
        T object = instantiateObject(descriptor);

        int length = dis.readInt();
        byte[] bytes = new byte[length];
        dis.readFully(bytes);

        try (var ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            object.readExternal(ois);
        }

        return object;
    }

    @SuppressWarnings("unchecked")
    private <T extends Externalizable> T instantiateObject(ClassDescriptor descriptor) throws UnmarshalException {
        try {
            return (T) descriptor.clazz().getConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new UnmarshalException("Cannot instantiate " + descriptor.clazz(), e);
        }
    }

    private Object applyReadResolve(Object object) throws UnmarshalException {
        Method readResolveMethod;
        try {
            readResolveMethod = object.getClass().getDeclaredMethod("readResolve");
        } catch (NoSuchMethodException e) {
            throw new UnmarshalException("readResolve() was not found on " + object.getClass()
                    + " even though the descriptor says the class has the method", e);
        }

        readResolveMethod.setAccessible(true);

        try {
            return readResolveMethod.invoke(object);
        } catch (ReflectiveOperationException e) {
            throw new UnmarshalException("readResolve() invocation failed on " + object, e);
        }

        // TODO: IGNITE-16155 what if null is returned?
    }
}
