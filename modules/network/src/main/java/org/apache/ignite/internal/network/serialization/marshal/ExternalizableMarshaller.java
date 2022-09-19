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

import java.io.Externalizable;
import java.io.IOException;
import org.apache.ignite.internal.network.serialization.ClassDescriptor;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.apache.ignite.internal.util.io.IgniteUnsafeDataInput;

/**
 * (Um)marshalling specific to EXTERNALIZABLE serialization type.
 */
class ExternalizableMarshaller {
    private final TypedValueReader valueReader;
    private final TypedValueReader unsharedReader;
    private final TypedValueWriter valueWriter;
    private final TypedValueWriter unsharedWriter;
    private final DefaultFieldsReaderWriter defaultFieldsReaderWriter;

    private final SchemaMismatchHandlers schemaMismatchHandlers;

    private final NoArgConstructorInstantiation instantiation = new NoArgConstructorInstantiation();

    ExternalizableMarshaller(
            TypedValueWriter typedValueWriter,
            TypedValueWriter unsharedWriter,
            TypedValueReader valueReader,
            TypedValueReader unsharedReader,
            DefaultFieldsReaderWriter defaultFieldsReaderWriter,
            SchemaMismatchHandlers schemaMismatchHandlers
    ) {
        this.valueWriter = typedValueWriter;
        this.unsharedWriter = unsharedWriter;
        this.valueReader = valueReader;
        this.unsharedReader = unsharedReader;
        this.defaultFieldsReaderWriter = defaultFieldsReaderWriter;
        this.schemaMismatchHandlers = schemaMismatchHandlers;
    }

    void writeExternalizable(Externalizable externalizable, ClassDescriptor descriptor, IgniteDataOutput output, MarshallingContext context)
            throws IOException {
        externalizeTo(externalizable, output, context);

        context.addUsedDescriptor(descriptor);
    }

    private void externalizeTo(Externalizable externalizable, IgniteDataOutput output, MarshallingContext context)
            throws IOException {
        // Do not close the stream yet!
        UosObjectOutputStream oos = context.objectOutputStream(output, valueWriter, unsharedWriter, defaultFieldsReaderWriter);

        UosObjectOutputStream.UosPutField oldPut = oos.replaceCurrentPutFieldWithNull();
        context.endWritingWithWriteObject();

        try {
            writeWithLength(externalizable, oos);
        } finally {
            oos.restoreCurrentPutFieldTo(oldPut);
        }
    }

    private void writeWithLength(Externalizable externalizable, UosObjectOutputStream oos) throws IOException {
        // NB: this only works with purely in-memory IgniteDataInput implementations!

        int offsetBefore = oos.memoryBufferOffset();

        writeLengthPlaceholder(oos);

        externalizable.writeExternal(oos);
        oos.flush();

        int externalDataLength = oos.memoryBufferOffset() - offsetBefore - Integer.BYTES;

        oos.writeIntAtOffset(offsetBefore, externalDataLength);
    }

    private void writeLengthPlaceholder(UosObjectOutputStream oos) throws IOException {
        oos.writeInt(0);
    }

    Object preInstantiateExternalizable(ClassDescriptor descriptor) throws UnmarshalException {
        try {
            return instantiation.newInstance(descriptor.localClass());
        } catch (InstantiationException e) {
            throw new UnmarshalException("Cannot instantiate " + descriptor.className(), e);
        }
    }

    void fillFromRemotelyExternalizable(IgniteDataInput input, Object object, UnmarshallingContext context)
            throws UnmarshalException, IOException {
        if (object instanceof Externalizable) {
            fillExternalizableFrom(input, (Externalizable) object, context);
        } else {
            // it was serialized as an Externalizable, but locally it is not Externalizable; delegate to handler
            fireExternalizableIgnored(object, input, context);
        }
    }

    private <T extends Externalizable> void fillExternalizableFrom(IgniteDataInput input, T object, UnmarshallingContext context)
            throws IOException, UnmarshalException {
        // Do not close the stream yet!
        UosObjectInputStream ois = context.objectInputStream(input, valueReader, unsharedReader, defaultFieldsReaderWriter);

        UosObjectInputStream.UosGetField oldGet = ois.replaceCurrentGetFieldWithNull();
        context.endReadingWithReadObject();

        try {
            readWithLength(object, ois);
        } catch (ClassNotFoundException e) {
            throw new UnmarshalException("Cannot unmarshal due to a missing class", e);
        } finally {
            ois.restoreCurrentGetFieldTo(oldGet);
        }
    }

    private <T extends Externalizable> void readWithLength(T object, UosObjectInputStream ois) throws IOException, ClassNotFoundException {
        skipExternalDataLength(ois);

        object.readExternal(ois);
    }

    private void skipExternalDataLength(UosObjectInputStream ois) throws IOException {
        ois.readInt();
    }

    private void fireExternalizableIgnored(Object object, IgniteDataInput input, UnmarshallingContext context)
            throws SchemaMismatchException, IOException {
        // We have additional allocations and copying here. It simplifies the code a lot, and it seems that we should
        // not optimize for this rare corner case.

        int externalDataLength = input.readInt();
        byte[] externalDataBytes = input.readByteArray(externalDataLength);
        IgniteDataInput externalDataInput = new IgniteUnsafeDataInput(externalDataBytes);

        try (var oos = new UosObjectInputStream(externalDataInput, valueReader, unsharedReader, defaultFieldsReaderWriter, context)) {
            schemaMismatchHandlers.onExternalizableIgnored(object, oos);
        }
    }
}
