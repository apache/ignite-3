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

import java.io.IOException;
import java.io.NotActiveException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.BitSet;
import org.apache.ignite.internal.network.serialization.ClassDescriptor;
import org.apache.ignite.internal.network.serialization.DeclaredType;
import org.apache.ignite.internal.network.serialization.FieldDescriptor;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.jetbrains.annotations.Nullable;

/**
 * {@link ObjectOutputStream} specialization used by User Object Serialization.
 */
class UosObjectOutputStream extends ObjectOutputStream {
    private final IgniteDataOutput output;
    private final TypedValueWriter valueWriter;
    private final TypedValueWriter unsharedWriter;
    private final DefaultFieldsReaderWriter defaultFieldsReaderWriter;
    private final MarshallingContext context;

    private UosPutField currentPut;

    UosObjectOutputStream(
            IgniteDataOutput output,
            TypedValueWriter valueWriter,
            TypedValueWriter unsharedWriter, DefaultFieldsReaderWriter defaultFieldsReaderWriter,
            MarshallingContext context
    ) throws IOException {
        this.output = output;
        this.valueWriter = valueWriter;
        this.unsharedWriter = unsharedWriter;
        this.defaultFieldsReaderWriter = defaultFieldsReaderWriter;
        this.context = context;
    }

    /** {@inheritDoc} */
    @Override
    public void write(int val) throws IOException {
        output.write(val);
    }

    /** {@inheritDoc} */
    @Override
    public void write(byte[] buf) throws IOException {
        output.write(buf);
    }

    /** {@inheritDoc} */
    @Override
    public void write(byte[] buf, int off, int len) throws IOException {
        output.write(buf, off, len);
    }

    /** {@inheritDoc} */
    @Override
    public void writeByte(int val) throws IOException {
        output.writeByte(val);
    }

    /** {@inheritDoc} */
    @Override
    public void writeShort(int val) throws IOException {
        output.writeShort(val);
    }

    /** {@inheritDoc} */
    @Override
    public void writeInt(int val) throws IOException {
        output.writeInt(val);
    }

    /** {@inheritDoc} */
    @Override
    public void writeLong(long val) throws IOException {
        output.writeLong(val);
    }

    /** {@inheritDoc} */
    @Override
    public void writeFloat(float val) throws IOException {
        output.writeFloat(val);
    }

    /** {@inheritDoc} */
    @Override
    public void writeDouble(double val) throws IOException {
        output.writeDouble(val);
    }

    /** {@inheritDoc} */
    @Override
    public void writeChar(int val) throws IOException {
        output.writeChar(val);
    }

    /** {@inheritDoc} */
    @Override
    public void writeBoolean(boolean val) throws IOException {
        output.writeBoolean(val);
    }

    /** {@inheritDoc} */
    @Override
    public void writeBytes(String str) throws IOException {
        output.writeBytes(str);
    }

    /** {@inheritDoc} */
    @Override
    public void writeChars(String str) throws IOException {
        output.writeChars(str);
    }

    /** {@inheritDoc} */
    @Override
    public void writeUTF(String str) throws IOException {
        output.writeUTF(str);
    }

    /** {@inheritDoc} */
    @Override
    protected void writeObjectOverride(Object obj) throws IOException {
        doWriteObjectOfAnyType(obj);
    }

    private void doWriteObjectOfAnyType(Object obj) throws IOException {
        doWriteObject(obj, null);
    }

    private void doWriteObject(Object obj, DeclaredType declaredClass) throws IOException {
        try {
            valueWriter.write(obj, declaredClass, output, context);
        } catch (MarshalException e) {
            throw new UncheckedMarshalException("Cannot write an object", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void writeUnshared(Object obj) throws IOException {
        doWriteUnshared(obj);
    }

    private void doWriteUnshared(Object obj) throws IOException {
        try {
            unsharedWriter.write(obj, null, output, context);
        } catch (MarshalException e) {
            throw new UncheckedMarshalException("Cannot write an unshared object", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void defaultWriteObject() throws IOException {
        try {
            defaultFieldsReaderWriter.defaultWriteFields(
                    context.objectCurrentlyWrittenWithWriteObject(),
                    context.descriptorOfObjectCurrentlyWrittenWithWriteObject(),
                    output,
                    context
            );
        } catch (MarshalException e) {
            throw new UncheckedMarshalException("Cannot write fields in a default way", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public PutField putFields() {
        if (currentPut == null) {
            currentPut = new UosPutField(context.descriptorOfObjectCurrentlyWrittenWithWriteObject());
        }
        return currentPut;
    }

    /** {@inheritDoc} */
    @Override
    public void writeFields() throws IOException {
        if (currentPut == null) {
            throw new NotActiveException("no current PutField object");
        }
        currentPut.write(this);
    }

    /** {@inheritDoc} */
    @Override
    public void useProtocolVersion(int version) {
        // no op
    }

    /** {@inheritDoc} */
    @Override
    public void reset() throws IOException {
        throw new UnsupportedOperationException("The correct way to reset is via MarshallingContext."
                + " Note that it's not valid to call this from writeObject()/readObject() implementation.");
    }

    /** {@inheritDoc} */
    @Override
    public void flush() throws IOException {
        output.flush();
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
        flush();
    }

    UosPutField replaceCurrentPutFieldWithNull() {
        UosPutField oldPut = currentPut;
        currentPut = null;
        return oldPut;
    }

    void restoreCurrentPutFieldTo(UosPutField newPut) {
        currentPut = newPut;
    }

    int memoryBufferOffset() {
        return output.offset();
    }

    void writeIntAtOffset(int offset, int value) throws IOException {
        int oldOffset = output.offset();

        try {
            output.offset(offset);
            output.writeInt(value);
        } finally {
            output.offset(oldOffset);
        }
    }

    class UosPutField extends PutField {
        private final ClassDescriptor descriptor;

        private final byte[] primitiveFieldsData;
        private final Object[] objectFieldVals;

        private final NullsBitsetWriter nullsBitsetWriter = new PutFieldNullsBitsetWriter(this);

        private UosPutField(ClassDescriptor currentObjectDescriptor) {
            this.descriptor = currentObjectDescriptor;

            primitiveFieldsData = new byte[currentObjectDescriptor.primitiveFieldsDataSize()];
            objectFieldVals = new Object[currentObjectDescriptor.objectFieldsCount()];
        }

        /** {@inheritDoc} */
        @Override
        public void put(String name, boolean val) {
            LittleEndianBits.putBoolean(primitiveFieldsData, primitiveFieldDataOffset(name, boolean.class), val);
        }

        /** {@inheritDoc} */
        @Override
        public void put(String name, byte val) {
            primitiveFieldsData[primitiveFieldDataOffset(name, byte.class)] = val;
        }

        /** {@inheritDoc} */
        @Override
        public void put(String name, char val) {
            LittleEndianBits.putChar(primitiveFieldsData, primitiveFieldDataOffset(name, char.class), val);
        }

        /** {@inheritDoc} */
        @Override
        public void put(String name, short val) {
            LittleEndianBits.putShort(primitiveFieldsData, primitiveFieldDataOffset(name, short.class), val);
        }

        /** {@inheritDoc} */
        @Override
        public void put(String name, int val) {
            LittleEndianBits.putInt(primitiveFieldsData, primitiveFieldDataOffset(name, int.class), val);
        }

        /** {@inheritDoc} */
        @Override
        public void put(String name, long val) {
            LittleEndianBits.putLong(primitiveFieldsData, primitiveFieldDataOffset(name, long.class), val);
        }

        /** {@inheritDoc} */
        @Override
        public void put(String name, float val) {
            LittleEndianBits.putFloat(primitiveFieldsData, primitiveFieldDataOffset(name, float.class), val);
        }

        /** {@inheritDoc} */
        @Override
        public void put(String name, double val) {
            LittleEndianBits.putDouble(primitiveFieldsData, primitiveFieldDataOffset(name, double.class), val);
        }

        /** {@inheritDoc} */
        @Override
        public void put(String name, Object val) {
            objectFieldVals[objectFieldIndex(name)] = val;
        }

        private int primitiveFieldDataOffset(String fieldName, Class<?> requiredType) {
            return descriptor.primitiveFieldDataOffset(fieldName, requiredType.getName());
        }

        private int objectFieldIndex(String fieldName) {
            return descriptor.objectFieldIndex(fieldName);
        }

        /** {@inheritDoc} */
        @Override
        public void write(ObjectOutput out) throws IOException {
            if (out != UosObjectOutputStream.this) {
                throw new IllegalArgumentException("This is not my output: " + out);
            }

            @Nullable BitSet nullsBitSet = nullsBitsetWriter.writeNullsBitSet(
                    context.objectCurrentlyWrittenWithWriteObject(),
                    descriptor,
                    output
            );

            int objectFieldIndex = 0;
            for (FieldDescriptor fieldDesc : descriptor.fields()) {
                objectFieldIndex = writeNext(fieldDesc, objectFieldIndex, out, nullsBitSet);
            }
        }

        private int writeNext(FieldDescriptor fieldDesc, int objectFieldIndex, ObjectOutput out, @Nullable BitSet nullsBitSet)
                throws IOException {
            if (fieldDesc.isPrimitive()) {
                writePrimitive(out, fieldDesc);
                return objectFieldIndex;
            } else {
                return writeObject(fieldDesc, objectFieldIndex, nullsBitSet);
            }
        }

        private void writePrimitive(ObjectOutput out, FieldDescriptor fieldDesc) throws IOException {
            int offset = descriptor.primitiveFieldDataOffset(fieldDesc.name(), fieldDesc.typeName());
            int length = fieldDesc.primitiveWidthInBytes();
            out.write(primitiveFieldsData, offset, length);
        }

        private int writeObject(FieldDescriptor fieldDesc, int objectFieldIndex, @Nullable BitSet nullsBitSet) throws IOException {
            Object objectToWrite = objectFieldVals[objectFieldIndex];

            if (StructuredObjectMarshaller.cannotAvoidWritingNull(fieldDesc, descriptor, nullsBitSet)) {
                if (fieldDesc.isUnshared()) {
                    doWriteUnshared(objectToWrite);
                } else {
                    doWriteObject(objectToWrite, fieldDesc);
                }
            }

            return objectFieldIndex + 1;
        }

        private Object objectFieldValue(String fieldName) {
            return objectFieldVals[objectFieldIndex(fieldName)];
        }
    }

    private static class PutFieldNullsBitsetWriter extends NullsBitsetWriter {
        private final UosPutField putField;

        private PutFieldNullsBitsetWriter(UosPutField putField) {
            this.putField = putField;
        }

        /** {@inheritDoc} */
        @Override
        Object getFieldValue(Object target, FieldDescriptor fieldDescriptor) {
            return putField.objectFieldValue(fieldDescriptor.name());
        }
    }
}
