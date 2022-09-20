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

import java.io.DataInput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.util.BitSet;
import org.apache.ignite.internal.network.serialization.ClassDescriptor;
import org.apache.ignite.internal.network.serialization.DeclaredType;
import org.apache.ignite.internal.network.serialization.FieldDescriptor;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.jetbrains.annotations.Nullable;

/**
 * {@link ObjectInputStream} specialization used by User Object Serialization.
 */
class UosObjectInputStream extends ObjectInputStream {
    private final IgniteDataInput input;
    private final TypedValueReader valueReader;
    private final TypedValueReader unsharedReader;
    private final DefaultFieldsReaderWriter defaultFieldsReaderWriter;
    private final UnmarshallingContext context;

    private UosGetField currentGet;

    UosObjectInputStream(
            IgniteDataInput input,
            TypedValueReader valueReader,
            TypedValueReader unsharedReader,
            DefaultFieldsReaderWriter defaultFieldsReaderWriter,
            UnmarshallingContext context
    ) throws IOException {
        this.input = input;
        this.valueReader = valueReader;
        this.unsharedReader = unsharedReader;
        this.defaultFieldsReaderWriter = defaultFieldsReaderWriter;
        this.context = context;
    }

    /** {@inheritDoc} */
    @Override
    public int read() throws IOException {
        return input.read();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("NullableProblems")
    @Override
    public int read(byte[] buf) throws IOException {
        return input.read(buf);
    }

    /** {@inheritDoc} */
    @Override
    public int read(byte[] buf, int off, int len) throws IOException {
        return input.read(buf, off, len);
    }

    @Override
    public byte[] readAllBytes() throws IOException {
        return super.readAllBytes();
    }

    /** {@inheritDoc} */
    @Override
    public byte readByte() throws IOException {
        return input.readByte();
    }

    /** {@inheritDoc} */
    @Override
    public short readShort() throws IOException {
        return input.readShort();
    }

    /** {@inheritDoc} */
    @Override
    public int readInt() throws IOException {
        return input.readInt();
    }

    /** {@inheritDoc} */
    @Override
    public long readLong() throws IOException {
        return input.readLong();
    }

    /** {@inheritDoc} */
    @Override
    public float readFloat() throws IOException {
        return input.readFloat();
    }

    /** {@inheritDoc} */
    @Override
    public double readDouble() throws IOException {
        return input.readDouble();
    }

    /** {@inheritDoc} */
    @Override
    public char readChar() throws IOException {
        return input.readChar();
    }

    /** {@inheritDoc} */
    @Override
    public boolean readBoolean() throws IOException {
        return input.readBoolean();
    }

    /** {@inheritDoc} */
    @Override
    public String readUTF() throws IOException {
        return input.readUTF();
    }

    /** {@inheritDoc} */
    @Override
    public int readUnsignedByte() throws IOException {
        return input.readUnsignedByte();
    }

    /** {@inheritDoc} */
    @Override
    public int readUnsignedShort() throws IOException {
        return input.readUnsignedShort();
    }

    /** {@inheritDoc} */
    @Override
    public void readFully(byte[] buf) throws IOException {
        input.readFully(buf);
    }

    /** {@inheritDoc} */
    @Override
    public void readFully(byte[] buf, int off, int len) throws IOException {
        input.readFully(buf, off, len);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override
    public String readLine() throws IOException {
        return input.readLine();
    }

    /** {@inheritDoc} */
    @Override
    protected Object readObjectOverride() throws IOException {
        return doReadObjectOfAnyType();
    }

    private Object doReadObjectOfAnyType() throws IOException {
        return doReadObjectOf(null);
    }

    private Object doReadObjectOf(@Nullable DeclaredType declaredType) throws IOException {
        try {
            return valueReader.read(input, declaredType, context);
        } catch (UnmarshalException e) {
            throw new UncheckedUnmarshalException("Cannot read an object", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public Object readUnshared() throws IOException {
        return doReadUnsharedOfAnyType();
    }

    private Object doReadUnsharedOfAnyType() throws IOException {
        return doReadUnsharedOf(null);
    }

    private Object doReadUnsharedOf(@Nullable DeclaredType declaredType) throws IOException {
        try {
            return unsharedReader.read(input, declaredType, context);
        } catch (UnmarshalException e) {
            throw new UncheckedUnmarshalException("Cannot read an unshared object", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void defaultReadObject() throws IOException {
        try {
            defaultFieldsReaderWriter.defaultFillFieldsFrom(
                    input,
                    context.objectCurrentlyReadWithReadObject(),
                    context.descriptorOfObjectCurrentlyReadWithReadObject(),
                    context
            );
        } catch (UnmarshalException e) {
            throw new UncheckedUnmarshalException("Cannot read fields in a default way", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public GetField readFields() throws IOException {
        if (currentGet == null) {
            currentGet = new UosGetField(context.descriptorOfObjectCurrentlyReadWithReadObject());
            currentGet.readFields();
        }
        return currentGet;
    }

    /** {@inheritDoc} */
    @Override
    public int available() throws IOException {
        return input.available();
    }

    /** {@inheritDoc} */
    @Override
    public int skipBytes(int len) throws IOException {
        return input.skipBytes(len);
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
        // no-op
    }

    UosGetField replaceCurrentGetFieldWithNull() {
        UosGetField oldGet = currentGet;
        currentGet = null;
        return oldGet;
    }

    void restoreCurrentGetFieldTo(UosGetField newGet) {
        currentGet = newGet;
    }

    class UosGetField extends GetField {
        private final DataInput input = UosObjectInputStream.this;
        private final ClassDescriptor remoteDescriptor;

        private final byte[] primitiveFieldsData;
        private final Object[] objectFieldVals;

        private UosGetField(ClassDescriptor currentObjectDescriptor) {
            this.remoteDescriptor = currentObjectDescriptor;

            primitiveFieldsData = new byte[currentObjectDescriptor.primitiveFieldsDataSize()];
            objectFieldVals = new Object[currentObjectDescriptor.objectFieldsCount()];
        }

        /** {@inheritDoc} */
        @Override
        public ObjectStreamClass getObjectStreamClass() {
            // TODO: IGNITE-16572 - make it support schema changes

            return ObjectStreamClass.lookupAny(remoteDescriptor.localClass());
        }

        /** {@inheritDoc} */
        @Override
        public boolean defaulted(String name) {
            return remoteDescriptor.isPrimitiveFieldAddedLocally(name) || remoteDescriptor.isObjectFieldAddedLocally(name);
        }

        /** {@inheritDoc} */
        @Override
        public boolean get(String name, boolean defaultValue) throws IOException {
            if (remoteDescriptor.isPrimitiveFieldAddedLocally(name)) {
                return defaultValue;
            }

            return LittleEndianBits.getBoolean(primitiveFieldsData, primitiveFieldDataOffset(name, boolean.class));
        }

        /** {@inheritDoc} */
        @Override
        public byte get(String name, byte defaultValue) throws IOException {
            if (remoteDescriptor.isPrimitiveFieldAddedLocally(name)) {
                return defaultValue;
            }

            return primitiveFieldsData[primitiveFieldDataOffset(name, byte.class)];
        }

        /** {@inheritDoc} */
        @Override
        public char get(String name, char defaultValue) throws IOException {
            if (remoteDescriptor.isPrimitiveFieldAddedLocally(name)) {
                return defaultValue;
            }

            return LittleEndianBits.getChar(primitiveFieldsData, primitiveFieldDataOffset(name, char.class));
        }

        /** {@inheritDoc} */
        @Override
        public short get(String name, short defaultValue) throws IOException {
            if (remoteDescriptor.isPrimitiveFieldAddedLocally(name)) {
                return defaultValue;
            }

            return LittleEndianBits.getShort(primitiveFieldsData, primitiveFieldDataOffset(name, short.class));
        }

        /** {@inheritDoc} */
        @Override
        public int get(String name, int defaultValue) throws IOException {
            if (remoteDescriptor.isPrimitiveFieldAddedLocally(name)) {
                return defaultValue;
            }

            return LittleEndianBits.getInt(primitiveFieldsData, primitiveFieldDataOffset(name, int.class));
        }

        /** {@inheritDoc} */
        @Override
        public long get(String name, long defaultValue) throws IOException {
            if (remoteDescriptor.isPrimitiveFieldAddedLocally(name)) {
                return defaultValue;
            }

            return LittleEndianBits.getLong(primitiveFieldsData, primitiveFieldDataOffset(name, long.class));
        }

        /** {@inheritDoc} */
        @Override
        public float get(String name, float defaultValue) throws IOException {
            if (remoteDescriptor.isPrimitiveFieldAddedLocally(name)) {
                return defaultValue;
            }

            return LittleEndianBits.getFloat(primitiveFieldsData, primitiveFieldDataOffset(name, float.class));
        }

        /** {@inheritDoc} */
        @Override
        public double get(String name, double defaultValue) throws IOException {
            if (remoteDescriptor.isPrimitiveFieldAddedLocally(name)) {
                return defaultValue;
            }

            return LittleEndianBits.getDouble(primitiveFieldsData, primitiveFieldDataOffset(name, double.class));
        }

        /** {@inheritDoc} */
        @Override
        public Object get(String name, Object defaultValue) throws IOException {
            if (remoteDescriptor.isObjectFieldAddedLocally(name)) {
                return defaultValue;
            }

            return objectFieldVals[remoteDescriptor.objectFieldIndex(name)];
        }

        private int primitiveFieldDataOffset(String fieldName, Class<?> requiredType) {
            return remoteDescriptor.primitiveFieldDataOffset(fieldName, requiredType.getName());
        }

        private void readFields() throws IOException {
            @Nullable BitSet nullsBitSet = NullsBitsetReader.readNullsBitSet(input, remoteDescriptor);

            int objectFieldIndex = 0;
            for (FieldDescriptor fieldDesc : remoteDescriptor.fields()) {
                objectFieldIndex = readNext(fieldDesc, objectFieldIndex, nullsBitSet);
            }
        }

        private int readNext(FieldDescriptor fieldDesc, int objectFieldIndex, @Nullable BitSet nullsBitSet) throws IOException {
            if (fieldDesc.isPrimitive()) {
                readPrimitive(fieldDesc);
                return objectFieldIndex;
            } else {
                return readObject(fieldDesc, objectFieldIndex, nullsBitSet);
            }
        }

        private void readPrimitive(FieldDescriptor fieldDesc) throws IOException {
            int offset = remoteDescriptor.primitiveFieldDataOffset(fieldDesc.name(), fieldDesc.typeName());
            int length = fieldDesc.primitiveWidthInBytes();
            input.readFully(primitiveFieldsData, offset, length);
        }

        private int readObject(FieldDescriptor fieldDesc, int objectFieldIndex, @Nullable BitSet nullsBitSet) throws IOException {
            if (!StructuredObjectMarshaller.nullWasSkippedWhileWriting(fieldDesc, remoteDescriptor, nullsBitSet)) {
                Object readObject;
                if (fieldDesc.isUnshared()) {
                    readObject = doReadUnsharedOf(fieldDesc);
                } else {
                    readObject = doReadObjectOf(fieldDesc);
                }

                objectFieldVals[objectFieldIndex] = readObject;
            }

            return objectFieldIndex + 1;
        }
    }
}
