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
import java.util.BitSet;
import java.util.List;
import org.apache.ignite.internal.network.serialization.BuiltInTypeIds;
import org.apache.ignite.internal.network.serialization.ClassDescriptor;
import org.apache.ignite.internal.network.serialization.DescriptorRegistry;
import org.apache.ignite.internal.network.serialization.FieldAccessor;
import org.apache.ignite.internal.network.serialization.FieldDescriptor;
import org.apache.ignite.internal.network.serialization.MergedField;
import org.apache.ignite.internal.network.serialization.MergedLayer;
import org.apache.ignite.internal.network.serialization.SpecialMethodInvocationException;
import org.apache.ignite.internal.network.serialization.marshal.UosObjectInputStream.UosGetField;
import org.apache.ignite.internal.network.serialization.marshal.UosObjectOutputStream.UosPutField;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.apache.ignite.internal.util.io.IgniteUnsafeDataInput;
import org.jetbrains.annotations.Nullable;

/**
 * (Un)marshals objects that have structure (fields). These are {@link java.io.Serializable}s
 * (which are not {@link java.io.Externalizable}s) and arbitrary (non-serializable, non-externalizable) objects.
 */
class StructuredObjectMarshaller implements DefaultFieldsReaderWriter {
    private final DescriptorRegistry localDescriptors;

    private final TypedValueWriter valueWriter;
    private final TypedValueWriter unsharedWriter;
    private final TypedValueReader valueReader;
    private final TypedValueReader unsharedReader;

    private final SchemaMismatchHandlers schemaMismatchHandlers;

    private final Instantiation instantiation = new BestEffortInstantiation(
            new SerializableInstantiation(),
            new UnsafeInstantiation()
    );

    private final NullsBitsetWriter nullsBitsetWriter = new DefaultNullsBitsetWriter();

    StructuredObjectMarshaller(
            DescriptorRegistry localDescriptors,
            TypedValueWriter valueWriter,
            TypedValueWriter unsharedWriter,
            TypedValueReader valueReader,
            TypedValueReader unsharedReader,
            SchemaMismatchHandlers schemaMismatchHandlers
    ) {
        this.localDescriptors = localDescriptors;
        this.valueWriter = valueWriter;
        this.unsharedWriter = unsharedWriter;
        this.valueReader = valueReader;
        this.unsharedReader = unsharedReader;
        this.schemaMismatchHandlers = schemaMismatchHandlers;
    }

    void writeStructuredObject(Object object, ClassDescriptor descriptor, IgniteDataOutput output, MarshallingContext context)
            throws MarshalException, IOException {
        List<ClassDescriptor> lineage = descriptor.lineage();

        for (ClassDescriptor layer : lineage) {
            writeStructuredObjectLayer(object, layer, output, context);
        }
    }

    private void writeStructuredObjectLayer(Object object, ClassDescriptor layer, IgniteDataOutput output, MarshallingContext context)
            throws IOException, MarshalException {
        if (layer.hasWriteObject()) {
            writeWithWriteObject(object, layer, output, context);
        } else {
            defaultWriteFields(object, layer, output, context);
        }

        context.addUsedDescriptor(layer);
    }

    private void writeWithWriteObject(Object object, ClassDescriptor descriptor, IgniteDataOutput output, MarshallingContext context)
            throws IOException, MarshalException {
        // Do not close the stream yet!
        UosObjectOutputStream oos = context.objectOutputStream(output, valueWriter, unsharedWriter, this);

        UosPutField oldPut = oos.replaceCurrentPutFieldWithNull();
        context.startWritingWithWriteObject(object, descriptor);

        try {
            writeObjectWithLength(object, descriptor, oos);
            oos.flush();
        } catch (SpecialMethodInvocationException e) {
            throw new MarshalException("Cannot invoke writeObject()", e);
        } finally {
            context.endWritingWithWriteObject();
            oos.restoreCurrentPutFieldTo(oldPut);
        }
    }

    private void writeObjectWithLength(Object object, ClassDescriptor descriptor, UosObjectOutputStream oos)
            throws IOException, SpecialMethodInvocationException {
        // NB: this only works with purely in-memory IgniteDataInput implementations!

        int offsetBefore = oos.memoryBufferOffset();

        writeLengthPlaceholder(oos);

        descriptor.serializationMethods().writeObject(object, oos);
        oos.flush();

        int externalDataLength = oos.memoryBufferOffset() - offsetBefore - Integer.BYTES;

        oos.writeIntAtOffset(offsetBefore, externalDataLength);
    }

    private void writeLengthPlaceholder(UosObjectOutputStream oos) throws IOException {
        oos.writeInt(0);
    }

    /** {@inheritDoc} */
    @Override
    public void defaultWriteFields(Object object, ClassDescriptor descriptor, IgniteDataOutput output, MarshallingContext context)
            throws MarshalException, IOException {
        @Nullable BitSet nullsBitSet = nullsBitsetWriter.writeNullsBitSet(object, descriptor, output);

        for (FieldDescriptor fieldDescriptor : descriptor.fields()) {
            if (cannotAvoidWritingNull(fieldDescriptor, descriptor, nullsBitSet)) {
                writeField(object, fieldDescriptor, output, context);
            }
        }
    }

    static boolean cannotAvoidWritingNull(FieldDescriptor fieldDescriptor, ClassDescriptor descriptor, @Nullable BitSet nullsBitSet) {
        int maybeIndexInBitmap = descriptor.fieldIndexInNullsBitmap(fieldDescriptor.name());

        assert maybeIndexInBitmap < 0 || nullsBitSet != null : "Index is " + maybeIndexInBitmap;

        return maybeIndexInBitmap < 0 || !nullsBitSet.get(maybeIndexInBitmap);
    }

    private void writeField(Object object, FieldDescriptor fieldDescriptor, IgniteDataOutput output, MarshallingContext context)
            throws MarshalException, IOException {
        if (fieldDescriptor.isPrimitive()) {
            writePrimitiveFieldValue(object, fieldDescriptor, output);

            context.addUsedDescriptor(localDescriptors.getRequiredDescriptor(fieldDescriptor.typeDescriptorId()));
        } else {
            Object fieldValue = getFieldValue(object, fieldDescriptor);
            valueWriter.write(fieldValue, fieldDescriptor, output, context);
        }
    }

    private Object getFieldValue(Object object, FieldDescriptor fieldDescriptor) {
        return fieldDescriptor.accessor().getObject(object);
    }

    private void writePrimitiveFieldValue(Object object, FieldDescriptor fieldDescriptor, IgniteDataOutput output) throws IOException {
        FieldAccessor fieldAccessor = fieldDescriptor.accessor();

        switch (fieldDescriptor.typeDescriptorId()) {
            case BuiltInTypeIds.BYTE:
                output.writeByte(fieldAccessor.getByte(object));
                break;
            case BuiltInTypeIds.SHORT:
                output.writeShort(fieldAccessor.getShort(object));
                break;
            case BuiltInTypeIds.INT:
                output.writeInt(fieldAccessor.getInt(object));
                break;
            case BuiltInTypeIds.LONG:
                output.writeLong(fieldAccessor.getLong(object));
                break;
            case BuiltInTypeIds.FLOAT:
                output.writeFloat(fieldAccessor.getFloat(object));
                break;
            case BuiltInTypeIds.DOUBLE:
                output.writeDouble(fieldAccessor.getDouble(object));
                break;
            case BuiltInTypeIds.CHAR:
                output.writeChar(fieldAccessor.getChar(object));
                break;
            case BuiltInTypeIds.BOOLEAN:
                output.writeBoolean(fieldAccessor.getBoolean(object));
                break;
            default:
                throw new IllegalStateException(fieldDescriptor.typeName() + " is primitive but not covered");
        }
    }

    Object preInstantiateStructuredObject(ClassDescriptor descriptor) throws UnmarshalException {
        try {
            return instantiation.newInstance(descriptor.localClass());
        } catch (InstantiationException e) {
            throw new UnmarshalException("Cannot instantiate " + descriptor.className(), e);
        }
    }

    void fillStructuredObjectFrom(IgniteDataInput input, Object object, ClassDescriptor remoteDescriptor, UnmarshallingContext context)
            throws IOException, UnmarshalException {
        List<MergedLayer> lineage = remoteDescriptor.mergedLineage();

        for (MergedLayer mergedLayer : lineage) {
            if (mergedLayer.hasRemote()) {
                fillStructuredObjectLayerFrom(input, mergedLayer.remote(), object, context);
            } else if (mergedLayer.hasLocal()) {
                fireEventsForLocalOnlyLayer(object, mergedLayer.local());
            }
        }
    }

    private void fireEventsForLocalOnlyLayer(Object object, ClassDescriptor localLayer) throws SchemaMismatchException {
        fireOnFieldMissedOnLayerFields(object, localLayer);
        schemaMismatchHandlers.onReadObjectMissed(localLayer.className(), object);
    }

    private void fireOnFieldMissedOnLayerFields(Object object, ClassDescriptor layer) throws SchemaMismatchException {
        for (FieldDescriptor localField : layer.fields()) {
            schemaMismatchHandlers.onFieldMissed(layer.className(), object, localField.name());
        }
    }

    private void fillStructuredObjectLayerFrom(
            IgniteDataInput input,
            ClassDescriptor remoteLayer,
            Object object,
            UnmarshallingContext context
    ) throws IOException, UnmarshalException {
        boolean hasReadObjectLocally = remoteLayer.hasLocal() && remoteLayer.local().hasReadObject();

        if (remoteLayer.hasWriteObject()) {
            if (hasReadObjectLocally) {
                fillObjectWithReadObjectFrom(input, object, remoteLayer, context);
            } else {
                fireReadObjectIgnored(remoteLayer, object, input, context);
            }
        } else {
            defaultFillFieldsFrom(input, object, remoteLayer, context);

            if (hasReadObjectLocally) {
                schemaMismatchHandlers.onReadObjectMissed(remoteLayer.className(), object);
            }
        }
    }

    private void fillObjectWithReadObjectFrom(
            IgniteDataInput input,
            Object object,
            ClassDescriptor descriptor,
            UnmarshallingContext context
    ) throws IOException, UnmarshalException {
        // Do not close the stream yet!
        UosObjectInputStream ois = context.objectInputStream(input, valueReader, unsharedReader, this);

        UosGetField oldGet = ois.replaceCurrentGetFieldWithNull();
        context.startReadingWithReadObject(object, descriptor);

        try {
            readObjectWithLength(object, descriptor, ois);
        } catch (SpecialMethodInvocationException e) {
            throw new UnmarshalException("Cannot invoke readObject()", e);
        } finally {
            context.endReadingWithReadObject();
            ois.restoreCurrentGetFieldTo(oldGet);
        }
    }

    private void readObjectWithLength(Object object, ClassDescriptor descriptor, UosObjectInputStream ois)
            throws IOException, SpecialMethodInvocationException {
        skipWriteObjectDataLength(ois);

        descriptor.serializationMethods().readObject(object, ois);
    }

    private void skipWriteObjectDataLength(UosObjectInputStream ois) throws IOException {
        ois.readInt();
    }

    private void fireReadObjectIgnored(ClassDescriptor remoteLayer, Object object, IgniteDataInput input, UnmarshallingContext context)
            throws SchemaMismatchException, IOException {
        // We have additional allocations and copying here. It simplifies the code a lot, and it seems that we should
        // not optimize for this rare corner case.

        int writeObjectDataLength = input.readInt();
        byte[] writeObjectDataBytes = new byte[writeObjectDataLength];
        int actuallyRead = input.readFewBytes(writeObjectDataBytes, 0, writeObjectDataLength);
        if (actuallyRead != writeObjectDataLength) {
            throw new IOException("Premature end of the input stream: expected to read " + writeObjectDataLength
                    + ", but only got " + actuallyRead);
        }
        IgniteDataInput externalDataInput = new IgniteUnsafeDataInput(writeObjectDataBytes);

        try (var oos = new UosObjectInputStream(externalDataInput, valueReader, unsharedReader, this, context)) {
            schemaMismatchHandlers.onReadObjectIgnored(remoteLayer.className(), object, oos);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void defaultFillFieldsFrom(IgniteDataInput input, Object object, ClassDescriptor remoteLayer, UnmarshallingContext context)
            throws IOException, UnmarshalException {
        @Nullable BitSet nullsBitSet = NullsBitsetReader.readNullsBitSet(input, remoteLayer);

        for (MergedField mergedField : remoteLayer.mergedFields()) {
            if (mergedField.hasRemote()) {
                fillFieldWithNullSkippedCheckFrom(input, object, mergedField, remoteLayer, nullsBitSet, context);
            } else {
                schemaMismatchHandlers.onFieldMissed(remoteLayer.className(), object, mergedField.name());
            }
        }
    }

    private void fillFieldWithNullSkippedCheckFrom(
            IgniteDataInput input,
            Object object,
            MergedField mergedField,
            ClassDescriptor layerDescriptor,
            @Nullable BitSet nullsBitSet,
            UnmarshallingContext context
    ) throws IOException, UnmarshalException {
        if (nullWasSkippedWhileWriting(mergedField.remote(), layerDescriptor, nullsBitSet)) {
            setFieldValue(object, mergedField, null, layerDescriptor);
        } else {
            fillFieldFrom(input, object, context, mergedField, layerDescriptor);
        }
    }

    static boolean nullWasSkippedWhileWriting(FieldDescriptor fieldDescriptor, ClassDescriptor descriptor, @Nullable BitSet nullsBitSet) {
        int maybeIndexInBitmap = descriptor.fieldIndexInNullsBitmap(fieldDescriptor.name());

        assert maybeIndexInBitmap < 0 || nullsBitSet != null : "Index is " + maybeIndexInBitmap;

        return maybeIndexInBitmap >= 0 && nullsBitSet.get(maybeIndexInBitmap);
    }

    private void fillFieldFrom(
            IgniteDataInput input,
            Object object,
            UnmarshallingContext context,
            MergedField mergedField,
            ClassDescriptor layerDescriptor
    ) throws IOException, UnmarshalException {
        if (mergedField.remote().isPrimitive()) {
            fillPrimitiveFieldFrom(input, object, mergedField, layerDescriptor);
        } else {
            Object fieldValue = valueReader.read(input, mergedField.remote(), context);
            setFieldValue(object, mergedField, fieldValue, layerDescriptor);
        }
    }

    private void setFieldValue(Object object, MergedField mergedField, Object fieldValue, ClassDescriptor layerDescriptor)
            throws SchemaMismatchException {
        if (!mergedField.hasLocal()) {
            fireFieldIgnored(layerDescriptor, object, mergedField, fieldValue);
            return;
        }
        if (mergedField.typesAreDifferent() && !mergedField.typesAreCompatible()) {
            fireFieldTypeChanged(layerDescriptor, object, mergedField, fieldValue);
            return;
        }

        if (mergedField.typesAreDifferent()) {
            // TODO: IGNITE-16564 - special handling for numeric values
            fieldValue = mergedField.convertToLocalType(fieldValue);
        }

        mergedField.local().accessor().setObject(object, fieldValue);
    }

    private void fireFieldIgnored(ClassDescriptor layerDescriptor, Object object, MergedField mergedField, Object fieldValue)
            throws SchemaMismatchException {
        schemaMismatchHandlers.onFieldIgnored(layerDescriptor.className(), object, mergedField.name(), fieldValue);
    }

    private void fireFieldTypeChanged(ClassDescriptor layerDescriptor, Object object, MergedField mergedField, Object fieldValue)
            throws SchemaMismatchException {
        schemaMismatchHandlers.onFieldTypeChanged(
                layerDescriptor.className(),
                object,
                mergedField.name(),
                mergedField.remote().localClass(),
                fieldValue
        );
    }

    private void fillPrimitiveFieldFrom(
            DataInput input,
            Object object,
            MergedField mergedField,
            ClassDescriptor layerDescriptor
    ) throws IOException, SchemaMismatchException {
        if (!mergedField.hasLocal()) {
            Object value = readPrimitiveValue(input, mergedField.remote());
            fireFieldIgnored(layerDescriptor, object, mergedField, value);
            return;
        }
        if (mergedField.typesAreDifferent()) {
            // TODO: IGNITE-16564 - special handling for numeric values
            Object value = readPrimitiveValue(input, mergedField.remote());
            fireFieldTypeChanged(layerDescriptor, object, mergedField, value);
            return;
        }

        fillPrimitiveFieldWithAccessorFrom(input, object, mergedField, mergedField.local().accessor());
    }

    private Object readPrimitiveValue(DataInput input, FieldDescriptor fieldDescriptor) throws IOException {
        switch (fieldDescriptor.typeDescriptorId()) {
            case BuiltInTypeIds.BYTE:
                return input.readByte();
            case BuiltInTypeIds.SHORT:
                return input.readShort();
            case BuiltInTypeIds.INT:
                return input.readInt();
            case BuiltInTypeIds.LONG:
                return input.readLong();
            case BuiltInTypeIds.FLOAT:
                return input.readFloat();
            case BuiltInTypeIds.DOUBLE:
                return input.readDouble();
            case BuiltInTypeIds.CHAR:
                return input.readChar();
            case BuiltInTypeIds.BOOLEAN:
                return input.readBoolean();
            default:
                throw new IllegalStateException(fieldDescriptor.typeName() + " is primitive but not covered");
        }
    }

    private void fillPrimitiveFieldWithAccessorFrom(DataInput input, Object object, MergedField mergedField, FieldAccessor localAccessor)
            throws IOException {
        switch (mergedField.remote().typeDescriptorId()) {
            case BuiltInTypeIds.BYTE:
                localAccessor.setByte(object, input.readByte());
                break;
            case BuiltInTypeIds.SHORT:
                localAccessor.setShort(object, input.readShort());
                break;
            case BuiltInTypeIds.INT:
                localAccessor.setInt(object, input.readInt());
                break;
            case BuiltInTypeIds.LONG:
                localAccessor.setLong(object, input.readLong());
                break;
            case BuiltInTypeIds.FLOAT:
                localAccessor.setFloat(object, input.readFloat());
                break;
            case BuiltInTypeIds.DOUBLE:
                localAccessor.setDouble(object, input.readDouble());
                break;
            case BuiltInTypeIds.CHAR:
                localAccessor.setChar(object, input.readChar());
                break;
            case BuiltInTypeIds.BOOLEAN:
                localAccessor.setBoolean(object, input.readBoolean());
                break;
            default:
                throw new IllegalStateException(mergedField.remote().typeName() + " is primitive but not covered");
        }
    }
}
