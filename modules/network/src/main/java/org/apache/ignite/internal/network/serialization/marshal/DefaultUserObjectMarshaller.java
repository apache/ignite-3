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
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InvalidObjectException;
import java.util.Collection;
import java.util.Map;
import org.apache.ignite.internal.network.serialization.BuiltInType;
import org.apache.ignite.internal.network.serialization.ClassDescriptor;
import org.apache.ignite.internal.network.serialization.ClassDescriptorFactory;
import org.apache.ignite.internal.network.serialization.ClassDescriptorRegistry;
import org.apache.ignite.internal.network.serialization.DeclaredType;
import org.apache.ignite.internal.network.serialization.DescriptorRegistry;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.apache.ignite.internal.util.io.IgniteUnsafeDataInput;
import org.jetbrains.annotations.Nullable;

/**
 * Default implementation of {@link UserObjectMarshaller}.
 */
public class DefaultUserObjectMarshaller implements UserObjectMarshaller, SchemaMismatchEventSource {
    private static final boolean UNSHARED = true;
    private static final boolean NOT_UNSHARED = false;

    private static final DeclaredType NO_DECLARED_TYPE = null;

    private final SchemaMismatchHandlers schemaMismatchHandlers = new SchemaMismatchHandlers();

    private final LocalDescriptors localDescriptors;

    private final WriteReplacer writeReplacer;
    private final ReadResolver readResolver;

    private final BuiltInNonContainerMarshallers builtInNonContainerMarshallers = new BuiltInNonContainerMarshallers();
    private final BuiltInContainerMarshallers builtInContainerMarshallers = new BuiltInContainerMarshallers(
            this::marshalShared,
            this::unmarshalShared
    );
    private final StructuredObjectMarshaller structuredObjectMarshaller;
    private final ExternalizableMarshaller externalizableMarshaller;
    private final ProxyMarshaller proxyMarshaller;

    private final MarshallingValidations validations = new MarshallingValidations();

    private final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

    private final ThreadLocal<UosIgniteOutputStream> threadLocalDataOutput = ThreadLocal.withInitial(this::newOutput);

    private UosIgniteOutputStream newOutput() {
        return new UosIgniteOutputStream(4096);
    }

    /**
     * Constructor.
     *
     * @param localRegistry registry of local descriptors to consult with (and to write to if an unseen class is encountered)
     * @param descriptorFactory  descriptor factory to create new descriptors from classes
     */
    public DefaultUserObjectMarshaller(ClassDescriptorRegistry localRegistry, ClassDescriptorFactory descriptorFactory) {
        localDescriptors = new LocalDescriptors(localRegistry, descriptorFactory);

        writeReplacer = new WriteReplacer(localDescriptors);
        readResolver = new ReadResolver(schemaMismatchHandlers);

        structuredObjectMarshaller = new StructuredObjectMarshaller(
                localRegistry,
                this::marshalShared,
                this::marshalUnshared,
                this::unmarshalShared,
                this::unmarshalUnshared,
                schemaMismatchHandlers
        );

        externalizableMarshaller = new ExternalizableMarshaller(
                this::marshalShared,
                this::marshalUnshared,
                this::unmarshalShared,
                this::unmarshalUnshared,
                structuredObjectMarshaller,
                schemaMismatchHandlers
        );

        proxyMarshaller = new ProxyMarshaller(this::marshalShared, this::unmarshalShared);
    }

    /** {@inheritDoc} */
    @Override
    public MarshalledObject marshal(@Nullable Object object) throws MarshalException {
        MarshallingContext context = new MarshallingContext();

        UosIgniteOutputStream output = freshByteArrayOutputStream();
        try {
            marshalShared(object, output, context);
        } catch (IOException e) {
            throw new MarshalException("Cannot marshal", e);
        } finally {
            output.release();
        }

        return new MarshalledObject(output.array(), context.usedDescriptorIds());
    }

    private UosIgniteOutputStream freshByteArrayOutputStream() {
        UosIgniteOutputStream output = threadLocalDataOutput.get();

        if (output.isOccupied()) {
            // This is a nested invocation, probably from a callback method like writeObject(), we can't reuse
            // the 'outer' output, let's make a new one as this should not happen often.
            output = newOutput();
        } else {
            output.cleanup();
        }
        output.occupy();

        return output;
    }

    private void marshalShared(@Nullable Object object, IgniteDataOutput output, MarshallingContext context)
            throws MarshalException, IOException {
        marshalShared(object, NO_DECLARED_TYPE, output, context);
    }

    private void marshalShared(
            @Nullable Object object,
            @Nullable DeclaredType declaredType,
            IgniteDataOutput output,
            MarshallingContext context
    ) throws MarshalException, IOException {
        marshalToOutput(object, declaredType, output, context, NOT_UNSHARED);
    }

    private void marshalUnshared(@Nullable Object object, DeclaredType declaredType, IgniteDataOutput output, MarshallingContext context)
            throws MarshalException, IOException {
        marshalToOutput(object, declaredType, output, context, UNSHARED);
    }

    private void marshalToOutput(
            @Nullable Object object,
            @Nullable DeclaredType declaredType,
            IgniteDataOutput output,
            MarshallingContext context,
            boolean unshared
    ) throws MarshalException, IOException {
        validations.throwIfMarshallingNotSupported(object);

        ClassDescriptor originalDescriptor = localDescriptors.getOrCreateDescriptor(object);

        DescribedObject afterReplacement = writeReplacer.applyWriteReplaceIfNeeded(object, originalDescriptor);

        if (hasObjectIdentity(afterReplacement.object, afterReplacement.descriptor)) {
            long flaggedObjectId = context.memorizeObject(afterReplacement.object, unshared);
            int objectId = FlaggedObjectIds.objectId(flaggedObjectId);

            if (FlaggedObjectIds.isAlreadySeen(flaggedObjectId)) {
                writeReference(objectId, declaredType, output);
            } else {
                marshalIdentifiable(afterReplacement.object, afterReplacement.descriptor, declaredType, objectId, output, context);
            }
        } else {
            marshalValue(afterReplacement.object, afterReplacement.descriptor, declaredType, output, context);
        }
    }

    private boolean hasObjectIdentity(@Nullable Object object, ClassDescriptor descriptor) {
        return object != null && mayHaveObjectIdentity(descriptor);
    }

    private boolean mayHaveObjectIdentity(ClassDescriptor descriptor) {
        return !descriptor.isPrimitive() && !descriptor.isNull();
    }

    private void writeReference(int objectId, @Nullable DeclaredType declaredClass, DataOutput output) throws IOException {
        if (!serializationTypeIsKnownUpfront(declaredClass)) {
            ProtocolMarshalling.writeDescriptorOrCommandId(BuiltInType.REFERENCE.descriptorId(), output);
        }
        ProtocolMarshalling.writeObjectId(objectId, output);
    }

    private void marshalIdentifiable(
            @Nullable Object object,
            ClassDescriptor descriptor,
            @Nullable DeclaredType declaredType,
            int objectId,
            IgniteDataOutput output,
            MarshallingContext context
    ) throws IOException, MarshalException {
        if (!serializationTypeIsKnownUpfront(declaredType)) {
            writeDescriptorId(descriptor, output);
        }
        ProtocolMarshalling.writeObjectId(objectId, output);

        writeObject(object, descriptor, output, context);
    }

    private boolean serializationTypeIsKnownUpfront(@Nullable DeclaredType declaredType) {
        return declaredType != null && declaredType.isSerializationTypeKnownUpfront();
    }

    private void writeDescriptorId(ClassDescriptor descriptor, DataOutput output) throws IOException {
        ProtocolMarshalling.writeDescriptorOrCommandId(descriptor.descriptorId(), output);
    }

    private void marshalValue(
            @Nullable Object object,
            ClassDescriptor descriptor,
            @Nullable DeclaredType declaredType,
            IgniteDataOutput output,
            MarshallingContext context
    ) throws IOException, MarshalException {
        if (!serializationTypeIsKnownUpfront(declaredType)) {
            writeDescriptorId(descriptor, output);
        }

        writeObject(object, descriptor, output, context);
    }

    private void writeObject(@Nullable Object object, ClassDescriptor descriptor, IgniteDataOutput output, MarshallingContext context)
            throws IOException, MarshalException {
        if (isBuiltInNonContainer(descriptor)) {
            builtInNonContainerMarshallers.writeBuiltIn(object, descriptor, output, context);
        } else if (isBuiltInCollection(descriptor)) {
            builtInContainerMarshallers.writeBuiltInCollection((Collection<?>) object, descriptor, output, context);
        } else if (isBuiltInMap(descriptor)) {
            builtInContainerMarshallers.writeBuiltInMap((Map<?, ?>) object, descriptor, output, context);
        } else if (descriptor.isArray()) {
            //noinspection ConstantConditions
            builtInContainerMarshallers.writeGenericRefArray((Object[]) object, descriptor, output, context);
        } else if (descriptor.isExternalizable()) {
            externalizableMarshaller.writeExternalizable((Externalizable) object, descriptor, output, context);
        } else if (descriptor.isProxy()) {
            //noinspection ConstantConditions
            proxyMarshaller.writeProxy(object, output, context);
        } else {
            structuredObjectMarshaller.writeStructuredObject(object, descriptor, output, context);
        }
    }

    private boolean isBuiltInNonContainer(ClassDescriptor descriptor) {
        return builtInNonContainerMarshallers.supports(descriptor);
    }

    private boolean isBuiltInCollection(ClassDescriptor descriptor) {
        return builtInContainerMarshallers.supportsCollection(descriptor);
    }

    private boolean isBuiltInMap(ClassDescriptor descriptor) {
        return builtInContainerMarshallers.supportsAsBuiltInMap(descriptor);
    }

    /** {@inheritDoc} */
    @Override
    @Nullable
    public <T> T unmarshal(byte[] bytes, Object mergedDescriptors) throws UnmarshalException {
        var input = new IgniteUnsafeDataInput(bytes);

        try {
            UnmarshallingContext context = new UnmarshallingContext(input, (DescriptorRegistry) mergedDescriptors, classLoader);
            T result = unmarshalShared(input, context);

            throwIfNotDrained(input);

            return result;
        } catch (IOException e) {
            throw new UnmarshalException("Cannot unmarshal", e);
        }
    }

    private <T> T unmarshalShared(IgniteDataInput input, UnmarshallingContext context) throws IOException, UnmarshalException {
        return unmarshalShared(input, NO_DECLARED_TYPE, context);
    }

    private <T> T unmarshalShared(IgniteDataInput input, @Nullable DeclaredType declaredType, UnmarshallingContext context)
            throws IOException, UnmarshalException {
        return unmarshalFromInput(input, declaredType, context, NOT_UNSHARED);
    }

    private <T> T unmarshalUnshared(IgniteDataInput input, @Nullable DeclaredType declaredType, UnmarshallingContext context)
            throws IOException, UnmarshalException {
        return unmarshalFromInput(input, declaredType, context, UNSHARED);
    }

    private <T> T unmarshalFromInput(
            IgniteDataInput input,
            @Nullable DeclaredType declaredType,
            UnmarshallingContext context,
            boolean unshared
    ) throws IOException, UnmarshalException {
        ClassDescriptor remoteDescriptor = resolveDescriptor(input, declaredType, context);

        if (mayHaveObjectIdentity(remoteDescriptor)) {
            int objectId = peekObjectId(input, context);
            if (context.isKnownObjectId(objectId)) {
                // this is a back-reference
                return unmarshalReference(input, context, unshared);
            }
        }

        Object readObject = readObject(input, context, remoteDescriptor, unshared);

        return (T) readResolver.applyReadResolveIfNeeded(readObject, remoteDescriptor);
    }

    private ClassDescriptor resolveDescriptor(IgniteDataInput input, @Nullable DeclaredType declaredType, UnmarshallingContext context)
            throws IOException {
        if (serializationTypeIsKnownUpfront(declaredType)) {
            return context.getRequiredDescriptor(declaredType.typeDescriptorId());
        } else {
            int commandOrDescriptorId = ProtocolMarshalling.readDescriptorOrCommandId(input);
            return context.getRequiredDescriptor(commandOrDescriptorId);
        }
    }

    private int peekObjectId(DataInput input, UnmarshallingContext context) throws IOException {
        context.markSource(ProtocolMarshalling.MAX_LENGTH_BYTE_COUNT);
        int objectId = ProtocolMarshalling.readObjectId(input);
        context.resetSourceToMark();
        return objectId;
    }

    private <T> T unmarshalReference(DataInput input, UnmarshallingContext context, boolean unshared) throws IOException {
        if (unshared) {
            throw new InvalidObjectException("cannot read back reference as unshared");
        }

        int objectId = ProtocolMarshalling.readObjectId(input);

        if (context.isUnsharedObjectId(objectId)) {
            throw new InvalidObjectException("cannot read back reference to unshared object");
        }

        return context.dereference(objectId);
    }

    @Nullable
    private Object readObject(IgniteDataInput input, UnmarshallingContext context, ClassDescriptor remoteDescriptor, boolean unshared)
            throws IOException, UnmarshalException {
        if (!mayHaveObjectIdentity(remoteDescriptor)) {
            return readValue(input, remoteDescriptor, context);
        } else if (mustBeReadInOneStage(remoteDescriptor)) {
            return readIdentifiableInOneStage(input, remoteDescriptor, context, unshared);
        } else {
            return readIdentifiableInTwoStages(input, remoteDescriptor, context, unshared);
        }
    }

    private boolean mustBeReadInOneStage(ClassDescriptor descriptor) {
        return builtInNonContainerMarshallers.supports(descriptor);
    }

    @Nullable
    private Object readIdentifiableInOneStage(
            IgniteDataInput input,
            ClassDescriptor descriptor,
            UnmarshallingContext context,
            boolean unshared
    ) throws IOException, UnmarshalException {
        int objectId = readObjectId(input);

        Object object = readValue(input, descriptor, context);
        context.registerReference(objectId, object, unshared);

        return object;
    }

    private int readObjectId(DataInput input) throws IOException {
        return ProtocolMarshalling.readObjectId(input);
    }

    private Object readIdentifiableInTwoStages(
            IgniteDataInput input,
            ClassDescriptor remoteDescriptor,
            UnmarshallingContext context,
            boolean unshared
    ) throws IOException, UnmarshalException {
        int objectId = readObjectId(input);

        Object object = preInstantiate(remoteDescriptor, input, context);
        context.registerReference(objectId, object, unshared);

        fillObjectFrom(input, object, remoteDescriptor, context);

        return object;
    }

    private Object preInstantiate(ClassDescriptor remoteDescriptor, IgniteDataInput input, UnmarshallingContext context)
            throws IOException, UnmarshalException {
        if (isBuiltInNonContainer(remoteDescriptor)) {
            throw new IllegalStateException("Should not be here, descriptor is " + remoteDescriptor);
        } else if (isBuiltInCollection(remoteDescriptor)) {
            return builtInContainerMarshallers.preInstantiateBuiltInMutableCollection(remoteDescriptor, input, context);
        } else if (isBuiltInMap(remoteDescriptor)) {
            return builtInContainerMarshallers.preInstantiateBuiltInMutableMap(remoteDescriptor, input, context);
        } else if (remoteDescriptor.isArray()) {
            return builtInContainerMarshallers.preInstantiateGenericRefArray(input, context);
        } else if (remoteDescriptor.isExternalizable()) {
            return externalizableMarshaller.preInstantiateExternalizable(remoteDescriptor);
        } else if (remoteDescriptor.isProxy()) {
            return proxyMarshaller.preInstantiateProxy(input, context);
        } else {
            return structuredObjectMarshaller.preInstantiateStructuredObject(remoteDescriptor);
        }
    }

    private void fillObjectFrom(IgniteDataInput input, Object objectToFill, ClassDescriptor remoteDescriptor, UnmarshallingContext context)
            throws UnmarshalException, IOException {
        if (isBuiltInNonContainer(remoteDescriptor)) {
            throw new IllegalStateException("Cannot fill " + remoteDescriptor.className() + ", this is a programmatic error");
        } else if (isBuiltInCollection(remoteDescriptor)) {
            fillBuiltInCollectionFrom(input, (Collection<?>) objectToFill, remoteDescriptor, context);
        } else if (isBuiltInMap(remoteDescriptor)) {
            fillBuiltInMapFrom(input, (Map<?, ?>) objectToFill, context);
        } else if (remoteDescriptor.isArray()) {
            fillGenericRefArrayFrom(input, (Object[]) objectToFill, remoteDescriptor, context);
        } else if (remoteDescriptor.isExternalizable()) {
            externalizableMarshaller.fillFromRemotelyExternalizable(input, objectToFill, context);
        } else if (remoteDescriptor.isProxy()) {
            proxyMarshaller.fillProxyFrom(input, objectToFill, context);
        } else {
            structuredObjectMarshaller.fillStructuredObjectFrom(input, objectToFill, remoteDescriptor, context);
            fireExternalizableMissedIfExternalizableLocally(objectToFill);
        }
    }

    private void fillBuiltInCollectionFrom(
            IgniteDataInput input,
            Collection<?> collectionToFill,
            ClassDescriptor descriptor,
            UnmarshallingContext context
    ) throws UnmarshalException, IOException {
        builtInContainerMarshallers.fillBuiltInCollectionFrom(input, collectionToFill, descriptor, this::unmarshalShared, context);
    }

    private void fillBuiltInMapFrom(IgniteDataInput input, Map<?, ?> mapToFill, UnmarshallingContext context)
            throws UnmarshalException, IOException {
        builtInContainerMarshallers.fillBuiltInMapFrom(input, mapToFill, this::unmarshalShared, this::unmarshalShared, context);
    }

    private void fillGenericRefArrayFrom(
            IgniteDataInput input,
            Object[] array,
            ClassDescriptor arrayDescriptor,
            UnmarshallingContext context
    ) throws IOException, UnmarshalException {
        builtInContainerMarshallers.fillGenericRefArrayFrom(input, array, arrayDescriptor, context);
    }

    @Nullable
    private Object readValue(IgniteDataInput input, ClassDescriptor descriptor, UnmarshallingContext context)
            throws IOException, UnmarshalException {
        if (isBuiltInNonContainer(descriptor)) {
            return builtInNonContainerMarshallers.readBuiltIn(descriptor, input, context);
        } else {
            throw new IllegalStateException("Cannot read an instance of " + descriptor.className() + ", this is a programmatic error");
        }
    }

    private void throwIfNotDrained(InputStream dis) throws IOException, UnmarshalException {
        if (dis.available() > 0) {
            throw new UnmarshalException("After reading a value, " + dis.available() + " excessive byte(s) still remain");
        }
    }

    private void fireExternalizableMissedIfExternalizableLocally(Object objectToFill) throws SchemaMismatchException {
        if (objectToFill instanceof Externalizable) {
            schemaMismatchHandlers.onExternalizableMissed(objectToFill);
        }
    }

    /** {@inheritDoc} */
    @Override
    public <T> void replaceSchemaMismatchHandler(Class<T> layerClass, SchemaMismatchHandler<T> handler) {
        schemaMismatchHandlers.registerHandler(layerClass, handler);
    }

    @Override
    public <T> void replaceSchemaMismatchHandler(String layerClassName, SchemaMismatchHandler<T> handler) {
        schemaMismatchHandlers.registerHandler(layerClassName, handler);
    }
}
