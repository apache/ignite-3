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

package org.apache.ignite.internal.network.serialization;

import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.network.serialization.ClassDescriptorRegistry.shouldBeBuiltIn;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.network.NetworkMessagesFactory;
import org.apache.ignite.internal.network.message.ClassDescriptorMessage;
import org.apache.ignite.internal.network.message.FieldDescriptorMessage;
import org.apache.ignite.internal.network.serialization.marshal.MarshalledObject;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.serialization.MessageDeserializer;
import org.apache.ignite.network.serialization.MessageSerializer;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Per-session serialization service.
 * Handles (de-)serialization of messages, object (de-)serialization and class descriptor merging.
 */
public class PerSessionSerializationService {
    /** Network messages factory. */
    private static final NetworkMessagesFactory MSG_FACTORY = new NetworkMessagesFactory();

    /** Integer value that is sent when there is no descriptor. */
    private static final int NO_DESCRIPTOR_ID = Integer.MIN_VALUE;

    /** Global serialization service. */
    private final SerializationService serializationService;

    /**
     * Map with merged class descriptors. They are the result of the merging of a local and a remote descriptor.
     * The key in this map is a <b>remote</b> descriptor id.
     */
    private final Int2ObjectMap<ClassDescriptor> mergedIdToDescriptorMap = new Int2ObjectOpenHashMap<>();
    /**
     * Map with merged class descriptors. They are the result of the merging of a local and a remote descriptor.
     * The key in this map is the class.
     */
    private final Map<Class<?>, ClassDescriptor> mergedClassToDescriptorMap = new HashMap<>();

    /**
     * A collection of the descriptors that were sent to the remote node.
     */
    private final IntSet sentDescriptors = new IntOpenHashSet();

    /**
     * Descriptors provider.
     */
    private final CompositeDescriptorRegistry descriptors;

    private final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

    /**
     * Constructor.
     *
     * @param serializationService Serialization service.
     */
    public PerSessionSerializationService(SerializationService serializationService) {
        this.serializationService = serializationService;
        this.descriptors = new CompositeDescriptorRegistry(
                new MapBackedIdIndexedDescriptors(mergedIdToDescriptorMap),
                new MapBackedClassIndexedDescriptors(mergedClassToDescriptorMap),
                serializationService.getLocalDescriptorRegistry()
        );
    }

    /**
     * Creates a message serializer.
     *
     * @see SerializationService#createSerializer(short, short)
     */
    public <T extends NetworkMessage> MessageSerializer<T> createMessageSerializer(short groupType, short messageType) {
        return serializationService.createSerializer(groupType, messageType);
    }

    /**
     * Creates a message deserializer.
     *
     * @see SerializationService#createDeserializer(short, short)
     */
    public <T extends NetworkMessage> MessageDeserializer<T> createMessageDeserializer(short groupType, short messageType) {
        return serializationService.createDeserializer(groupType, messageType);
    }

    /**
     * Serializes a marshallable object to a byte array.
     *
     * @param marshallable Marshallable object to serialize.
     * @param <T> Object's type.
     * @throws UserObjectSerializationException If failed to serialize an object.
     * @see SerializationService#writeMarshallable(Object)
     */
    public <T> MarshalledObject writeMarshallable(T marshallable) throws UserObjectSerializationException {
        return serializationService.writeMarshallable(marshallable);
    }

    /**
     * Deserializes a marshallable object from a byte array.
     *
     * @param missingDescriptors Descriptors that were received from the remote node.
     * @param array Byte array that contains a serialized object.
     * @param <T> Object's type.
     * @throws UserObjectSerializationException If failed to deserialize an object.
     * @see SerializationService#readMarshallable(DescriptorRegistry, byte[])
     */
    public <T> T readMarshallable(List<ClassDescriptorMessage> missingDescriptors, byte[] array)
            throws UserObjectSerializationException {
        mergeDescriptors(missingDescriptors);

        return serializationService.readMarshallable(descriptors, array);
    }

    /**
     * Creates a list of messages holding class descriptors.
     *
     * @param descriptorIds Class descriptors.
     * @return List of class descriptor network messages.
     */
    @Nullable
    public List<ClassDescriptorMessage> createClassDescriptorsMessages(IntSet descriptorIds) {
        List<ClassDescriptorMessage> messages = descriptorIds.intStream()
                .mapToObj(serializationService::getLocalDescriptor)
                .filter(descriptor -> {
                    int descriptorId = descriptor.descriptorId();
                    return !sentDescriptors.contains(descriptorId) && !shouldBeBuiltIn(descriptorId);
                })
                .map(descriptor -> {
                    List<FieldDescriptorMessage> fields = descriptor.fields().stream()
                            .map(d -> {
                                return MSG_FACTORY.fieldDescriptorMessage()
                                        .name(d.name())
                                        .typeDescriptorId(d.typeDescriptorId())
                                        .className(d.typeName())
                                        .flags(fieldFlags(d))
                                        .build();
                            })
                            .collect(toList());

                    Serialization serialization = descriptor.serialization();

                    return MSG_FACTORY.classDescriptorMessage()
                            .fields(fields)
                            .serializationType((byte) serialization.type().value())
                            .serializationFlags(serializationAttributeFlags(serialization))
                            .descriptorId(descriptor.descriptorId())
                            .className(descriptor.className())
                            .superClassDescriptorId(superClassDescriptorIdForMessage(descriptor))
                            .superClassName(descriptor.superClassName())
                            .componentTypeDescriptorId(componentTypeDescriptorIdForMessage(descriptor))
                            .componentTypeName(descriptor.componentTypeName())
                            .attributes(classDescriptorAttributeFlags(descriptor))
                            .build();
                }).collect(toList());

        messages.forEach(classDescriptorMessage -> sentDescriptors.add(classDescriptorMessage.descriptorId()));

        return messages;
    }

    private byte fieldFlags(FieldDescriptor fieldDescriptor) {
        int bits = condMask(fieldDescriptor.isUnshared(), FieldDescriptorMessage.UNSHARED_MASK)
                | condMask(fieldDescriptor.isPrimitive(), FieldDescriptorMessage.IS_PRIMITIVE)
                | condMask(fieldDescriptor.isRuntimeTypeKnownUpfront(), FieldDescriptorMessage.IS_RUNTIME_TYPE_KNOWN_UPFRONT);
        return (byte) bits;
    }

    private byte serializationAttributeFlags(Serialization serialization) {
        int bits = condMask(serialization.hasWriteObject(), ClassDescriptorMessage.HAS_WRITE_OBJECT_MASK)
                | condMask(serialization.hasReadObject(), ClassDescriptorMessage.HAS_READ_OBJECT_MASK)
                | condMask(serialization.hasReadObjectNoData(), ClassDescriptorMessage.HAS_READ_OBJECT_NO_DATA_MASK)
                | condMask(serialization.hasWriteReplace(), ClassDescriptorMessage.HAS_WRITE_REPLACE_MASK)
                | condMask(serialization.hasReadResolve(), ClassDescriptorMessage.HAS_READ_RESOLVE_MASK);
        return (byte) bits;
    }

    private int condMask(boolean value, int mask) {
        return value ? mask : 0;
    }

    private byte classDescriptorAttributeFlags(ClassDescriptor descriptor) {
        int bits = condMask(descriptor.isPrimitive(), ClassDescriptorMessage.IS_PRIMITIVE_MASK)
                | condMask(descriptor.isArray(), ClassDescriptorMessage.IS_ARRAY_MASK)
                | condMask(descriptor.isRuntimeEnum(), ClassDescriptorMessage.IS_RUNTIME_ENUM_MASK)
                | condMask(descriptor.isRuntimeTypeKnownUpfront(), ClassDescriptorMessage.IS_RUNTIME_TYPE_KNOWN_UPFRONT_MASK);
        return (byte) bits;
    }

    private int superClassDescriptorIdForMessage(ClassDescriptor descriptor) {
        Integer id = descriptor.superClassDescriptorId();

        if (id == null) {
            return NO_DESCRIPTOR_ID;
        }

        return id;
    }

    private int componentTypeDescriptorIdForMessage(ClassDescriptor descriptor) {
        Integer id = descriptor.componentTypeDescriptorId();

        if (id == null) {
            return NO_DESCRIPTOR_ID;
        }

        return id;
    }

    private void mergeDescriptors(List<ClassDescriptorMessage> remoteDescriptors) {
        List<ClassDescriptorMessage> leftToProcess = remoteDescriptors.stream()
                .filter(classMessage -> !knownMergedDescriptor(classMessage.descriptorId()))
                .collect(toCollection(LinkedList::new));

        while (!leftToProcess.isEmpty()) {
            boolean processedSomethingDuringThisPass = false;

            Iterator<ClassDescriptorMessage> it = leftToProcess.iterator();
            while (it.hasNext()) {
                ClassDescriptorMessage classMessage = it.next();
                if (knownMergedDescriptor(classMessage.descriptorId())) {
                    it.remove();
                } else if (dependenciesAreMerged(classMessage)) {
                    Class<?> localClass = classForName(classMessage.className());
                    ClassDescriptor mergedDescriptor = messageToMergedClassDescriptor(classMessage, localClass);

                    mergedIdToDescriptorMap.put(classMessage.descriptorId(), mergedDescriptor);
                    mergedClassToDescriptorMap.put(localClass, mergedDescriptor);

                    it.remove();

                    processedSomethingDuringThisPass = true;
                }
            }

            if (!processedSomethingDuringThisPass && !leftToProcess.isEmpty()) {
                throw new IllegalStateException("Cannot merge descriptors in the correct order; a cycle? " + leftToProcess);
            }
        }
    }

    private boolean dependenciesAreMerged(ClassDescriptorMessage classMessage) {
        return knownMergedDescriptor(classMessage.superClassDescriptorId())
                && knownMergedDescriptor(classMessage.componentTypeDescriptorId());
    }

    private boolean knownMergedDescriptor(int descriptorId) {
        return shouldBeBuiltIn(descriptorId) || mergedIdToDescriptorMap.containsKey(descriptorId);
    }

    /**
     * Converts {@link ClassDescriptorMessage} to a {@link ClassDescriptor} and merges it with a local {@link ClassDescriptor} of the
     * same class.
     *
     * @param clsMsg ClassDescriptorMessage.
     * @param localClass the local class
     * @return Merged class descriptor.
     */
    private ClassDescriptor messageToMergedClassDescriptor(ClassDescriptorMessage clsMsg, Class<?> localClass) {
        ClassDescriptor localDescriptor = serializationService.getOrCreateLocalDescriptor(localClass);
        return buildRemoteDescriptor(clsMsg, localClass, localDescriptor);
    }

    private ClassDescriptor buildRemoteDescriptor(
            ClassDescriptorMessage classMessage,
            Class<?> localClass,
            ClassDescriptor localDescriptor
    ) {
        List<FieldDescriptor> remoteFields = classMessage.fields().stream()
                .map(fieldMsg -> fieldDescriptorFromMessage(fieldMsg, localClass))
                .collect(toList());

        SerializationType serializationType = SerializationType.getByValue(classMessage.serializationType());

        var serialization = new Serialization(
                serializationType,
                bitValue(classMessage.serializationFlags(), ClassDescriptorMessage.HAS_WRITE_OBJECT_MASK),
                bitValue(classMessage.serializationFlags(), ClassDescriptorMessage.HAS_READ_OBJECT_MASK),
                bitValue(classMessage.serializationFlags(), ClassDescriptorMessage.HAS_READ_OBJECT_NO_DATA_MASK),
                bitValue(classMessage.serializationFlags(), ClassDescriptorMessage.HAS_WRITE_REPLACE_MASK),
                bitValue(classMessage.serializationFlags(), ClassDescriptorMessage.HAS_READ_RESOLVE_MASK)
        );

        return ClassDescriptor.remote(
                localClass,
                classMessage.descriptorId(),
                remoteSuperClassDescriptor(classMessage),
                remoteComponentTypeDescriptor(classMessage),
                bitValue(classMessage.attributes(), ClassDescriptorMessage.IS_PRIMITIVE_MASK),
                bitValue(classMessage.attributes(), ClassDescriptorMessage.IS_ARRAY_MASK),
                bitValue(classMessage.attributes(), ClassDescriptorMessage.IS_RUNTIME_ENUM_MASK),
                bitValue(classMessage.attributes(), ClassDescriptorMessage.IS_RUNTIME_TYPE_KNOWN_UPFRONT_MASK),
                remoteFields,
                serialization,
                localDescriptor
        );
    }

    private boolean bitValue(byte flags, int bitMask) {
        return (flags & bitMask) != 0;
    }

    @Nullable
    private ClassDescriptor remoteSuperClassDescriptor(ClassDescriptorMessage clsMsg) {
        if (clsMsg.superClassDescriptorId() == NO_DESCRIPTOR_ID) {
            return null;
        }
        return remoteClassDescriptor(clsMsg.superClassDescriptorId(), clsMsg.superClassName());
    }

    @Nullable
    private ClassDescriptor remoteComponentTypeDescriptor(ClassDescriptorMessage clsMsg) {
        if (clsMsg.componentTypeDescriptorId() == NO_DESCRIPTOR_ID) {
            return null;
        }
        return remoteClassDescriptor(clsMsg.componentTypeDescriptorId(), clsMsg.componentTypeName());
    }

    private FieldDescriptor fieldDescriptorFromMessage(FieldDescriptorMessage fieldMsg, Class<?> declaringClass) {
        int typeDescriptorId = fieldMsg.typeDescriptorId();
        return FieldDescriptor.remote(
                fieldMsg.name(),
                fieldType(typeDescriptorId, fieldMsg.className()),
                typeDescriptorId,
                bitValue(fieldMsg.flags(), FieldDescriptorMessage.UNSHARED_MASK),
                bitValue(fieldMsg.flags(), FieldDescriptorMessage.IS_PRIMITIVE),
                bitValue(fieldMsg.flags(), FieldDescriptorMessage.IS_RUNTIME_TYPE_KNOWN_UPFRONT),
                declaringClass
        );
    }

    private ClassDescriptor remoteClassDescriptor(int descriptorId, String typeName) {
        if (shouldBeBuiltIn(descriptorId)) {
            return serializationService.getLocalDescriptor(descriptorId);
        } else {
            return mergedClassToDescriptorMap.get(classForName(typeName));
        }
    }

    private Class<?> fieldType(int descriptorId, String typeName) {
        if (shouldBeBuiltIn(descriptorId)) {
            return BuiltInType.findByDescriptorId(descriptorId).clazz();
        } else {
            return classForName(typeName);
        }
    }

    private Class<?> classForName(String className) {
        try {
            return Class.forName(className, true, classLoader);
        } catch (ClassNotFoundException e) {
            throw new SerializationException("Class " + className + " is not found", e);
        }
    }

    @TestOnly
    Map<Integer, ClassDescriptor> getDescriptorMapView() {
        return mergedIdToDescriptorMap;
    }
}
