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

package org.apache.ignite.internal.network.serialization;

import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.network.serialization.ClassDescriptorRegistry.shouldBeBuiltIn;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.NetworkMessagesFactory;
import org.apache.ignite.internal.network.message.ClassDescriptorMessage;
import org.apache.ignite.internal.network.message.FieldDescriptorMessage;
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
    private static final int NO_DESCRIPTOR_ID = -1;

    /** Global serialization service. */
    private final SerializationService serializationService;

    /**
     * Map with merged class descriptors. They are the result of the merging of a local and a remote descriptor.
     * The key in this map is a <b>remote</b> descriptor id.
     */
    private final Int2ObjectMap<ClassDescriptor> mergedIdToDescriptorMap = new Int2ObjectOpenHashMap<>();
    /**
     * Map with merged class descriptors. They are the result of the merging of a local and a remote descriptor.
     * The key in this map is the class name.
     */
    private final Map<String, ClassDescriptor> mergedClassToDescriptorMap = new HashMap<>();

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
                new ClassNameMapBackedClassIndexedDescriptors(mergedClassToDescriptorMap),
                serializationService.getLocalDescriptorRegistry()
        );
    }

    /**
     * Returns underlying serialization registry.
     */
    public MessageSerializationRegistry serializationRegistry() {
        return serializationService.serializationRegistry();
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
     * Returns {@code true} if the descriptor was sent, {@code false} otherwise.
     *
     * @param descriptorId Descriptor's id.
     * @return {@code true} if the descriptor was sent, {@code false} otherwise.
     */
    public boolean isDescriptorSent(int descriptorId) {
        return sentDescriptors.contains(descriptorId);
    }

    /**
     * Adds sent descriptor.
     *
     * @param descriptorId Descriptor id.
     */
    public void addSentDescriptor(int descriptorId) {
        sentDescriptors.add(descriptorId);
    }

    /**
     * Creates a list of network messages holding class descriptors.
     *
     * @param descriptorIds Class descriptor IDs
     * @param registry Class descriptor registry.
     */
    public static List<ClassDescriptorMessage> createClassDescriptorsMessages(IntSet descriptorIds, ClassDescriptorRegistry registry) {
        if (descriptorIds.isEmpty()) {
            return List.of();
        }

        var classDescriptorMessages = new ArrayList<ClassDescriptorMessage>();

        // Specially made by classical loops for optimization.
        for (IntIterator it = descriptorIds.intIterator(); it.hasNext(); ) {
            int descriptorId = it.nextInt();

            if (shouldBeBuiltIn(descriptorId)) {
                continue;
            }

            ClassDescriptor classDescriptor = registry.getRequiredDescriptor(descriptorId);

            classDescriptorMessages.add(convert(classDescriptor));
        }

        return classDescriptorMessages;
    }

    private static byte fieldFlags(FieldDescriptor fieldDescriptor) {
        int bits = condMask(fieldDescriptor.isUnshared(), FieldDescriptorMessage.UNSHARED_MASK)
                | condMask(fieldDescriptor.isPrimitive(), FieldDescriptorMessage.IS_PRIMITIVE)
                | condMask(fieldDescriptor.isSerializationTypeKnownUpfront(), FieldDescriptorMessage.IS_SERIALIZATION_TYPE_KNOWN_UPFRONT);
        return (byte) bits;
    }

    private static byte serializationAttributeFlags(Serialization serialization) {
        int bits = condMask(serialization.hasWriteObject(), ClassDescriptorMessage.HAS_WRITE_OBJECT_MASK)
                | condMask(serialization.hasReadObject(), ClassDescriptorMessage.HAS_READ_OBJECT_MASK)
                | condMask(serialization.hasReadObjectNoData(), ClassDescriptorMessage.HAS_READ_OBJECT_NO_DATA_MASK)
                | condMask(serialization.hasWriteReplace(), ClassDescriptorMessage.HAS_WRITE_REPLACE_MASK)
                | condMask(serialization.hasReadResolve(), ClassDescriptorMessage.HAS_READ_RESOLVE_MASK);
        return (byte) bits;
    }

    private static int condMask(boolean value, int mask) {
        return value ? mask : 0;
    }

    private static byte classDescriptorAttributeFlags(ClassDescriptor descriptor) {
        int bits = condMask(descriptor.isPrimitive(), ClassDescriptorMessage.IS_PRIMITIVE_MASK)
                | condMask(descriptor.isArray(), ClassDescriptorMessage.IS_ARRAY_MASK)
                | condMask(descriptor.isRuntimeEnum(), ClassDescriptorMessage.IS_RUNTIME_ENUM_MASK)
                | condMask(descriptor.isSerializationTypeKnownUpfront(), ClassDescriptorMessage.IS_SERIALIZATION_TYPE_KNOWN_UPFRONT_MASK);
        return (byte) bits;
    }

    private static int superClassDescriptorIdForMessage(ClassDescriptor descriptor) {
        Integer id = descriptor.superClassDescriptorId();

        if (id == null) {
            return NO_DESCRIPTOR_ID;
        }

        return id;
    }

    private static int componentTypeDescriptorIdForMessage(ClassDescriptor descriptor) {
        Integer id = descriptor.componentTypeDescriptorId();

        if (id == null) {
            return NO_DESCRIPTOR_ID;
        }

        return id;
    }

    /**
     * Merges incoming remote descriptors.
     *
     * @param remoteDescriptors Remote descriptors.
     */
    public void mergeDescriptors(Collection<ClassDescriptorMessage> remoteDescriptors) {
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
                    ClassDescriptor mergedDescriptor = messageToMergedClassDescriptor(classMessage);

                    mergedIdToDescriptorMap.put(classMessage.descriptorId(), mergedDescriptor);
                    mergedClassToDescriptorMap.put(classMessage.className(), mergedDescriptor);

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
     * @param classMessage ClassDescriptorMessage.
     * @return Merged class descriptor.
     */
    private ClassDescriptor messageToMergedClassDescriptor(ClassDescriptorMessage classMessage) {
        @Nullable Class<?> localClass = maybeClassForName(classMessage.className());

        if (localClass != null) {
            return buildRemoteDescriptor(classMessage, localClass);
        } else {
            return buildRemoteDescriptor(classMessage);
        }
    }

    private ClassDescriptor buildRemoteDescriptor(ClassDescriptorMessage classMessage, Class<?> localClass) {
        ClassDescriptor localDescriptor = serializationService.getOrCreateLocalDescriptor(localClass);

        List<FieldDescriptor> remoteFields = classMessage.fields().stream()
                .map(fieldMessage -> fieldDescriptorFromMessage(fieldMessage, localClass))
                .collect(toList());

        Serialization serialization = buildSerialization(classMessage);

        return ClassDescriptor.forRemote(
                localClass,
                classMessage.descriptorId(),
                remoteSuperClassDescriptor(classMessage),
                remoteComponentTypeDescriptor(classMessage),
                bitValue(classMessage.attributes(), ClassDescriptorMessage.IS_PRIMITIVE_MASK),
                bitValue(classMessage.attributes(), ClassDescriptorMessage.IS_ARRAY_MASK),
                bitValue(classMessage.attributes(), ClassDescriptorMessage.IS_RUNTIME_ENUM_MASK),
                bitValue(classMessage.attributes(), ClassDescriptorMessage.IS_SERIALIZATION_TYPE_KNOWN_UPFRONT_MASK),
                remoteFields,
                serialization,
                localDescriptor
        );
    }

    private ClassDescriptor buildRemoteDescriptor(ClassDescriptorMessage classMessage) {
        List<FieldDescriptor> remoteFields = classMessage.fields().stream()
                .map(fieldMessage -> fieldDescriptorFromMessage(fieldMessage, classMessage.className()))
                .collect(toList());

        Serialization serialization = buildSerialization(classMessage);

        return ClassDescriptor.forRemote(
                classMessage.className(),
                classMessage.descriptorId(),
                remoteSuperClassDescriptor(classMessage),
                remoteComponentTypeDescriptor(classMessage),
                bitValue(classMessage.attributes(), ClassDescriptorMessage.IS_PRIMITIVE_MASK),
                bitValue(classMessage.attributes(), ClassDescriptorMessage.IS_ARRAY_MASK),
                bitValue(classMessage.attributes(), ClassDescriptorMessage.IS_RUNTIME_ENUM_MASK),
                bitValue(classMessage.attributes(), ClassDescriptorMessage.IS_SERIALIZATION_TYPE_KNOWN_UPFRONT_MASK),
                remoteFields,
                serialization
        );
    }

    private Serialization buildSerialization(ClassDescriptorMessage classMessage) {
        SerializationType serializationType = SerializationType.getByValue(classMessage.serializationType());

        return new Serialization(
                serializationType,
                bitValue(classMessage.serializationFlags(), ClassDescriptorMessage.HAS_WRITE_OBJECT_MASK),
                bitValue(classMessage.serializationFlags(), ClassDescriptorMessage.HAS_READ_OBJECT_MASK),
                bitValue(classMessage.serializationFlags(), ClassDescriptorMessage.HAS_READ_OBJECT_NO_DATA_MASK),
                bitValue(classMessage.serializationFlags(), ClassDescriptorMessage.HAS_WRITE_REPLACE_MASK),
                bitValue(classMessage.serializationFlags(), ClassDescriptorMessage.HAS_READ_RESOLVE_MASK)
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

    private FieldDescriptor fieldDescriptorFromMessage(FieldDescriptorMessage fieldMessage, Class<?> declaringClass) {
        int typeDescriptorId = fieldMessage.typeDescriptorId();
        return FieldDescriptor.remote(
                fieldMessage.name(),
                fieldType(typeDescriptorId, fieldMessage.className()),
                typeDescriptorId,
                bitValue(fieldMessage.flags(), FieldDescriptorMessage.UNSHARED_MASK),
                bitValue(fieldMessage.flags(), FieldDescriptorMessage.IS_PRIMITIVE),
                bitValue(fieldMessage.flags(), FieldDescriptorMessage.IS_SERIALIZATION_TYPE_KNOWN_UPFRONT),
                declaringClass
        );
    }

    private FieldDescriptor fieldDescriptorFromMessage(FieldDescriptorMessage fieldMessage, String declaringClassName) {
        int typeDescriptorId = fieldMessage.typeDescriptorId();
        return FieldDescriptor.remote(
                fieldMessage.name(),
                fieldType(typeDescriptorId, fieldMessage.className()),
                typeDescriptorId,
                bitValue(fieldMessage.flags(), FieldDescriptorMessage.UNSHARED_MASK),
                bitValue(fieldMessage.flags(), FieldDescriptorMessage.IS_PRIMITIVE),
                bitValue(fieldMessage.flags(), FieldDescriptorMessage.IS_SERIALIZATION_TYPE_KNOWN_UPFRONT),
                declaringClassName
        );
    }

    private ClassDescriptor remoteClassDescriptor(int descriptorId, String typeName) {
        if (shouldBeBuiltIn(descriptorId)) {
            return serializationService.getLocalDescriptor(descriptorId);
        } else {
            return mergedClassToDescriptorMap.get(typeName);
        }
    }

    private Class<?> fieldType(int descriptorId, String typeName) {
        if (shouldBeBuiltIn(descriptorId)) {
            return BuiltInType.findByDescriptorId(descriptorId).clazz();
        } else {
            return requiredClassForName(typeName);
        }
    }

    private Class<?> requiredClassForName(String className) {
        Class<?> result = maybeClassForName(className);

        if (result == null) {
            throw new SerializationException("Class " + className + " is not found");
        }

        return result;
    }

    @Nullable
    private Class<?> maybeClassForName(String className) {
        try {
            return Class.forName(className, true, classLoader);
        } catch (ClassNotFoundException e) {
            return null;
        }
    }

    public CompositeDescriptorRegistry compositeDescriptorRegistry() {
        return descriptors;
    }


    @TestOnly
    Map<Integer, ClassDescriptor> getDescriptorMapView() {
        return mergedIdToDescriptorMap;
    }

    private static FieldDescriptorMessage convert(FieldDescriptor descriptor) {
        return MSG_FACTORY.fieldDescriptorMessage()
                .name(descriptor.name())
                .typeDescriptorId(descriptor.typeDescriptorId())
                .className(descriptor.typeName())
                .flags(fieldFlags(descriptor))
                .build();
    }

    private static ClassDescriptorMessage convert(ClassDescriptor descriptor) {
        List<FieldDescriptor> fieldDescriptors = descriptor.fields();
        List<FieldDescriptorMessage> fieldDescriptorMessages = new ArrayList<>(fieldDescriptors.size());

        // Specially made by classical loops for optimization.
        for (int i = 0; i < fieldDescriptors.size(); i++) {
            FieldDescriptor fieldDescriptor = fieldDescriptors.get(i);

            fieldDescriptorMessages.add(convert(fieldDescriptor));
        }

        Serialization serialization = descriptor.serialization();

        return MSG_FACTORY.classDescriptorMessage()
                .fields(fieldDescriptorMessages)
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
    }
}
