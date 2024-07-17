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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Class descriptor registry.
 */
public class ClassDescriptorRegistry implements DescriptorRegistry {
    /**
     * Quantity of descriptor ids reserved for the built-in descriptors.
     * 200 seems to leave enough space for future additions but at the same time it allows 55 custom descriptor IDs
     * to be lower than 255 and be encoded using 1 byte in our varint encoding.
     */
    private static final int BUILTIN_DESCRIPTORS_OFFSET_COUNT = 200;

    /** Sequential id generator for class descriptors. */
    private final AtomicInteger idGenerator = new AtomicInteger(BUILTIN_DESCRIPTORS_OFFSET_COUNT);

    // TODO: IGNITE-16464 - do not keep references to classes forever

    /** Map class -> descriptor id. */
    private final ConcurrentMap<Class<?>, Integer> idMap = new ConcurrentHashMap<>();

    /** Map descriptor id -> class descriptor. */
    private final ConcurrentMap<Integer, ClassDescriptor> descriptorMap = new ConcurrentHashMap<>();

    /**
     * Constructor.
     */
    public ClassDescriptorRegistry() {
        for (BuiltInType value : BuiltInType.values()) {
            addPredefinedDescriptor(value.clazz(), value.asClassDescriptor());
        }
    }

    /**
     * Adds predefined class descriptor with a statically configured id.
     *
     * @param clazz Class.
     * @param descriptor Descriptor.
     */
    private void addPredefinedDescriptor(Class<?> clazz, ClassDescriptor descriptor) {
        int descriptorId = descriptor.descriptorId();

        Integer existingId = idMap.put(clazz, descriptorId);

        assert existingId == null : clazz;

        ClassDescriptor existingDescriptor = descriptorMap.put(descriptorId, descriptor);

        assert existingDescriptor == null;
    }

    /**
     * Gets descriptor id for the class.
     *
     * @param clazz Class.
     * @return Descriptor id.
     */
    public int getId(Class<?> clazz) {
        return idMap.computeIfAbsent(clazz, unused -> idGenerator.getAndIncrement());
    }

    /**
     * Gets a descriptor by the id.
     *
     * @param descriptorId Descriptor id.
     * @return Descriptor.
     */
    @Override
    @Nullable
    public ClassDescriptor getDescriptor(int descriptorId) {
        return descriptorMap.get(descriptorId);
    }

    /**
     * Gets a descriptor by the class.
     *
     * @param clazz Class.
     * @return Descriptor.
     */
    @Override
    @Nullable
    public ClassDescriptor getDescriptor(Class<?> clazz) {
        Integer descriptorId = idMap.get(clazz);

        if (descriptorId == null) {
            return null;
        }

        return descriptorMap.get(descriptorId);
    }

    /**
     * Returns a descriptor for {@code null} value.
     *
     * @return a descriptor for {@code null} value
     */
    public ClassDescriptor getNullDescriptor() {
        return getRequiredDescriptor(Null.class);
    }

    /**
     * Adds a descriptor.
     *
     * @param descriptor Descriptor.
     */
    void addDescriptor(Class<?> clazz, ClassDescriptor descriptor) {
        Integer descriptorId = idMap.get(clazz);

        assert descriptorId != null : "Attempting to store an unregistered descriptor";

        int realDescriptorId = descriptor.descriptorId();

        assert descriptorId == realDescriptorId : "Descriptor id doesn't match, registered=" + descriptorId + ", real="
            + realDescriptorId;

        addDescriptorToMap(clazz, descriptor);
    }

    private void addDescriptorToMap(Class<?> clazz, ClassDescriptor descriptor) {
        assert clazz.getName().equals(descriptor.className());

        descriptorMap.put(descriptor.descriptorId(), descriptor);
    }

    /**
     * Injects a class descriptor created in another registry, with the ID assigned in the original registry.
     *
     * @param descriptor Descriptor to inject.
     */
    @TestOnly
    public void injectDescriptor(ClassDescriptor descriptor) {
        if (shouldBeBuiltIn(descriptor.typeDescriptorId())) {
            return;
        }

        assert !idMap.containsKey(descriptor.localClass());
        assert !descriptorMap.containsKey(descriptor.descriptorId());

        idMap.put(descriptor.localClass(), descriptor.descriptorId());

        addDescriptorToMap(descriptor.localClass(), descriptor);
    }

    /**
     * Returns {@code true} if descriptor with the specified descriptor id belongs to the range reserved for built-in
     * types, {@code false} otherwise.
     *
     * @param descriptorId Descriptor id.
     * @return Whether descriptor should be a built-in.
     */
    static boolean shouldBeBuiltIn(int descriptorId) {
        return descriptorId < BUILTIN_DESCRIPTORS_OFFSET_COUNT;
    }
}
