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

import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenCustomHashMap;
import java.io.IOException;
import java.io.NotActiveException;
import java.util.Objects;
import org.apache.ignite.internal.network.serialization.ClassDescriptor;
import org.apache.ignite.internal.util.io.IgniteDataOutput;

/**
 * Context using during marshalling of an object graph accessible from a root object.
 */
class MarshallingContext {
    private final IntSet usedDescriptorIds = new IntOpenHashSet();

    private final Object2IntMap<Object> objectsToIds = new Object2IntOpenCustomHashMap<>(new IdentityHashStrategy());

    private int nextObjectId = 0;

    private Object objectCurrentlyWrittenWithWriteObject;
    private ClassDescriptor descriptorOfObjectCurrentlyWrittenWithWriteObject;

    private UosObjectOutputStream objectOutputStream;

    public void addUsedDescriptor(ClassDescriptor descriptor) {
        usedDescriptorIds.add(descriptor.descriptorId());
    }

    public IntSet usedDescriptorIds() {
        return usedDescriptorIds;
    }

    /**
     * Memorizes the object and returns an object ID (with flags) for this object. The returned value is a long that should be
     * handled using {@link FlaggedObjectIds}.
     * If {@code unshared} is {@code true}, a fresh object ID is generated and the object is not memorized.
     * If {@code unshared} is {@code false}: if the object was already seen before, its previous ID is returned; otherwise,
     * it's memorized as seen with a fresh ID which is returned.
     *
     * @param object object to operate upon ({@code null} is not supported)
     * @param unshared if this is {@code true}, then new object ID is generated even if the object was already seen before
     * @return object ID with flags (use {@link FlaggedObjectIds} to work with this value)
     */
    public long memorizeObject(Object object, boolean unshared) {
        Objects.requireNonNull(object);

        if (unshared) {
            int newId = nextId();
            return FlaggedObjectIds.freshObjectId(newId);
        }

        if (objectsToIds.containsKey(object)) {
            return FlaggedObjectIds.alreadySeenObjectId(objectsToIds.getInt(object));
        } else {
            int newId = nextId();

            objectsToIds.put(object, newId);

            return FlaggedObjectIds.freshObjectId(newId);
        }
    }

    private int nextId() {
        return nextObjectId++;
    }

    public Object objectCurrentlyWrittenWithWriteObject() throws NotActiveException {
        if (objectCurrentlyWrittenWithWriteObject == null) {
            throw new NotActiveException("not in call to writeObject");
        }

        return objectCurrentlyWrittenWithWriteObject;
    }

    public ClassDescriptor descriptorOfObjectCurrentlyWrittenWithWriteObject() {
        if (descriptorOfObjectCurrentlyWrittenWithWriteObject == null) {
            throw new IllegalStateException("No object is currently being written");
        }

        return descriptorOfObjectCurrentlyWrittenWithWriteObject;
    }

    public void startWritingWithWriteObject(Object object, ClassDescriptor descriptor) {
        objectCurrentlyWrittenWithWriteObject = object;
        descriptorOfObjectCurrentlyWrittenWithWriteObject = descriptor;
    }

    public void endWritingWithWriteObject() {
        objectCurrentlyWrittenWithWriteObject = null;
        descriptorOfObjectCurrentlyWrittenWithWriteObject = null;
    }

    UosObjectOutputStream objectOutputStream(
            IgniteDataOutput output,
            TypedValueWriter valueWriter,
            TypedValueWriter unsharedWriter,
            DefaultFieldsReaderWriter defaultFieldsReaderWriter
    ) throws IOException {
        if (objectOutputStream == null) {
            objectOutputStream = new UosObjectOutputStream(output, valueWriter, unsharedWriter, defaultFieldsReaderWriter, this);
        }

        return objectOutputStream;
    }

    private static class IdentityHashStrategy implements Hash.Strategy<Object> {
        /** {@inheritDoc} */
        @Override
        public int hashCode(Object o) {
            return System.identityHashCode(o);
        }

        /** {@inheritDoc} */
        @Override
        public boolean equals(Object a, Object b) {
            return a == b;
        }
    }
}
