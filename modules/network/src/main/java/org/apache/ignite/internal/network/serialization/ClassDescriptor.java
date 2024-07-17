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

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toUnmodifiableMap;

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntMaps;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;

/**
 * Class descriptor for the user object serialization.
 */
public class ClassDescriptor implements DeclaredType {
    /**
     * Class name.
     */
    private final String className;

    /**
     * Class. It is local to the current JVM; the descriptor could
     * be created on a remote JVM/machine where a class with same name could represent a different class.
     * Might be absent in a descriptor describing a remote class that is not present locally.
     */
    @Nullable
    private final Class<?> localClass;

    /**
     * Descriptor id.
     */
    private final int descriptorId;

    /**
     * Superclass descriptor (might be missing).
     */
    @Nullable
    private final ClassDescriptor superClassDescriptor;

    /**
     * Component type descriptor (only present for arrays).
     */
    @Nullable
    private final ClassDescriptor componentTypeDescriptor;

    private final boolean isPrimitive;
    private final boolean isArray;
    private final boolean isRuntimeEnum;
    private final boolean isSerializationTypeKnownUpfront;

    /**
     * List of the declared class fields' descriptors.
     */
    private final List<FieldDescriptor> fields;

    /**
     * How the class is to be serialized.
     */
    private final Serialization serialization;

    /**
     * Descriptor of the local class corresponding to the remote class represented with this descriptor.
     * If this descriptor represents a local descriptor, this field value is equal to this descriptor.
     */
    private final ClassDescriptor localDescriptor;

    private final List<MergedField> mergedFields;

    /** Total number of bytes needed to store all primitive fields. */
    private final int primitiveFieldsDataSize;
    /** Total number of non-primitive fields. */
    private final int objectFieldsCount;

    /**
     * Size of the nulls' bitmap for the described class; it is equal to the number of nullable (i.e. non-primitive)
     * fields that have a type known upfront.
     *
     * @see #fieldNullsBitmapIndices
     * @see #isSerializationTypeKnownUpfront()
     */
    private final int fieldNullsBitmapSize;

    /**
     * Map from field names to indices in the nulls' bitmap corresponding to the described class. It contains an entry
     * for each field, but only entries for nullable (that is, non-primitive) fields which types are known upfront
     * have meaningful (non-negative) indices; all other fields have -1 as a value in this map.
     *
     * @see #isSerializationTypeKnownUpfront()
     */
    private final Object2IntMap<String> fieldNullsBitmapIndices;

    private Map<String, FieldDescriptor> fieldsByName;
    /**
     * Offsets into primitive fields data array (which has size {@link #primitiveFieldsDataSize}).
     * This array is a byte array containing data of all the primitive fields of an object.
     * (Not to be confused with the offsets used in the context of {@link sun.misc.Unsafe}).
     */
    private Object2IntMap<String> primitiveFieldDataOffsets;
    /** Indices of non-primitive fields in the object fields array. */
    private Object2IntMap<String> objectFieldIndices;

    private final List<ClassDescriptor> lineage;

    private final List<MergedLayer> mergedLineage;

    private final SpecialSerializationMethods serializationMethods;

    /**
     * Creates a descriptor describing a local (i.e. residing in this JVM) class.
     */
    public static ClassDescriptor forLocal(
            Class<?> localClass,
            int descriptorId,
            @Nullable ClassDescriptor superClassDescriptor,
            @Nullable ClassDescriptor componentTypeDescriptor,
            List<FieldDescriptor> fields,
            Serialization serialization
    ) {
        return new ClassDescriptor(localClass, descriptorId, superClassDescriptor, componentTypeDescriptor, fields, serialization);
    }

    /**
     * Creates a descriptor describing a remote class (when a local class corresponding to the remote class exists).
     */
    public static ClassDescriptor forRemote(
            Class<?> localClass,
            int descriptorId,
            @Nullable ClassDescriptor superClassDescriptor,
            @Nullable ClassDescriptor componentTypeDescriptor,
            boolean isPrimitive,
            boolean isArray,
            boolean isRuntimeEnum,
            boolean isSerializationTypeKnownUpfront,
            List<FieldDescriptor> fields,
            Serialization serialization,
            ClassDescriptor localDescriptor
    ) {
        Objects.requireNonNull(localDescriptor);

        return new ClassDescriptor(
                localClass.getName(),
                localClass,
                descriptorId,
                superClassDescriptor,
                componentTypeDescriptor,
                isPrimitive,
                isArray,
                isRuntimeEnum,
                isSerializationTypeKnownUpfront,
                fields,
                serialization,
                ClassDescriptorMerger.mergeFields(localDescriptor.fields(), fields),
                false,
                localDescriptor
        );
    }

    /**
     * Creates a descriptor describing a remote class (when no local class corresponding to the remote class exists).
     */
    public static ClassDescriptor forRemote(
            String className,
            int descriptorId,
            @Nullable ClassDescriptor superClassDescriptor,
            @Nullable ClassDescriptor componentTypeDescriptor,
            boolean isPrimitive,
            boolean isArray,
            boolean isRuntimeEnum,
            boolean isSerializationTypeKnownUpfront,
            List<FieldDescriptor> fields,
            Serialization serialization
    ) {
        return new ClassDescriptor(
                className,
                null,
                descriptorId,
                superClassDescriptor,
                componentTypeDescriptor,
                isPrimitive,
                isArray,
                isRuntimeEnum,
                isSerializationTypeKnownUpfront,
                fields,
                serialization,
                fields.stream().map(MergedField::remoteOnly).collect(toList()),
                false,
                null
        );
    }

    /**
     * Constructor for the local class case.
     */
    private ClassDescriptor(
            Class<?> localClass,
            int descriptorId,
            @Nullable ClassDescriptor superClassDescriptor,
            @Nullable ClassDescriptor componentTypeDescriptor,
            List<FieldDescriptor> fields,
            Serialization serialization
    ) {
        this(
                localClass.getName(),
                localClass,
                descriptorId,
                superClassDescriptor,
                componentTypeDescriptor,
                localClass.isPrimitive(),
                localClass.isArray(),
                Classes.isRuntimeEnum(localClass),
                Classes.isSerializationTypeKnownUpfront(localClass),
                fields,
                serialization,
                fields.stream().map(field -> new MergedField(field, field)).collect(toList()),
                true,
                null
        );
    }

    /**
     * Constructor.
     */
    private ClassDescriptor(
            String className,
            @Nullable Class<?> localClass,
            int descriptorId,
            @Nullable ClassDescriptor superClassDescriptor,
            @Nullable ClassDescriptor componentTypeDescriptor,
            boolean isPrimitive,
            boolean isArray,
            boolean isRuntimeEnum,
            boolean isSerializationTypeKnownUpfront,
            List<FieldDescriptor> fields,
            Serialization serialization,
            List<MergedField> mergedFields,
            boolean thisIsLocal,
            @Nullable ClassDescriptor localDescriptor
    ) {
        assert localClass != null && (thisIsLocal || localDescriptor != null)
                || (localClass == null && !thisIsLocal && localDescriptor == null);

        this.className = className;
        this.localClass = localClass;
        this.descriptorId = descriptorId;
        this.superClassDescriptor = superClassDescriptor;
        this.componentTypeDescriptor = componentTypeDescriptor;
        this.isPrimitive = isPrimitive;
        this.isArray = isArray;
        this.isRuntimeEnum = isRuntimeEnum;
        this.isSerializationTypeKnownUpfront = isSerializationTypeKnownUpfront;

        this.fields = List.copyOf(fields);
        this.serialization = serialization;

        this.localDescriptor = thisIsLocal ? this : localDescriptor;

        this.mergedFields = List.copyOf(mergedFields);

        primitiveFieldsDataSize = computePrimitiveFieldsDataSize(fields);
        objectFieldsCount = computeObjectFieldsCount(fields);

        fieldNullsBitmapSize = computeFieldNullsBitmapSize(fields);
        fieldNullsBitmapIndices = computeFieldNullsBitmapIndices(fields);

        lineage = computeLineage(this);
        if (thisIsLocal) {
            mergedLineage = lineage.stream().map(layer -> new MergedLayer(layer, layer)).collect(toList());
        } else if (localDescriptor == null) {
            mergedLineage = lineage.stream().map(MergedLayer::remoteOnly).collect(toList());
        } else {
            mergedLineage = ClassDescriptorMerger.mergeLineages(localDescriptor.lineage(), this.lineage);
        }

        serializationMethods = localClass != null ? new SpecialSerializationMethodsImpl(this)
                : new BrokenSerializationMethods(className);
    }

    private static int computePrimitiveFieldsDataSize(List<FieldDescriptor> fields) {
        int accumulatedBytes = 0;
        for (FieldDescriptor fieldDesc : fields) {
            if (fieldDesc.isPrimitive()) {
                accumulatedBytes += fieldDesc.primitiveWidthInBytes();
            }
        }
        return accumulatedBytes;
    }

    private static int computeObjectFieldsCount(List<FieldDescriptor> fields) {
        return (int) fields.stream()
                .filter(fieldDesc -> !fieldDesc.isPrimitive())
                .count();
    }

    private static int computeFieldNullsBitmapSize(List<FieldDescriptor> fields) {
        int count = 0;
        for (FieldDescriptor fieldDescriptor : fields) {
            if (isIncludedInNullsBitmap(fieldDescriptor)) {
                count++;
            }
        }

        return count;
    }

    private static boolean isIncludedInNullsBitmap(FieldDescriptor fieldDescriptor) {
        return !fieldDescriptor.isPrimitive() && fieldDescriptor.isSerializationTypeKnownUpfront();
    }

    private static Object2IntMap<String> computeFieldNullsBitmapIndices(List<FieldDescriptor> fields) {
        Object2IntMap<String> map = new Object2IntOpenHashMap<>();

        int index = 0;
        for (FieldDescriptor fieldDescriptor : fields) {
            int indexToPut = isIncludedInNullsBitmap(fieldDescriptor) ? (index++) : -1;
            map.put(fieldDescriptor.name(), indexToPut);
        }

        return Object2IntMaps.unmodifiable(map);
    }

    private static List<ClassDescriptor> computeLineage(ClassDescriptor descriptor) {
        List<ClassDescriptor> descriptors = new ArrayList<>();

        ClassDescriptor currentDesc = descriptor;
        while (currentDesc != null) {
            descriptors.add(currentDesc);
            currentDesc = currentDesc.superClassDescriptor();
        }

        Collections.reverse(descriptors);

        return List.copyOf(descriptors);
    }

    /**
     * Returns descriptor id.
     *
     * @return Descriptor id.
     */
    public int descriptorId() {
        return descriptorId;
    }

    /**
     * Returns descriptor of the superclass of the described class (might be {@code null}).
     *
     * @return descriptor of the superclass of the described class (might be {@code null})
     */
    @Nullable
    public ClassDescriptor superClassDescriptor() {
        return superClassDescriptor;
    }

    /**
     * Returns ID of the superclass descriptor (might be {@code null}).
     *
     * @return ID of the superclass descriptor (might be {@code null})
     */
    @Nullable
    public Integer superClassDescriptorId() {
        return superClassDescriptor == null ? null : superClassDescriptor.descriptorId();
    }

    /**
     * Returns name of the superclass (might be {@code null}).
     *
     * @return name of the superclass (might be {@code null})
     */
    @Nullable
    public String superClassName() {
        return superClassDescriptor == null ? null : superClassDescriptor.className();
    }

    /**
     * Returns descriptor of the component type (only non-{@code null} for array types).
     *
     * @return descriptor of the component type (only non-{@code null} for array types)
     */
    @Nullable
    public ClassDescriptor componentTypeDescriptor() {
        return componentTypeDescriptor;
    }

    /**
     * Returns descriptor ID of the component type (only non-{@code null} for array types).
     *
     * @return descriptor ID of the component type (only non-{@code null} for array types)
     */
    @Nullable
    public Integer componentTypeDescriptorId() {
        return componentTypeDescriptor == null ? null : componentTypeDescriptor.descriptorId();
    }

    /**
     * Returns name of the component type (only non-{@code null} for array types).
     *
     * @return name of the component type (only non-{@code null} for array types)
     */
    @Nullable
    public String componentTypeName() {
        return componentTypeDescriptor == null ? null : componentTypeDescriptor.className();
    }

    /**
     * Returns {@code true} if the array component serialization type is known upfront.
     *
     * @see #isSerializationTypeKnownUpfront()
     */
    public boolean isComponentSerializationTypeKnownUpfront() {
        return componentTypeDescriptor != null && componentTypeDescriptor.isSerializationTypeKnownUpfront();
    }

    /**
     * Returns declared fields' descriptors.
     *
     * @return Fields' descriptors.
     */
    public List<FieldDescriptor> fields() {
        return fields;
    }

    /**
     * Returns class' name.
     *
     * @return Class' name.
     */
    public String className() {
        return className;
    }

    /**
     * Returns descriptor's class (represented by a local class). Local means 'on this machine', but the descriptor could
     * be created on a remote machine where a class with same name could represent a different class.
     *
     * @return local class
     * @throws IllegalStateException if no local class exists for the described class (this could happen for a remote descriptor)
     */
    public Class<?> localClass() {
        if (localClass == null) {
            throw new IllegalStateException("No local class exists for '" + className + "'");
        }

        return localClass;
    }

    /**
     * Returns serialization.
     *
     * @return Serialization.
     */
    public Serialization serialization() {
        return serialization;
    }

    /**
     * Returns serialization type.
     *
     * @return Serialization type.
     */
    public SerializationType serializationType() {
        return serialization.type();
    }

    /**
     * Returns local and remote field descriptors merged together.
     *
     * @return local and remote field descriptors merged together
     */
    public List<MergedField> mergedFields() {
        return mergedFields;
    }

    /**
     * Returns {@code true} if the described class should be serialized as a {@link java.io.Serializable} (but not
     * using the mechanism for {@link java.io.Externalizable}).
     *
     * @return {@code true} if the described class should be serialized as a {@link java.io.Serializable}.
     */
    public boolean isSerializable() {
        return serialization.type() == SerializationType.SERIALIZABLE;
    }

    /**
     * Returns {@code true} if the described class should be serialized as an {@link java.io.Externalizable}.
     *
     * @return {@code true} if the described class should be serialized as an {@link java.io.Externalizable}.
     */
    public boolean isExternalizable() {
        return serialization.type() == SerializationType.EXTERNALIZABLE;
    }

    /**
     * Returns {@code true} if this field has a primitive type.
     *
     * @return {@code true} if this field has a primitive type
     */
    public boolean isPrimitive() {
        return isPrimitive;
    }

    /**
     * Returns {@code true} if the described class is an array class.
     *
     * @return {@code true} if the described class is an array class
     */
    public boolean isArray() {
        return isArray;
    }

    /**
     * Returns {@code true} if the descriptor describes an enum class that can have instances at runtime (i.e. it's an enum,
     * but not exactly @{code Enum.class}).
     *
     * @return {@code true} if the descriptor describes an enum class that can have instances at runtime
     */
    public boolean isRuntimeEnum() {
        return isRuntimeEnum;
    }

    /**
     * Returns {@code true} if the described class has writeObject() method.
     *
     * @return {@code true} if the described class has writeObject() method
     */
    public boolean hasWriteObject() {
        return serialization.hasWriteObject();
    }

    /**
     * Returns {@code true} if the described class has readObject() method.
     *
     * @return {@code true} if the described class has readObject() method
     */
    public boolean hasReadObject() {
        return serialization.hasReadObject();
    }

    /**
     * Returns {@code true} if the described class has readObjectNoData() method.
     *
     * @return {@code true} if the described class has readObjectNoData() method
     */
    public boolean hasReadObjectNoData() {
        return serialization.hasReadObjectNoData();
    }

    /**
     * Returns {@code true} if the described class has {@code writeReplace()} method.
     *
     * @return {@code true} if the described class has {@code writeReplace()} method
     */
    public boolean hasWriteReplace() {
        return serialization.hasWriteReplace();
    }

    /**
     * Returns {@code true} if the described class has {@code readResolve()} method.
     *
     * @return {@code true} if the described class has {@code readResolve()} method
     */
    public boolean hasReadResolve() {
        return serialization.hasReadResolve();
    }

    /**
     * Returns {@code true} if this is the descriptor of {@code null} values.
     *
     * @return {@code true} if this is the descriptor of {@code null} values
     */
    public boolean isNull() {
        return descriptorId == BuiltInType.NULL.descriptorId();
    }

    /**
     * Returns {@code true} if this is the descriptor of {@link java.util.Collections#singletonList(Object)} type.
     *
     * @return {@code true} if this is the descriptor of {@link java.util.Collections#singletonList(Object)} type
     */
    public boolean isSingletonList() {
        return descriptorId == BuiltInType.SINGLETON_LIST.descriptorId();
    }

    /**
     * Returns {@code true} if the described class has writeReplace() method, and it makes sense for the needs of
     * our serialization (i.e. it is SERIALIZABLE or EXTERNALIZABLE).
     *
     * @return {@code true} if the described class has writeReplace() method, and it makes sense for the needs of
     *     our serialization
     */
    public boolean supportsWriteReplace() {
        return (isSerializable() || isExternalizable()) && hasWriteReplace();
    }

    /**
     * Returns {@code true} if the described class is a proxy.
     *
     * @return {@code true} if the described class is a proxy
     */
    public boolean isProxy() {
        return descriptorId == BuiltInType.PROXY.descriptorId();
    }

    public boolean hasLocal() {
        return localDescriptor != null;
    }

    /**
     * Returns descriptor of the local version of the remote class described by this descriptor
     * (or {@code null} if the current descriptor describes a local class).
     *
     * @return descriptor of the local version of the remote class described by this descriptor
     *         (or {@code null} if the current descriptor describes a local class)
     * @throws IllegalStateException if no local counterpart exists for the described class (this could happen for a remote descriptor)
     */
    public ClassDescriptor local() {
        if (localDescriptor == null) {
            throw new IllegalStateException("No local descriptor exists for '" + className + "'");
        }

        return localDescriptor;
    }

    /**
     * Returns special serialization methods facility.
     *
     * @return special serialization methods facility
     */
    public SpecialSerializationMethods serializationMethods() {
        return serializationMethods;
    }

    /**
     * Returns {@code true} if this descriptor describes same class as the given descriptor.
     *
     * @param other a descriptor to match against
     * @return {@code true} if this descriptor describes same class as the given descriptor
     */
    public boolean describesSameClass(ClassDescriptor other) {
        return Objects.equals(className, other.className);
    }

    /**
     * Returns total number of bytes needed to store all primitive fields.
     *
     * @return total number of bytes needed to store all primitive fields
     */
    public int primitiveFieldsDataSize() {
        return primitiveFieldsDataSize;
    }

    /**
     * Returns total number of object (i.e. non-primitive) fields.
     *
     * @return total number of object (i.e. non-primitive) fields
     */
    public int objectFieldsCount() {
        return objectFieldsCount;
    }

    /**
     * Returns size of the nulls' bitmap for the described class; it is equal to the number of nullable (i.e. non-primitive)
     * fields that have a serialization type known upfront.
     *
     * @return size of the nulls' bitmap for the described class
     * @see #fieldIndexInNullsBitmap(String)
     * @see #isSerializationTypeKnownUpfront()
     */
    public int fieldIndexInNullsBitmapSize() {
        return fieldNullsBitmapSize;
    }

    /**
     * Returns index of a field in the nulls' bitmap for the described class (if it's nullable and its type is known upfront),
     * or -1 otherwise.
     *
     * @param fieldName name of the field
     * @return index of a field in the nulls' bitmap for the described class (if it's nullable and its type is known upfront),
     *     or -1 otherwise
     */
    public int fieldIndexInNullsBitmap(String fieldName) {
        if (!fieldNullsBitmapIndices.containsKey(fieldName)) {
            throw new IllegalStateException("Unknown field " + fieldName);
        }

        return fieldNullsBitmapIndices.getInt(fieldName);
    }

    /**
     * Returns offset into primitive fields data (which has size {@link #primitiveFieldsDataSize()}).
     * These are different from the offsets used in the context of {@link sun.misc.Unsafe}.
     *
     * @param fieldName    primitive field name
     * @param requiredTypeName type name that we expect to see for the field
     * @return offset into primitive fields data
     */
    public int primitiveFieldDataOffset(String fieldName, String requiredTypeName) {
        FieldDescriptor fieldDesc = requiredFieldByName(fieldName);
        if (!Objects.equals(fieldDesc.typeName(), requiredTypeName)) {
            throw new IllegalStateException("Field " + fieldName + " has type " + fieldDesc.typeName()
                    + ", but it was used as " + requiredTypeName);
        }

        ensurePrimitiveDataOffsetsMapComputed();

        assert primitiveFieldDataOffsets.containsKey(fieldName);

        return primitiveFieldDataOffsets.getInt(fieldName);
    }

    private void ensurePrimitiveDataOffsetsMapComputed() {
        if (primitiveFieldDataOffsets == null) {
            primitiveFieldDataOffsets = primitiveFieldDataOffsetsMap(fields);
        }
    }

    private FieldDescriptor requiredFieldByName(String fieldName) {
        FieldDescriptor fieldDesc = fieldByName(fieldName);

        if (fieldDesc == null) {
            throw new IllegalStateException("Did not find a field with name " + fieldName);
        }

        return fieldDesc;
    }

    @Nullable
    private FieldDescriptor fieldByName(String fieldName) {
        if (fieldsByName == null) {
            fieldsByName = fieldsByNameMap(fields);
        }

        return fieldsByName.get(fieldName);
    }

    private static Map<String, FieldDescriptor> fieldsByNameMap(List<FieldDescriptor> fields) {
        return fields.stream()
                .collect(toUnmodifiableMap(FieldDescriptor::name, Function.identity()));
    }

    private static Object2IntMap<String> primitiveFieldDataOffsetsMap(List<FieldDescriptor> fields) {
        Object2IntMap<String> map = new Object2IntOpenHashMap<>();

        int accumulatedOffset = 0;
        for (FieldDescriptor fieldDesc : fields) {
            if (fieldDesc.isPrimitive()) {
                map.put(fieldDesc.name(), accumulatedOffset);
                accumulatedOffset += fieldDesc.primitiveWidthInBytes();
            }
        }

        return Object2IntMaps.unmodifiable(map);
    }

    /**
     * Returns {@code true} iff the described class has a primitive field with the given name.
     *
     * @param fieldName    primitive field name
     * @return {@code true} iff the described class has a primitive field with the given name
     */
    private boolean hasPrimitiveField(String fieldName) {
        FieldDescriptor fieldDesc = fieldByName(fieldName);

        if (fieldDesc == null) {
            return false;
        }

        ensurePrimitiveDataOffsetsMapComputed();

        return primitiveFieldDataOffsets.containsKey(fieldName);
    }

    /**
     * Returns {@code true} iff the given primitive field does not exist in the remote class corresponding to this descriptor,
     * but it does exist in the local version of the class.
     *
     * @param fieldName    primitive field name
     * @return {@code true} iff the given primitive field does not exist in the remote class corresponding to this descriptor,
     *     but it does exist in the local version of the class
     */
    public boolean isPrimitiveFieldAddedLocally(String fieldName) {
        return !this.hasPrimitiveField(fieldName) && this.local().hasPrimitiveField(fieldName);
    }

    /**
     * Returns index of a non-primitive (i.e. object) field in the object fields array.
     *
     * @param fieldName object field name
     * @return index of a non-primitive (i.e. object) field in the object fields array
     */
    public int objectFieldIndex(String fieldName) {
        ensureObjectFieldIndicesComputed();

        if (!objectFieldIndices.containsKey(fieldName)) {
            throw new IllegalStateException("Did not find an object field with name " + fieldName);
        }

        return objectFieldIndices.getInt(fieldName);
    }

    private void ensureObjectFieldIndicesComputed() {
        if (objectFieldIndices == null) {
            objectFieldIndices = computeObjectFieldIndices(fields);
        }
    }

    private Object2IntMap<String> computeObjectFieldIndices(List<FieldDescriptor> fields) {
        Object2IntMap<String> map = new Object2IntOpenHashMap<>();

        int currentIndex = 0;
        for (FieldDescriptor fieldDesc : fields) {
            if (!fieldDesc.isPrimitive()) {
                map.put(fieldDesc.name(), currentIndex);
                currentIndex++;
            }
        }

        return Object2IntMaps.unmodifiable(map);
    }

    /**
     * Returns @{code true} iff the described class has an object field with the given name.
     *
     * @param fieldName object field name
     * @return @{code true} iff the described class has an object field with the given name
     */
    private boolean hasObjectField(String fieldName) {
        ensureObjectFieldIndicesComputed();

        return objectFieldIndices.containsKey(fieldName);
    }

    /**
     * Returns {@code true} iff the given object field does not exist in the remote class corresponding to this descriptor,
     * but it does exist in the local version of the class.
     *
     * @param fieldName    object field name
     * @return {@code true} iff the given object field does not exist in the remote class corresponding to this descriptor,
     *     but it does exist in the local version of the class
     */
    public boolean isObjectFieldAddedLocally(String fieldName) {
        return !this.hasObjectField(fieldName) && this.local().hasObjectField(fieldName);
    }

    /**
     * Returns the lineage (all the ancestors, from the progenitor (excluding Object) down the line, including this descriptor).
     *
     * @return ancestors from the progenitor (excluding Object) down the line, plus this descriptor
     */
    public List<ClassDescriptor> lineage() {
        return lineage;
    }

    public List<MergedLayer> mergedLineage() {
        return mergedLineage;
    }

    /**
     * Returns {@code true} if the descriptor describes a String that is represented with Latin-1 internally.
     * Needed to apply an optimization.
     *
     * @return {@code true} if the descriptor describes a String that is represented with Latin-1 internally
     */
    public boolean isLatin1String() {
        return descriptorId == BuiltInType.STRING_LATIN1.descriptorId();
    }

    /** {@inheritDoc} */
    @Override
    public int typeDescriptorId() {
        return descriptorId;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isSerializationTypeKnownUpfront() {
        return isSerializationTypeKnownUpfront;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return "ClassDescriptor{"
                + "className='" + className() + '\''
                + ", descriptorId=" + descriptorId
                + '}';
    }
}
