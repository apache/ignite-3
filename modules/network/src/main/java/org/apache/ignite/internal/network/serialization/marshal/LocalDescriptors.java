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

import org.apache.ignite.internal.network.serialization.BuiltInType;
import org.apache.ignite.internal.network.serialization.ClassDescriptor;
import org.apache.ignite.internal.network.serialization.ClassDescriptorFactory;
import org.apache.ignite.internal.network.serialization.ClassDescriptorRegistry;
import org.apache.ignite.internal.network.serialization.Classes;
import org.apache.ignite.internal.util.StringIntrospection;
import org.jetbrains.annotations.Nullable;

/**
 * Represents class descriptors for classes loaded by the local node.
 */
class LocalDescriptors {
    private final ClassDescriptorRegistry localRegistry;
    private final ClassDescriptorFactory descriptorFactory;

    LocalDescriptors(ClassDescriptorRegistry localRegistry, ClassDescriptorFactory descriptorFactory) {
        this.localRegistry = localRegistry;
        this.descriptorFactory = descriptorFactory;
    }

    ClassDescriptor getOrCreateDescriptor(@Nullable Object object) {
        if (object == null) {
            return localRegistry.getNullDescriptor();
        }
        if (object instanceof String && StringIntrospection.supportsFastGetLatin1Bytes((String) object)) {
            return localRegistry.getBuiltInDescriptor(BuiltInType.STRING_LATIN1);
        }

        return getOrCreateDescriptor(object.getClass());
    }

    private ClassDescriptor getOrCreateDescriptor(Class<?> objectClass) {
        ClassDescriptor descriptor = resolveDescriptor(objectClass);
        if (descriptor != null) {
            return descriptor;
        }

        Class<?> normalizedClass = normalizeClass(objectClass);

        return descriptorFactory.create(normalizedClass);
    }

    /**
     * Resolves a descriptor by the given class. If it's a built-in, returns the built-in. If it's custom, searches among
     * the already known descriptors for the exact class. If not found there, returns {@code null}.
     *
     * @param objectClass   class for which to obtain a descriptor
     * @return corresponding descriptor or @{code null} if nothing is found
     */
    @Nullable
    private ClassDescriptor resolveDescriptor(Class<?> objectClass) {
        if (ProxyMarshaller.isProxyClass(objectClass)) {
            return localRegistry.getProxyDescriptor();
        }

        Class<?> normalizedClass = normalizeClass(objectClass);

        return localRegistry.getDescriptor(normalizedClass);
    }

    private Class<?> normalizeClass(Class<?> objectClass) {
        if (Classes.isRuntimeEnum(objectClass)) {
            return Classes.enumClassAsInSourceCode(objectClass);
        } else {
            return objectClass;
        }
    }
}
