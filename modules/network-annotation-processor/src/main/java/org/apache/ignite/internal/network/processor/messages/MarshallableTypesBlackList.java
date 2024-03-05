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

package org.apache.ignite.internal.network.processor.messages;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.ArrayType;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.SimpleTypeVisitor9;
import org.apache.ignite.internal.lang.IgniteUuid;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.annotations.Marshallable;
import org.apache.ignite.internal.network.processor.ProcessingException;
import org.apache.ignite.internal.network.processor.TypeUtils;

/**
 * Class for checking that a given message field can be annotated as {@link Marshallable}. A message field can be annotated as
 * {@code Marshallable} only if it is not supported by the Direct Marshaller natively.
 */
public class MarshallableTypesBlackList {
    /** Name of the file containing a newline-separated list of blacklisted class names. */
    private static final String BLACKLIST_FILE_NAME = "marshallable.blacklist";

    /** Types supported by the Direct Marshaller. */
    public static final List<Class<?>> NATIVE_TYPES = List.of(
            // Primitive type wrappers
            Boolean.class,
            Byte.class,
            Character.class,
            Short.class,
            Integer.class,
            Long.class,
            Double.class,
            Float.class,
            Void.class,

            // Other types
            String.class,
            UUID.class,
            IgniteUuid.class,
            BitSet.class,
            ByteBuffer.class
    );

    private static final List<Class<?>> COLLECTION_TYPES = List.of(
            Collection.class,
            List.class,
            Set.class,
            Map.class
    );

    private final TypeVisitor typeVisitor;

    MarshallableTypesBlackList(TypeUtils typeUtils) {
        this.typeVisitor = new TypeVisitor(typeUtils);
    }

    boolean canBeMarshallable(TypeMirror type) {
        return typeVisitor.visit(type);
    }

    private static class TypeVisitor extends SimpleTypeVisitor9<Boolean, Void> {
        private final TypeUtils typeUtils;

        private final List<TypeMirror> resourcesBlacklist;

        TypeVisitor(TypeUtils typeUtils) {
            super(false);

            this.typeUtils = typeUtils;
            this.resourcesBlacklist = readBlacklistFromResources();
        }

        private List<TypeMirror> readBlacklistFromResources() {
            try (
                    InputStream is = getClass().getClassLoader().getResourceAsStream(BLACKLIST_FILE_NAME);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(is, UTF_8))
            ) {
                return reader.lines()
                        .map(typeUtils.elements()::getTypeElement)
                        .filter(Objects::nonNull)
                        .map(TypeElement::asType)
                        .collect(Collectors.toList());
            } catch (IOException e) {
                throw new ProcessingException("Unable to read " + BLACKLIST_FILE_NAME, e);
            }
        }

        @Override
        public Boolean visitArray(ArrayType t, Void unused) {
            return visit(t.getComponentType());
        }

        @Override
        public Boolean visitDeclared(DeclaredType t, Void unused) {
            // Check that nested collection types are also not supported by the Direct Marshaller.
            if (isSameType(COLLECTION_TYPES, t)) {
                return t.getTypeArguments().stream().anyMatch(this::visit);
            }

            return !isSameType(NATIVE_TYPES, t)
                    && !typeUtils.isSubType(t, NetworkMessage.class)
                    && !isSubType(resourcesBlacklist, t);
        }

        private boolean isSameType(List<Class<?>> types, DeclaredType type) {
            return types.stream().anyMatch(cls -> typeUtils.isSameType(type, cls));
        }

        private boolean isSubType(List<TypeMirror> types, DeclaredType type) {
            return types.stream().anyMatch(cls -> typeUtils.isSubType(type, cls));
        }
    }
}
