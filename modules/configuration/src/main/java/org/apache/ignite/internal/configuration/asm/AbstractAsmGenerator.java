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

package org.apache.ignite.internal.configuration.asm;

import com.facebook.presto.bytecode.ClassDefinition;
import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.configuration.tree.ConstructableTreeNode;
import org.jetbrains.annotations.Nullable;

/**
 * Class that holds constants and commonly used fields for generators.
 */
abstract class AbstractAsmGenerator {
    /** {@link LambdaMetafactory#metafactory(Lookup, String, MethodType, MethodType, MethodHandle, MethodType)}. */
    static final Method LAMBDA_METAFACTORY;

    /** {@link ConstructableTreeNode#copy()}. */
    static final Method COPY;

    static {
        try {
            LAMBDA_METAFACTORY = LambdaMetafactory.class.getDeclaredMethod(
                    "metafactory",
                    Lookup.class,
                    String.class,
                    MethodType.class,
                    MethodType.class,
                    MethodHandle.class,
                    MethodType.class
            );

            COPY = ConstructableTreeNode.class.getDeclaredMethod("copy");
        } catch (NoSuchMethodException nsme) {
            throw new ExceptionInInitializerError(nsme);
        }
    }

    /** This generator instance. */
    final ConfigurationAsmGenerator cgen;

    /** Configuration schema class. */
    final Class<?> schemaClass;

    /** Extensions of the configuration schema (public and internal ones). */
    final Set<Class<?>> extensions;

    /** Polymorphic extensions of the configuration schema. */
    final Set<Class<?>> polymorphicExtensions;

    /** Fields of the schema class. */
    final List<Field> schemaFields;

    /** Fields of public extensions of the configuration schema. */
    final Collection<Field> publicExtensionFields;

    /** Fields of internal extensions of the configuration schema. */
    final Collection<Field> internalExtensionFields;

    /** Fields of polymorphic extensions of the configuration schema. */
    final Collection<Field> polymorphicFields;

    /** Internal id field or {@code null} if it's not present. */
    final Field internalIdField;

    /**
     * Constructor.
     * Please refer to individual fields for comments.
     */
    AbstractAsmGenerator(
            ConfigurationAsmGenerator cgen,
            Class<?> schemaClass,
            Set<Class<?>> extensions,
            Set<Class<?>> polymorphicExtensions,
            List<Field> schemaFields,
            Collection<Field> publicExtensionFields,
            Collection<Field> internalExtensionFields,
            Collection<Field> polymorphicFields,
            @Nullable Field internalIdField
    ) {
        this.cgen = cgen;
        this.schemaClass = schemaClass;
        this.extensions = extensions;
        this.polymorphicExtensions = polymorphicExtensions;
        this.schemaFields = schemaFields;
        this.publicExtensionFields = publicExtensionFields;
        this.internalExtensionFields = internalExtensionFields;
        this.polymorphicFields = polymorphicFields;
        this.internalIdField = internalIdField;
    }

    /**
     * Generates class definitions. Expected to be called once at most. There can be more than one definition in case when there's a need
     * to generate an inner class, for example.
     */
    abstract List<ClassDefinition> generate();
}
