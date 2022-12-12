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
import java.io.Serializable;
import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.ignite.configuration.ConfigurationProperty;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.internal.configuration.ConfigurationNode;
import org.apache.ignite.internal.configuration.DynamicConfiguration;
import org.apache.ignite.internal.configuration.DynamicConfigurationChanger;
import org.apache.ignite.internal.configuration.direct.DirectConfigurationProxy;
import org.apache.ignite.internal.configuration.tree.ConfigurationSource;
import org.apache.ignite.internal.configuration.tree.ConfigurationVisitor;
import org.apache.ignite.internal.configuration.tree.ConstructableTreeNode;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.apache.ignite.internal.configuration.tree.NamedListNode;
import org.apache.ignite.internal.configuration.util.ConfigurationUtil;
import org.jetbrains.annotations.Nullable;

/**
 * Class that holds constants and commonly used fields for generators.
 */
abstract class AbstractAsmGenerator {
    /** {@link DynamicConfiguration#DynamicConfiguration} constructor. */
    static final Constructor<?> DYNAMIC_CONFIGURATION_CTOR;

    /** {@link LambdaMetafactory#metafactory(Lookup, String, MethodType, MethodType, MethodHandle, MethodType)}. */
    static final Method LAMBDA_METAFACTORY;

    /** {@link Consumer#accept(Object)}. */
    static final Method ACCEPT;

    /** {@link ConfigurationVisitor#visitLeafNode(String, Serializable)}. */
    static final Method VISIT_LEAF;

    /** {@link ConfigurationVisitor#visitInnerNode(String, InnerNode)}. */
    static final Method VISIT_INNER;

    /** {@link ConfigurationVisitor#visitNamedListNode(String, NamedListNode)}. */
    static final Method VISIT_NAMED;

    /** {@link ConfigurationSource#unwrap(Class)}. */
    static final Method UNWRAP;

    /** {@link ConfigurationSource#descend(ConstructableTreeNode)}. */
    static final Method DESCEND;

    /** {@link ConstructableTreeNode#copy()}. */
    static final Method COPY;

    /** {@link InnerNode#internalId()}. */
    static final Method INTERNAL_ID;

    /** {@code DynamicConfiguration#add} method. */
    static final Method DYNAMIC_CONFIGURATION_ADD_MTD;

    /** {@link Objects#requireNonNull(Object, String)}. */
    static final Method REQUIRE_NON_NULL;

    /** {@link Class#getName} method. */
    static final Method CLASS_GET_NAME_MTD;

    /** {@link String#equals} method. */
    static final Method STRING_EQUALS_MTD;

    /** {@link ConfigurationSource#polymorphicTypeId} method. */
    static final Method POLYMORPHIC_TYPE_ID_MTD;

    /** {@link InnerNode#constructDefault} method. */
    static final Method CONSTRUCT_DEFAULT_MTD;

    /** {@code ConfigurationNode#refreshValue} method. */
    static final Method REFRESH_VALUE_MTD;

    /** {@code DynamicConfiguration#addMember} method. */
    static final Method ADD_MEMBER_MTD;

    /** {@code DynamicConfiguration#removeMember} method. */
    static final Method REMOVE_MEMBER_MTD;

    /** {@link InnerNode#specificNode} method. */
    static final Method SPECIFIC_NODE_MTD;

    /** {@link DynamicConfiguration#specificConfigTree} method. */
    static final Method SPECIFIC_CONFIG_TREE_MTD;

    /** {@link ConfigurationUtil#addDefaults}. */
    static final Method ADD_DEFAULTS_MTD;

    /** {@link InnerNode#setInjectedNameFieldValue}. */
    static final Method SET_INJECTED_NAME_FIELD_VALUE_MTD;

    /** {@code ConfigurationNode#currentValue}. */
    static final Method CURRENT_VALUE_MTD;

    /** {@link DynamicConfiguration#isRemovedFromNamedList}. */
    static final Method IS_REMOVED_FROM_NAMED_LIST_MTD;

    /** {@link InnerNode#isPolymorphic}. */
    static final Method IS_POLYMORPHIC_MTD;

    /** {@link InnerNode#internalSchemaTypes}. */
    static final Method INTERNAL_SCHEMA_TYPES_MTD;

    /** {@code Node#convert} method name. */
    static final String CONVERT_MTD_NAME = "convert";

    /** {@link ConstructableTreeNode#construct(String, ConfigurationSource, boolean)} method name. */
    static final String CONSTRUCT_MTD_NAME = "construct";

    /** Field name for method {@link DynamicConfiguration#internalConfigTypes}. */
    static final String INTERNAL_CONFIG_TYPES_FIELD_NAME = "_internalConfigTypes";

    /** {@link DirectConfigurationProxy#DirectConfigurationProxy(List, DynamicConfigurationChanger)}. */
    static final Constructor<?> DIRECT_CFG_CTOR;

    /** {@link ConfigurationUtil#appendKey(List, Object)}. */
    static final Method APPEND_KEY;

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

            ACCEPT = Consumer.class.getDeclaredMethod("accept", Object.class);

            VISIT_LEAF = ConfigurationVisitor.class
                    .getDeclaredMethod("visitLeafNode", String.class, Serializable.class);

            VISIT_INNER = ConfigurationVisitor.class
                    .getDeclaredMethod("visitInnerNode", String.class, InnerNode.class);

            VISIT_NAMED = ConfigurationVisitor.class
                    .getDeclaredMethod("visitNamedListNode", String.class, NamedListNode.class);

            UNWRAP = ConfigurationSource.class.getDeclaredMethod("unwrap", Class.class);

            DESCEND = ConfigurationSource.class.getDeclaredMethod("descend", ConstructableTreeNode.class);

            COPY = ConstructableTreeNode.class.getDeclaredMethod("copy");

            INTERNAL_ID = InnerNode.class.getDeclaredMethod("internalId");

            DYNAMIC_CONFIGURATION_CTOR = DynamicConfiguration.class.getDeclaredConstructor(
                    List.class,
                    String.class,
                    RootKey.class,
                    DynamicConfigurationChanger.class,
                    boolean.class
            );

            DYNAMIC_CONFIGURATION_ADD_MTD = DynamicConfiguration.class.getDeclaredMethod(
                    "add",
                    ConfigurationProperty.class
            );

            REQUIRE_NON_NULL = Objects.class.getDeclaredMethod("requireNonNull", Object.class, String.class);

            CLASS_GET_NAME_MTD = Class.class.getDeclaredMethod("getName");

            STRING_EQUALS_MTD = String.class.getDeclaredMethod("equals", Object.class);

            POLYMORPHIC_TYPE_ID_MTD = ConfigurationSource.class.getDeclaredMethod("polymorphicTypeId", String.class);

            CONSTRUCT_DEFAULT_MTD = InnerNode.class.getDeclaredMethod("constructDefault", String.class);

            REFRESH_VALUE_MTD = ConfigurationNode.class.getDeclaredMethod("refreshValue");

            ADD_MEMBER_MTD = DynamicConfiguration.class.getDeclaredMethod("addMember", Map.class, ConfigurationProperty.class);

            REMOVE_MEMBER_MTD = DynamicConfiguration.class.getDeclaredMethod("removeMember", Map.class, ConfigurationProperty.class);

            SPECIFIC_NODE_MTD = InnerNode.class.getDeclaredMethod("specificNode");

            SPECIFIC_CONFIG_TREE_MTD = DynamicConfiguration.class.getDeclaredMethod("specificConfigTree");

            ADD_DEFAULTS_MTD = ConfigurationUtil.class.getDeclaredMethod("addDefaults", InnerNode.class);

            SET_INJECTED_NAME_FIELD_VALUE_MTD = InnerNode.class.getDeclaredMethod("setInjectedNameFieldValue", String.class);

            CURRENT_VALUE_MTD = ConfigurationNode.class.getDeclaredMethod("currentValue");

            IS_REMOVED_FROM_NAMED_LIST_MTD = DynamicConfiguration.class.getDeclaredMethod("isRemovedFromNamedList");

            IS_POLYMORPHIC_MTD = InnerNode.class.getDeclaredMethod("isPolymorphic");

            INTERNAL_SCHEMA_TYPES_MTD = InnerNode.class.getDeclaredMethod("internalSchemaTypes");

            DIRECT_CFG_CTOR = DirectConfigurationProxy.class.getDeclaredConstructor(List.class, DynamicConfigurationChanger.class);

            APPEND_KEY = ConfigurationUtil.class.getDeclaredMethod("appendKey", List.class, Object.class);
        } catch (NoSuchMethodException nsme) {
            throw new ExceptionInInitializerError(nsme);
        }
    }

    /** This generator instance. */
    final ConfigurationAsmGenerator cgen;

    /** Configuration schema class. */
    final Class<?> schemaClass;

    /** Internal extensions of the configuration schema. */
    final Set<Class<?>> internalExtensions;

    /** Polymorphic extensions of the configuration schema. */
    final Set<Class<?>> polymorphicExtensions;

    /** Fields of the schema class. */
    final List<Field> schemaFields;

    /** Fields of internal extensions of the configuration schema. */
    final Collection<Field> internalFields;

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
            Set<Class<?>> internalExtensions,
            Set<Class<?>> polymorphicExtensions,
            List<Field> schemaFields,
            Collection<Field> internalFields,
            Collection<Field> polymorphicFields,
            @Nullable Field internalIdField
    ) {
        this.cgen = cgen;
        this.schemaClass = schemaClass;
        this.internalExtensions = internalExtensions;
        this.polymorphicExtensions = polymorphicExtensions;
        this.schemaFields = schemaFields;
        this.internalFields = internalFields;
        this.polymorphicFields = polymorphicFields;
        this.internalIdField = internalIdField;
    }

    /**
     * Generates class definition. Expected to be called once at most.
     */
    abstract List<ClassDefinition> generate();
}
