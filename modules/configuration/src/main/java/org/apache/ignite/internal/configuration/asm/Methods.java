package org.apache.ignite.internal.configuration.asm;

import java.io.Serializable;
import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import org.apache.ignite.configuration.ConfigurationProperty;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.internal.configuration.ConfigurationNode;
import org.apache.ignite.internal.configuration.DynamicConfiguration;
import org.apache.ignite.internal.configuration.DynamicConfigurationChanger;
import org.apache.ignite.internal.configuration.tree.ConfigurationSource;
import org.apache.ignite.internal.configuration.tree.ConfigurationVisitor;
import org.apache.ignite.internal.configuration.tree.ConstructableTreeNode;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.apache.ignite.internal.configuration.tree.NamedListNode;
import org.apache.ignite.internal.configuration.util.ConfigurationUtil;

/**
 * Class that holds {@link Method} constants to be used by generators.
 */
public class Methods {
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
        } catch (NoSuchMethodException nsme) {
            throw new ExceptionInInitializerError(nsme);
        }
    }
}
