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

package org.apache.ignite.internal.configuration.asm;

import java.io.File;
import java.io.Serializable;
import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.bytecode.ClassGenerator;
import com.facebook.presto.bytecode.FieldDefinition;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.ParameterizedType;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.control.IfStatement;
import com.facebook.presto.bytecode.expression.BytecodeExpression;
import org.apache.ignite.configuration.ConfigurationProperty;
import org.apache.ignite.configuration.ConfigurationTree;
import org.apache.ignite.configuration.ConfigurationValue;
import org.apache.ignite.configuration.DirectConfigurationProperty;
import org.apache.ignite.configuration.NamedConfigurationTree;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.DirectAccess;
import org.apache.ignite.configuration.annotation.InternalConfiguration;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.PolymorphicConfig;
import org.apache.ignite.configuration.annotation.PolymorphicConfigInstance;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.internal.configuration.ConfigurationTreeWrapper;
import org.apache.ignite.internal.configuration.DirectConfigurationTreeWrapper;
import org.apache.ignite.internal.configuration.DirectDynamicConfiguration;
import org.apache.ignite.internal.configuration.DirectDynamicProperty;
import org.apache.ignite.internal.configuration.DirectNamedListConfiguration;
import org.apache.ignite.internal.configuration.DynamicConfiguration;
import org.apache.ignite.internal.configuration.DynamicConfigurationChanger;
import org.apache.ignite.internal.configuration.DynamicProperty;
import org.apache.ignite.internal.configuration.NamedListConfiguration;
import org.apache.ignite.internal.configuration.TypeUtils;
import org.apache.ignite.internal.configuration.tree.ConfigurationSource;
import org.apache.ignite.internal.configuration.tree.ConfigurationVisitor;
import org.apache.ignite.internal.configuration.tree.ConstructableTreeNode;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.apache.ignite.internal.configuration.tree.NamedListNode;
import org.apache.ignite.internal.configuration.util.ConfigurationUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import static com.facebook.presto.bytecode.Access.BRIDGE;
import static com.facebook.presto.bytecode.Access.FINAL;
import static com.facebook.presto.bytecode.Access.PRIVATE;
import static com.facebook.presto.bytecode.Access.PUBLIC;
import static com.facebook.presto.bytecode.Access.STATIC;
import static com.facebook.presto.bytecode.Access.SYNTHETIC;
import static com.facebook.presto.bytecode.Parameter.arg;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.bytecode.ParameterizedType.typeFromJavaClassName;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantBoolean;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantClass;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantNull;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantString;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.inlineIf;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.invokeDynamic;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.invokeStatic;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.isNull;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.newInstance;
import static java.lang.invoke.MethodType.methodType;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.EnumSet.of;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.internal.configuration.asm.SchemaClassesInfo.changeClassName;
import static org.apache.ignite.internal.configuration.asm.SchemaClassesInfo.configurationClassName;
import static org.apache.ignite.internal.configuration.asm.SchemaClassesInfo.viewClassName;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.extensionsFields;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.isConfigValue;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.isNamedConfigValue;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.isPolymorphicId;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.isValue;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.schemaFields;
import static org.apache.ignite.internal.util.ArrayUtils.nullOrEmpty;
import static org.apache.ignite.internal.util.CollectionUtils.concat;
import static org.apache.ignite.internal.util.CollectionUtils.union;
import static org.objectweb.asm.Opcodes.H_NEWINVOKESPECIAL;
import static org.objectweb.asm.Type.getMethodDescriptor;
import static org.objectweb.asm.Type.getMethodType;
import static org.objectweb.asm.Type.getType;

/**
 * This class is responsible for generating internal implementation classes for configuration schemas. It uses classes
 * from {@code bytecode} module to achieve this goal, like {@link ClassGenerator}, for examples.
 */
public class ConfigurationAsmGenerator {
    /** {@link LambdaMetafactory#metafactory(Lookup, String, MethodType, MethodType, MethodHandle, MethodType)} */
    private static final Method LAMBDA_METAFACTORY;

    /** {@link Consumer#accept(Object)} */
    private static final Method ACCEPT;

    /** {@link ConfigurationVisitor#visitLeafNode(String, Serializable)} */
    private static final Method VISIT_LEAF;

    /** {@link ConfigurationVisitor#visitInnerNode(String, InnerNode)} */
    private static final Method VISIT_INNER;

    /** {@link ConfigurationVisitor#visitNamedListNode(String, NamedListNode)} */
    private static final Method VISIT_NAMED;

    /** {@link ConfigurationSource#unwrap(Class)} */
    private static final Method UNWRAP;

    /** {@link ConfigurationSource#descend(ConstructableTreeNode)} */
    private static final Method DESCEND;

    /** {@link ConstructableTreeNode#copy()} */
    private static final Method COPY;

    /** {@link DynamicConfiguration#DynamicConfiguration} */
    private static final Constructor<?> DYNAMIC_CONFIGURATION_CTOR;

    /** {@link DirectDynamicConfiguration#DirectDynamicConfiguration} */
    private static final Constructor<?> DIRECT_DYNAMIC_CONFIGURATION_CTOR;

    /** {@link DynamicConfiguration#add(ConfigurationProperty)} */
    private static final Method DYNAMIC_CONFIGURATION_ADD;

    /** {@link Objects#requireNonNull(Object, String)} */
    private static final Method REQUIRE_NON_NULL;

    /** {@link Object#Object()}. */
    private static final Constructor<?> OBJECT_CTOR;

    /** {@link DynamicProperty#value} method. */
    private static final Method DYNAMIC_PROPERTY_VALUE_MTD;

    /** {@link Class#getName} method. */
    private static final Method CLASS_GET_NAME_MTD;

    /** {@link InnerNode#specificView} method name. */
    private static final String SPECIFIC_VIEW_MTD_NAME = "specificView";

    /** {@link InnerNode#specificChange} method name. */
    private static final String SPECIFIC_CHANGE_MTD_NAME = "specificChange";

    /** {@link DynamicConfiguration#specificConfigTree} method name. */
    private static final String SPECIFIC_CONFIG_TREE_MTD_NAME = "specificConfigTree";

    /** {@code Node#convert} method name. */
    private static final String CONVERT_MTD_NAME = "convert";

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

            DYNAMIC_CONFIGURATION_CTOR = DynamicConfiguration.class.getDeclaredConstructor(
                List.class,
                String.class,
                RootKey.class,
                DynamicConfigurationChanger.class,
                boolean.class
            );

            DIRECT_DYNAMIC_CONFIGURATION_CTOR = DirectDynamicConfiguration.class.getDeclaredConstructor(
                List.class,
                String.class,
                RootKey.class,
                DynamicConfigurationChanger.class,
                boolean.class
            );

            DYNAMIC_CONFIGURATION_ADD = DynamicConfiguration.class.getDeclaredMethod(
                "add",
                ConfigurationProperty.class
            );

            REQUIRE_NON_NULL = Objects.class.getDeclaredMethod("requireNonNull", Object.class, String.class);

            OBJECT_CTOR = Object.class.getConstructor();

            DYNAMIC_PROPERTY_VALUE_MTD = DynamicProperty.class.getMethod("value");

            CLASS_GET_NAME_MTD = Class.class.getMethod("getName");
        }
        catch (NoSuchMethodException nsme) {
            throw new ExceptionInInitializerError(nsme);
        }
    }

    /** Information about schema classes - bunch of names and dynamically compiled internal classes. */
    private final Map<Class<?>, SchemaClassesInfo> schemasInfo = new HashMap<>();

    /** Class generator instance. */
    private final ClassGenerator generator = ClassGenerator.classGenerator(getClass().getClassLoader())
        .dumpClassFilesTo(new File("C:\\test").toPath());
    // TODO: IGNITE-14645 ^^^

    /**
     * Creates new instance of {@code *Node} class corresponding to the given Configuration Schema.
     *
     * @param schemaClass Configuration Schema class.
     * @return Node instance.
     */
    public synchronized InnerNode instantiateNode(Class<?> schemaClass) {
        SchemaClassesInfo info = schemasInfo.get(schemaClass);

        assert info != null && info.nodeClass != null : schemaClass;

        try {
            Constructor<? extends InnerNode> constructor = info.nodeClass.getConstructor();

            assert constructor.canAccess(null);

            return constructor.newInstance();
        }
        catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Creates new instance of {@code *Configuration} class corresponding to the given Configuration Schema.
     *
     * @param rootKey Root key of the configuration root.
     * @param changer Configuration changer instance to pass into constructor.
     * @return Configuration instance.
     */
    public synchronized DynamicConfiguration<?, ?> instantiateCfg(
        RootKey<?, ?> rootKey,
        DynamicConfigurationChanger changer
    ) {
        SchemaClassesInfo info = schemasInfo.get(rootKey.schemaClass());

        assert info != null && info.cfgImplClass != null;

        try {
            Constructor<? extends DynamicConfiguration<?, ?>> constructor = info.cfgImplClass.getConstructor(
                List.class,
                String.class,
                RootKey.class,
                DynamicConfigurationChanger.class,
                boolean.class
            );

            assert constructor.canAccess(null);

            return constructor.newInstance(Collections.emptyList(), rootKey.key(), rootKey, changer, false);
        }
        catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Generates, defines, loads and initializes all dynamic classes required for the given configuration schema.
     *
     * @param rootSchemaClass             Class of the root configuration schema.
     * @param internalSchemaExtensions    Internal extensions ({@link InternalConfiguration})
     *                                    of configuration schemas ({@link ConfigurationRoot} and {@link Config}).
     *                                    Mapping: original schema -> extensions.
     * @param polymorphicSchemaExtensions Polymorphic extensions ({@link PolymorphicConfigInstance})
     *                                    of configuration schemas ({@link PolymorphicConfig}).
     *                                    Mapping: original schema -> extensions.
     */
    public synchronized void compileRootSchema(
        Class<?> rootSchemaClass,
        Map<Class<?>, Set<Class<?>>> internalSchemaExtensions,
        Map<Class<?>, Set<Class<?>>> polymorphicSchemaExtensions
    ) {
        if (schemasInfo.containsKey(rootSchemaClass))
            return; // Already compiled.

        Queue<Class<?>> compileQueue = new ArrayDeque<>();
        compileQueue.add(rootSchemaClass);

        schemasInfo.put(rootSchemaClass, new SchemaClassesInfo(rootSchemaClass));

        Set<Class<?>> schemas = new HashSet<>();
        List<ClassDefinition> classDefs = new ArrayList<>();

        while (!compileQueue.isEmpty()) {
            Class<?> schemaClass = compileQueue.poll();

            assert schemaClass.isAnnotationPresent(ConfigurationRoot.class)
                || schemaClass.isAnnotationPresent(Config.class)
                || schemaClass.isAnnotationPresent(PolymorphicConfig.class)
                : schemaClass + " is not properly annotated";

            assert schemasInfo.containsKey(schemaClass) : schemaClass;

            Set<Class<?>> internalExtensions = internalSchemaExtensions.getOrDefault(schemaClass, Set.of());
            Set<Class<?>> polymorphicExtensions = polymorphicSchemaExtensions.getOrDefault(schemaClass, Set.of());

            assert internalExtensions.isEmpty() || polymorphicExtensions.isEmpty() :
                "Internal and polymorphic extensions are not allowed at the same time: " + schemaClass;

            Collection<Field> schemaFields = schemaFields(schemaClass);
            Collection<Field> internalExtensionsFields = extensionsFields(internalExtensions, true);
            Collection<Field> polymorphicExtensionsFields = extensionsFields(polymorphicExtensions, false);

            for (Field schemaField : concat(schemaFields, internalExtensionsFields, polymorphicExtensionsFields)) {
                if (isConfigValue(schemaField) || isNamedConfigValue(schemaField)) {
                    Class<?> subSchemaClass = schemaField.getType();

                    if (!schemasInfo.containsKey(subSchemaClass)) {
                        compileQueue.offer(subSchemaClass);

                        schemasInfo.put(subSchemaClass, new SchemaClassesInfo(subSchemaClass));
                    }
                }
            }

            for (Class<?> polymorphicExtension : polymorphicExtensions)
                schemasInfo.put(polymorphicExtension, new SchemaClassesInfo(polymorphicExtension));

            schemas.add(schemaClass);

            ClassDefinition innerNodeClassDef = createNodeClass(
                schemaClass,
                internalExtensions,
                polymorphicExtensions,
                schemaFields,
                internalExtensionsFields,
                polymorphicExtensionsFields
            );

            classDefs.add(innerNodeClassDef);

            ClassDefinition cfgImplClassDef = createCfgImplClass(
                schemaClass,
                internalExtensions,
                polymorphicExtensions,
                schemaFields,
                internalExtensionsFields,
                polymorphicExtensionsFields
            );

            classDefs.add(cfgImplClassDef);

            for (Class<?> polymorphicExtension : polymorphicExtensions) {
                // Only the fields of a specific instance of a polymorphic configuration.
                Collection<Field> polymorphicFields = polymorphicExtensionsFields.stream()
                    .filter(f -> f.getDeclaringClass() == polymorphicExtension)
                    .collect(toList());

                classDefs.add(createPolymorphicExtensionNodeClass(
                    schemaClass,
                    polymorphicExtension,
                    innerNodeClassDef,
                    schemaFields,
                    polymorphicFields
                ));

                classDefs.add(createPolymorphicExtensionCfgImplClass(
                    schemaClass,
                    polymorphicExtension,
                    cfgImplClassDef,
                    schemaFields,
                    polymorphicFields
                ));
            }
        }

        Map<String, Class<?>> definedClasses = generator.defineClasses(classDefs);

        for (Class<?> schemaClass : schemas) {
            SchemaClassesInfo info = schemasInfo.get(schemaClass);

            info.nodeClass = (Class<? extends InnerNode>)definedClasses.get(info.nodeClassName);
            info.cfgImplClass = (Class<? extends DynamicConfiguration<?, ?>>)definedClasses.get(info.cfgImplClassName);
        }
    }

    /**
     * Construct a {@link InnerNode} definition for a configuration schema.
     *
     * @param schemaClass           Configuration schema class.
     * @param internalExtensions    Internal extensions of the configuration schema.
     * @param polymorphicExtensions Polymorphic extensions of the configuration schema.
     * @param schemaFields          Fields of the schema class.
     * @param internalFields        Fields of internal extensions of the configuration schema.
     * @param polymorphicFields     Fields of polymorphic extensions of the configuration schema.
     * @return Constructed {@link InnerNode} definition for the configuration schema.
     */
    private ClassDefinition createNodeClass(
        Class<?> schemaClass,
        Set<Class<?>> internalExtensions,
        Set<Class<?>> polymorphicExtensions,
        Collection<Field> schemaFields,
        Collection<Field> internalFields,
        Collection<Field> polymorphicFields
    ) {
        SchemaClassesInfo schemaClassInfo = schemasInfo.get(schemaClass);

        // Node class definition.
        ClassDefinition classDef = new ClassDefinition(
            of(PUBLIC, FINAL),
            internalName(schemaClassInfo.nodeClassName),
            type(InnerNode.class),
            nodeClassInterfaces(schemaClass, internalExtensions)
        );

        // Spec fields.
        Map<Class<?>, FieldDefinition> specFields = new HashMap<>();

        int i = 0;

        for (Class<?> clazz : concat(List.of(schemaClass), internalExtensions, polymorphicExtensions))
            specFields.put(clazz, classDef.declareField(of(PRIVATE, FINAL), "_spec" + i++, clazz));

        // Define the rest of the fields.
        Map<String, FieldDefinition> fieldDefs = new HashMap<>();

        // To store the id of the polymorphic configuration instance.
        FieldDefinition polymorphicTypeIdFieldDef = null;

        for (Field schemaField : concat(schemaFields, internalFields, polymorphicFields)) {
            String fieldName = fieldName(schemaField);

            FieldDefinition fieldDef = addNodeField(classDef, schemaField, fieldName);

            fieldDefs.put(fieldName, fieldDef);

            if (isPolymorphicId(schemaField))
                polymorphicTypeIdFieldDef = fieldDef;
        }

        // org.apache.ignite.internal.configuration.tree.InnerNode#schemaType
        addNodeSchemaTypeMethod(classDef, schemaClass, polymorphicExtensions, specFields, polymorphicTypeIdFieldDef);

        // Constructor.
        addNodeConstructor(classDef, specFields, fieldDefs, schemaFields, internalFields, polymorphicFields);

        // VIEW and CHANGE methods.
        for (Field schemaField : concat(schemaFields, internalFields)) {
            // Must be skipped, this is an internal special field.
            if (isPolymorphicId(schemaField))
                continue;

            String fieldName = schemaField.getName();

            FieldDefinition fieldDef = fieldDefs.get(fieldName);

            addNodeViewMethod(classDef, schemaField, fieldDef);

            // Add change methods.
            MethodDefinition changeMtd =
                addNodeChangeMethod(classDef, schemaField, schemaClassInfo.nodeClassName, fieldDef);

            addNodeChangeBridgeMethod(classDef, changeClassName(schemaField.getDeclaringClass()), changeMtd);
        }

        // traverseChildren
        addNodeTraverseChildrenMethod(
            classDef,
            schemaClass,
            fieldDefs,
            schemaFields,
            internalFields,
            polymorphicFields,
            polymorphicTypeIdFieldDef
        );

        // traverseChild
        addNodeTraverseChildMethod(
            classDef,
            fieldDefs,
            schemaFields,
            internalFields,
            polymorphicFields,
            polymorphicTypeIdFieldDef
        );

        // construct
        addNodeConstructMethod(
            classDef,
            fieldDefs,
            schemaFields,
            internalFields,
            polymorphicFields,
            polymorphicTypeIdFieldDef
        );

        // constructDefault
        addNodeConstructDefaultMethod(
            classDef,
            schemaClass,
            specFields,
            fieldDefs,
            schemaFields,
            internalFields,
            polymorphicFields,
            polymorphicTypeIdFieldDef
        );

        if (!polymorphicExtensions.isEmpty()) {
            addNodeSpecificViewMethod(classDef, schemaClass, polymorphicExtensions, polymorphicTypeIdFieldDef);

            addNodeSpecificChangeMethod(classDef, schemaClass, polymorphicExtensions, polymorphicTypeIdFieldDef);

            addNodeConvertMethod(classDef, schemaClass, polymorphicExtensions, polymorphicTypeIdFieldDef);
        }

        return classDef;
    }

    /**
     * Add {@link InnerNode#schemaType()} method implementation to the class.
     *
     * @param classDef                  Class definition.
     * @param schemaClass               Configuration schema class.
     * @param polymorphicExtensions     Polymorphic extensions of the configuration schema.
     * @param specFields                Field definitions for the schema and its extensions: {@code _spec#}.
     * @param polymorphicTypeIdFieldDef Identification field for the polymorphic configuration instance.
     */
    private static void addNodeSchemaTypeMethod(
        ClassDefinition classDef,
        Class<?> schemaClass,
        Set<Class<?>> polymorphicExtensions,
        Map<Class<?>, FieldDefinition> specFields,
        @Nullable FieldDefinition polymorphicTypeIdFieldDef
    ) {
        MethodDefinition schemaTypeMtd = classDef.declareMethod(of(PUBLIC), "schemaType", type(Class.class));

        BytecodeBlock mtdBody = schemaTypeMtd.getBody();
        Variable thisVar = schemaTypeMtd.getThis();

        if (polymorphicTypeIdFieldDef == null)
            mtdBody.append(invokeGetClass(thisVar.getField(specFields.get(schemaClass))).ret());
        else {
            StringSwitchBuilder switchBuilderTypeId = typeIdSwitchBuilder(schemaTypeMtd, polymorphicTypeIdFieldDef);

            switchBuilderTypeId.addCase(
                schemaClass.getAnnotation(PolymorphicConfig.class).id(),
                invokeGetClass(thisVar.getField(specFields.get(schemaClass))).ret()
            );

            for (Class<?> extension : polymorphicExtensions) {
                switchBuilderTypeId.addCase(
                    extension.getAnnotation(PolymorphicConfigInstance.class).id(),
                    invokeGetClass(thisVar.getField(specFields.get(extension))).ret()
                );
            }

            mtdBody.append(switchBuilderTypeId.build());
        }
    }

    /**
     * Declares field that corresponds to configuration value. Depending on the schema, 3 options possible:
     * <ul>
     *     <li>
     *         {@code @Value public type fieldName}<br/>becomes<br/>
     *         {@code public BoxedType fieldName}
     *     </li>
     *     <li>
     *         {@code @ConfigValue public MyConfigurationSchema fieldName}<br/>becomes<br/>
     *         {@code public MyNode fieldName}
     *     </li>
     *     <li>
     *         {@code @NamedConfigValue public type fieldName}<br/>becomes<br/>
     *         {@code public NamedListNode fieldName}
     *     </li>
     *     <li>
     *         {@code @PolymorphicId public String fieldName}<br/>becomes<br/>
     *         {@code public String fieldName}
     *     </li>
     * </ul>
     *
     * @param classDef    Node class definition.
     * @param schemaField Configuration Schema class field.
     * @param fieldName   Field name.
     * @return Declared field definition.
     */
    private FieldDefinition addNodeField(ClassDefinition classDef, Field schemaField, String fieldName) {
        Class<?> schemaFieldClass = schemaField.getType();

        ParameterizedType nodeFieldType;

        if (isValue(schemaField) || isPolymorphicId(schemaField))
            nodeFieldType = type(box(schemaFieldClass));
        else if (isConfigValue(schemaField))
            nodeFieldType = typeFromJavaClassName(schemasInfo.get(schemaFieldClass).nodeClassName);
        else
            nodeFieldType = type(NamedListNode.class);

        return classDef.declareField(of(PUBLIC), fieldName, nodeFieldType);
    }

    /**
     * Implements default constructor for the node class. It initializes {@code _spec} field and every other field
     * that represents named list configuration.
     *
     * @param classDef          Node class definition.
     * @param specFields        Definition of fields for the {@code _spec#} fields of the node class.
     *                          Mapping: configuration schema class -> {@code _spec#} field.
     * @param fieldDefs         Field definitions for all fields of node class excluding {@code _spec}.
     * @param schemaFields      Fields of the schema class.
     * @param internalFields    Fields of internal extensions of the configuration schema.
     * @param polymorphicFields Fields of polymorphic extensions of the configuration schema.
     */
    private void addNodeConstructor(
        ClassDefinition classDef,
        Map<Class<?>, FieldDefinition> specFields,
        Map<String, FieldDefinition> fieldDefs,
        Collection<Field> schemaFields,
        Collection<Field> internalFields,
        Collection<Field> polymorphicFields
    ) {
        MethodDefinition ctor = classDef.declareConstructor(of(PUBLIC));

        // super();
        ctor.getBody().append(ctor.getThis()).invokeConstructor(InnerNode.class);

        // this._spec# = new MyConfigurationSchema();
        for (Map.Entry<Class<?>, FieldDefinition> e : specFields.entrySet())
            ctor.getBody().append(ctor.getThis().setField(e.getValue(), newInstance(e.getKey())));

        for (Field schemaField : concat(schemaFields, internalFields, polymorphicFields)) {
            if (!isNamedConfigValue(schemaField))
                continue;

            String fieldName = fieldName(schemaField);

            initNodeNamedConfigValue(ctor, schemaField, fieldDefs.get(fieldName));
        }

        // return;
        ctor.getBody().ret();
    }

    /**
     * Implements getter method from {@code VIEW} interface. It returns field value, possibly unboxed or cloned,
     * depending on type.
     *
     * @param classDef    Node class definition.
     * @param schemaField Configuration Schema class field.
     * @param fieldDefs   Field definitions.
     */
    private void addNodeViewMethod(
        ClassDefinition classDef,
        Field schemaField,
        FieldDefinition... fieldDefs
    ) {
        assert !nullOrEmpty(fieldDefs);

        Class<?> schemaFieldType = schemaField.getType();

        ParameterizedType returnType;

        SchemaClassesInfo schemaClassInfo = schemasInfo.get(schemaFieldType);

        // Return type is either corresponding VIEW type or the same type as declared in schema.
        if (isConfigValue(schemaField))
            returnType = typeFromJavaClassName(schemaClassInfo.viewClassName);
        else if (isNamedConfigValue(schemaField))
            returnType = type(NamedListView.class);
        else
            returnType = type(schemaFieldType);

        String fieldName = schemaField.getName();

        MethodDefinition viewMtd = classDef.declareMethod(
            of(PUBLIC),
            fieldName,
            returnType
        );

        // result = this.field;
        viewMtd.getBody().append(getThisFieldCode(viewMtd, fieldDefs));

        if (schemaFieldType.isPrimitive()) {
            // result = Box.boxValue(result); // Unboxing.
            viewMtd.getBody().invokeVirtual(
                box(schemaFieldType),
                schemaFieldType.getSimpleName() + "Value",
                schemaFieldType
            );
        }
        else if (schemaFieldType.isArray()) {
            // result = result.clone();
            viewMtd.getBody().invokeVirtual(schemaFieldType, "clone", Object.class).checkCast(schemaFieldType);
        }
        else if (schemaFieldType.isAnnotationPresent(PolymorphicConfig.class)) {
            // result = result.specificView();
            viewMtd.getBody().invokeVirtual(
                typeFromJavaClassName(schemaClassInfo.nodeClassName),
                SPECIFIC_VIEW_MTD_NAME,
                type(Object.class)
            );
        }

        // return result;
        viewMtd.getBody().ret(schemaFieldType);
    }

    /**
     * Implements changer method from {@code CHANGE} interface.
     *
     * @param classDef Node class definition.
     * @param schemaField Configuration Schema class field.
     * @param nodeClassName Class name for the Node class.
     * @param fieldDefs Field definitions.
     * @return Definition of change method.
     */
    private static MethodDefinition addNodeChangeMethod(
        ClassDefinition classDef,
        Field schemaField,
        String nodeClassName,
        FieldDefinition... fieldDefs
    ) {
        assert !nullOrEmpty(fieldDefs);

        Class<?> schemaFieldType = schemaField.getType();

        MethodDefinition changeMtd = classDef.declareMethod(
            of(PUBLIC),
            "change" + capitalize(schemaField.getName()),
            typeFromJavaClassName(nodeClassName),
            // Change argument type is a Consumer for all inner or named fields.
            arg("change", isValue(schemaField) ? type(schemaFieldType) : type(Consumer.class))
        );

        BytecodeBlock changeBody = changeMtd.getBody();

        // newValue = change;
        BytecodeExpression newValue = changeMtd.getScope().getVariable("change");

        if (!schemaFieldType.isPrimitive()) {
            // Objects.requireNonNull(newValue, "change");
            changeBody.append(invokeStatic(REQUIRE_NON_NULL, newValue, constantString("change")));
        }

        if (isValue(schemaField)) {
            // newValue = Box.valueOf(newValue); // Boxing.
            if (schemaFieldType.isPrimitive()) {
                ParameterizedType type = type(box(schemaFieldType));

                newValue = invokeStatic(type, "valueOf", type, singleton(newValue));
            }

            // newValue = newValue.clone();
            if (schemaFieldType.isArray())
                newValue = newValue.invoke("clone", Object.class).cast(schemaFieldType);

            // this.field = newValue;
            changeBody.append(setThisFieldCode(changeMtd, newValue, fieldDefs));
        }
        else {
            // this.field = (this.field == null) ? new ValueNode() : (ValueNode)this.field.copy();
            changeBody.append(copyNodeField(changeMtd, fieldDefs));

            // this.field;
            BytecodeExpression getFieldCode = getThisFieldCode(changeMtd, fieldDefs);

            // change.accept(this.field);
            changeBody.append(changeMtd.getScope().getVariable("change").invoke(ACCEPT, getFieldCode));
        }

        // return this;
        changeBody.append(changeMtd.getThis()).retObject();

        return changeMtd;
    }

    /**
     * Implements changer bridge method from {@code CHANGE} interface.
     *
     * @param classDef        Node class definition.
     * @param changeClassName Class name for the CHANGE class.
     * @param changeMtd       Definition of change method.
     */
    private static void addNodeChangeBridgeMethod(
        ClassDefinition classDef,
        String changeClassName,
        MethodDefinition changeMtd
    ) {
        MethodDefinition bridgeMtd = classDef.declareMethod(
            of(PUBLIC, SYNTHETIC, BRIDGE),
            changeMtd.getName(),
            typeFromJavaClassName(changeClassName),
            changeMtd.getParameters()
        );

        Variable changeVar = bridgeMtd.getScope().getVariable("change");

        // this.change*(change);
        BytecodeExpression invokeChangeMtd = bridgeMtd.getThis().invoke(changeMtd, List.of(changeVar));

        // return this.change*(change);
        bridgeMtd.getBody().append(invokeChangeMtd).retObject();
    }

    /**
     * Implements {@link InnerNode#traverseChildren(ConfigurationVisitor, boolean)} method.
     *
     * @param classDef                  Class definition.
     * @param schemaClass               Configuration schema class.
     * @param fieldDefs                 Definitions for all fields in {@code schemaFields}.
     * @param schemaFields              Fields of the schema class.
     * @param internalFields            Fields of internal extensions of the configuration schema.
     * @param polymorphicFields         Fields of polymorphic extensions of the configuration schema.
     * @param polymorphicTypeIdFieldDef Identification field for the polymorphic configuration instance.
     */
    private static void addNodeTraverseChildrenMethod(
        ClassDefinition classDef,
        Class<?> schemaClass,
        Map<String, FieldDefinition> fieldDefs,
        Collection<Field> schemaFields,
        Collection<Field> internalFields,
        Collection<Field> polymorphicFields,
        @Nullable FieldDefinition polymorphicTypeIdFieldDef
    ) {
        MethodDefinition traverseChildrenMtd = classDef.declareMethod(
            of(PUBLIC),
            "traverseChildren",
            type(void.class),
            arg("visitor", type(ConfigurationVisitor.class)),
            arg("includeInternal", type(boolean.class))
        ).addException(NoSuchElementException.class);

        BytecodeBlock mtdBody = traverseChildrenMtd.getBody();

        for (Field schemaField : schemaFields) {
            mtdBody.append(
                invokeVisit(traverseChildrenMtd, schemaField, fieldDefs.get(schemaField.getName())).pop()
            );
        }

        if (!internalFields.isEmpty()) {
            BytecodeBlock includeInternalBlock = new BytecodeBlock();

            for (Field internalField : internalFields) {
                includeInternalBlock.append(
                    invokeVisit(traverseChildrenMtd, internalField, fieldDefs.get(internalField.getName())).pop()
                );
            }

            mtdBody.append(
                new IfStatement()
                    .condition(traverseChildrenMtd.getScope().getVariable("includeInternal"))
                    .ifTrue(includeInternalBlock)
            );
        }
        else if (polymorphicTypeIdFieldDef != null) {
            StringSwitchBuilder switchBuilderTypeId = typeIdSwitchBuilder(traverseChildrenMtd, polymorphicTypeIdFieldDef)
                .addCase(schemaClass.getAnnotation(PolymorphicConfig.class).id(), new BytecodeBlock());

            Map<Class<?>, List<Field>> groupedByClass = polymorphicFields.stream()
                .collect(groupingBy(Field::getDeclaringClass, LinkedHashMap::new, toList()));

            for (Map.Entry<Class<?>, List<Field>> e : groupedByClass.entrySet()) {
                BytecodeBlock codeBlock = new BytecodeBlock();

                for (Field polymorphicField : e.getValue()) {
                    String fieldName = fieldName(polymorphicField);

                    codeBlock.append(
                        invokeVisit(traverseChildrenMtd, polymorphicField, fieldDefs.get(fieldName)).pop()
                    );
                }

                switchBuilderTypeId.addCase(
                    e.getKey().getAnnotation(PolymorphicConfigInstance.class).id(),
                    codeBlock
                );
            }

            mtdBody.append(switchBuilderTypeId.build());
        }

        mtdBody.ret();
    }

    /**
     * Implements {@link InnerNode#traverseChild(String, ConfigurationVisitor, boolean)} method.
     *
     * @param classDef                  Class definition.
     * @param fieldDefs                 Definitions for all fields in {@code schemaFields}.
     * @param schemaFields              Fields of the schema class.
     * @param internalFields            Fields of internal extensions of the configuration schema.
     * @param polymorphicFields         Fields of polymorphic extensions of the configuration schema.
     * @param polymorphicTypeIdFieldDef Identification field for the polymorphic configuration instance.
     */
    private static void addNodeTraverseChildMethod(
        ClassDefinition classDef,
        Map<String, FieldDefinition> fieldDefs,
        Collection<Field> schemaFields,
        Collection<Field> internalFields,
        Collection<Field> polymorphicFields,
        @Nullable FieldDefinition polymorphicTypeIdFieldDef
    ) {
        MethodDefinition traverseChildMtd = classDef.declareMethod(
            of(PUBLIC),
            "traverseChild",
            type(Object.class),
            arg("key", type(String.class)),
            arg("visitor", type(ConfigurationVisitor.class)),
            arg("includeInternal", type(boolean.class))
        ).addException(NoSuchElementException.class);

        Variable keyVar = traverseChildMtd.getScope().getVariable("key");

        StringSwitchBuilder switchBuilder = new StringSwitchBuilder(traverseChildMtd.getScope())
            .expression(keyVar)
            .defaultCase(throwException(NoSuchElementException.class, keyVar));

        for (Field schemaField : schemaFields) {
            String fieldName = schemaField.getName();

            switchBuilder.addCase(
                fieldName,
                invokeVisit(traverseChildMtd, schemaField, fieldDefs.get(fieldName)).retObject()
            );
        }

        if (polymorphicTypeIdFieldDef != null) {
            Map<String, List<Field>> groupByName = polymorphicFields.stream().collect(groupingBy(Field::getName));

            for (Map.Entry<String, List<Field>> e : groupByName.entrySet()) {
                StringSwitchBuilder switchBuilderTypeId = typeIdSwitchBuilder(traverseChildMtd, polymorphicTypeIdFieldDef);

                for (Field polymorphicField : e.getValue()) {
                    String fieldName = fieldName(polymorphicField);

                    switchBuilderTypeId.addCase(
                        polymorphicField.getDeclaringClass().getAnnotation(PolymorphicConfigInstance.class).id(),
                        invokeVisit(traverseChildMtd, polymorphicField, fieldDefs.get(fieldName)).retObject()
                    );
                }

                switchBuilder.addCase(e.getKey(), switchBuilderTypeId.build());
            }

            traverseChildMtd.getBody().append(switchBuilder.build());
        }
        else if (!internalFields.isEmpty()) {
            StringSwitchBuilder switchBuilderAllFields = new StringSwitchBuilder(traverseChildMtd.getScope())
                .expression(keyVar)
                .defaultCase(throwException(NoSuchElementException.class, keyVar));

            for (Field schemaField : union(schemaFields, internalFields)) {
                String fieldName = schemaField.getName();

                switchBuilderAllFields.addCase(
                    fieldName,
                    invokeVisit(traverseChildMtd, schemaField, fieldDefs.get(fieldName)).retObject()
                );
            }

            traverseChildMtd.getBody().append(
                new IfStatement()
                    .condition(traverseChildMtd.getScope().getVariable("includeInternal"))
                    .ifTrue(switchBuilderAllFields.build())
                    .ifFalse(switchBuilder.build())
            );
        }
        else
            traverseChildMtd.getBody().append(switchBuilder.build());
    }

    /**
     * Creates bytecode block that invokes one of {@link ConfigurationVisitor}'s methods.
     *
     * @param mtd         Method definition, either {@link InnerNode#traverseChildren(ConfigurationVisitor, boolean)} or
     *                    {@link InnerNode#traverseChild(String, ConfigurationVisitor, boolean)} defined in {@code *Node} class.
     * @param schemaField Configuration Schema field to visit.
     * @param fieldDef    Field definition from current class.
     * @return Bytecode block that invokes "visit*" method.
     */
    private static BytecodeBlock invokeVisit(MethodDefinition mtd, Field schemaField, FieldDefinition fieldDef) {
        Method visitMethod;

        if (isValue(schemaField) || isPolymorphicId(schemaField))
            visitMethod = VISIT_LEAF;
        else if (isConfigValue(schemaField))
            visitMethod = VISIT_INNER;
        else
            visitMethod = VISIT_NAMED;

        return new BytecodeBlock().append(mtd.getScope().getVariable("visitor").invoke(
            visitMethod,
            constantString(schemaField.getName()),
            mtd.getThis().getField(fieldDef)
        ));
    }

    /**
     * Implements {@link ConstructableTreeNode#construct(String, ConfigurationSource, boolean)} method.
     *
     * @param classDef                  Class definition.
     * @param fieldDefs                 Definitions for all fields in {@code schemaFields}.
     * @param schemaFields              Fields of the schema class.
     * @param internalFields            Fields of internal extensions of the configuration schema.
     * @param polymorphicFields         Fields of polymorphic extensions of the configuration schema.
     * @param polymorphicTypeIdFieldDef Identification field for the polymorphic configuration instance.
     */
    private void addNodeConstructMethod(
        ClassDefinition classDef,
        Map<String, FieldDefinition> fieldDefs,
        Collection<Field> schemaFields,
        Collection<Field> internalFields,
        Collection<Field> polymorphicFields,
        @Nullable FieldDefinition polymorphicTypeIdFieldDef
    ) {
        MethodDefinition constructMtd = classDef.declareMethod(
            of(PUBLIC),
            "construct",
            type(void.class),
            arg("key", type(String.class)),
            arg("src", type(ConfigurationSource.class)),
            arg("includeInternal", type(boolean.class))
        ).addException(NoSuchElementException.class);

        Variable keyVar = constructMtd.getScope().getVariable("key");

        StringSwitchBuilder switchBuilder = new StringSwitchBuilder(constructMtd.getScope())
            .expression(keyVar)
            .defaultCase(throwException(NoSuchElementException.class, keyVar));

        for (Field schemaField : schemaFields) {
            String fieldName = schemaField.getName();

            switchBuilder.addCase(
                fieldName,
                treatSourceForConstruct(constructMtd, schemaField, fieldDefs.get(fieldName))
            );
        }

        if (!polymorphicFields.isEmpty()) {
            Map<String, List<Field>> groupByName = polymorphicFields.stream().collect(groupingBy(Field::getName));

            for (Map.Entry<String, List<Field>> e : groupByName.entrySet()) {
                StringSwitchBuilder switchBuilderTypeId = typeIdSwitchBuilder(constructMtd, polymorphicTypeIdFieldDef);

                for (Field polymorphicField : e.getValue()) {
                    String fieldName = fieldName(polymorphicField);

                    switchBuilderTypeId.addCase(
                        polymorphicField.getDeclaringClass().getAnnotation(PolymorphicConfigInstance.class).id(),
                        treatSourceForConstruct(constructMtd, polymorphicField, fieldDefs.get(fieldName))
                    );
                }

                switchBuilder.addCase(e.getKey(), switchBuilderTypeId.build());
            }

            // TODO: IGNITE-14645 what about polymorphicTypeIdFieldDef ??

            constructMtd.getBody().append(switchBuilder.build()).ret();
        }
        else if (!internalFields.isEmpty()) {
            StringSwitchBuilder switchBuilderAllFields = new StringSwitchBuilder(constructMtd.getScope())
                .expression(keyVar)
                .defaultCase(throwException(NoSuchElementException.class, keyVar));

            for (Field schemaField : union(schemaFields, internalFields)) {
                String fieldName = schemaField.getName();

                switchBuilderAllFields.addCase(
                    fieldName,
                    treatSourceForConstruct(constructMtd, schemaField, fieldDefs.get(fieldName))
                );
            }

            constructMtd.getBody().append(
                new IfStatement().condition(constructMtd.getScope().getVariable("includeInternal"))
                    .ifTrue(switchBuilderAllFields.build())
                    .ifFalse(switchBuilder.build())
            ).ret();
        }
        else
            constructMtd.getBody().append(switchBuilder.build()).ret();
    }

    /**
     * Implements {@link InnerNode#constructDefault(String)} method.
     *
     * @param classDef                  Class definition.
     * @param schemaClass               Configuration schema class.
     * @param specFields                Field definitions for the schema and its extensions: {@code _spec#}.
     * @param fieldDefs                 Definitions for all fields in {@code schemaFields}.
     * @param schemaFields              Fields of the schema class.
     * @param internalFields            Fields of internal extensions of the configuration schema.
     * @param polymorphicFields         Fields of polymorphic extensions of the configuration schema.
     * @param polymorphicTypeIdFieldDef Identification field for the polymorphic configuration instance.
     */
    private static void addNodeConstructDefaultMethod(
        ClassDefinition classDef,
        Class<?> schemaClass,
        Map<Class<?>, FieldDefinition> specFields,
        Map<String, FieldDefinition> fieldDefs,
        Collection<Field> schemaFields,
        Collection<Field> internalFields,
        Collection<Field> polymorphicFields,
        @Nullable FieldDefinition polymorphicTypeIdFieldDef
    ) {
        MethodDefinition constructDfltMtd = classDef.declareMethod(
            of(PUBLIC),
            "constructDefault",
            type(void.class),
            arg("key", String.class)
        ).addException(NoSuchElementException.class);

        Variable keyVar = constructDfltMtd.getScope().getVariable("key");

        StringSwitchBuilder switchBuilder = new StringSwitchBuilder(constructDfltMtd.getScope())
            .expression(keyVar)
            .defaultCase(throwException(NoSuchElementException.class, keyVar));

        for (Field schemaField : concat(schemaFields, internalFields)) {
            if (!isValue(schemaField))
                continue;

            if (!schemaField.getAnnotation(Value.class).hasDefault()) {
                switchBuilder.addCase(schemaField.getName(), new BytecodeBlock());

                continue;
            }

            String fieldName = schemaField.getName();
            FieldDefinition fieldDef = fieldDefs.get(fieldName);
            FieldDefinition specFieldDef = specFields.get(schemaField.getDeclaringClass());

            switchBuilder.addCase(
                fieldName,
                addNodeConstructDefault(constructDfltMtd, schemaField, fieldDef, specFieldDef)
            );
        }

        if (polymorphicTypeIdFieldDef != null) {
            Map<String, List<Field>> groupByName = polymorphicFields.stream()
                .filter(ConfigurationUtil::isValue)
                .collect(groupingBy(Field::getName));

            Variable thisVar = constructDfltMtd.getThis();

            for (Map.Entry<String, List<Field>> e : groupByName.entrySet()) {
                BytecodeExpression typeIdVar = thisVar.getField(polymorphicTypeIdFieldDef);

                StringSwitchBuilder switchBuilder1 = new StringSwitchBuilder(constructDfltMtd.getScope())
                    .expression(typeIdVar)
                    .defaultCase(throwException(NoSuchElementException.class, typeIdVar));

                for (Field polymorphicField : e.getValue()) {
                    String fieldName = fieldName(polymorphicField);
                    FieldDefinition fieldDef = fieldDefs.get(fieldName);

                    Class<?> polymorphicClass = polymorphicField.getDeclaringClass();
                    FieldDefinition specFieldDef = specFields.get(polymorphicClass);

                    switchBuilder1.addCase(
                        polymorphicClass.getAnnotation(PolymorphicConfigInstance.class).id(),
                        addNodeConstructDefault(constructDfltMtd, polymorphicField, fieldDef, specFieldDef)
                    );
                }

                switchBuilder.addCase(e.getKey(), switchBuilder1.build());
            }

            // TODO: IGNITE-14645 Maybe change.
            // this.typeId = "base";
            switchBuilder.addCase(
                polymorphicTypeIdFieldDef.getName(),
                thisVar.setField(
                    polymorphicTypeIdFieldDef,
                    constantString(schemaClass.getAnnotation(PolymorphicConfig.class).id())
                )
            );
        }

        constructDfltMtd.getBody().append(switchBuilder.build()).ret();
    }

    /**
     * Copies field into itself or instantiates it if the field is null.
     *
     * @param mtd       Method definition.
     * @param fieldDefs Field definitions.
     * @return Bytecode expression.
     */
    private static BytecodeExpression copyNodeField(MethodDefinition mtd, FieldDefinition... fieldDefs) {
        assert !nullOrEmpty(fieldDefs);

        // this.field;
        BytecodeExpression getFieldCode = getThisFieldCode(mtd, fieldDefs);

        ParameterizedType fieldType = fieldDefs[fieldDefs.length - 1].getType();

        // (this.field == null) ? new ValueNode() : (ValueNode)this.field.copy();
        BytecodeExpression value = inlineIf(
            isNull(getFieldCode),
            newInstance(fieldType),
            getFieldCode.invoke(COPY).cast(fieldType)
        );

        // this.field = (this.field == null) ? new ValueNode() : (ValueNode)this.field.copy();
        return setThisFieldCode(mtd, value, fieldDefs);
    }

    /**
     * Creates {@code *Node::new} lambda expression with {@link Supplier} type.
     *
     * @param nodeClassName Name of the {@code *Node} class.
     * @return InvokeDynamic bytecode expression.
     */
    @NotNull private static BytecodeExpression newNamedListElementLambda(String nodeClassName) {
        return invokeDynamic(
            LAMBDA_METAFACTORY,
            asList(
                getMethodType(getType(Object.class)),
                new Handle(
                    H_NEWINVOKESPECIAL,
                    internalName(nodeClassName),
                    "<init>",
                    getMethodDescriptor(Type.VOID_TYPE),
                    false
                ),
                getMethodType(typeFromJavaClassName(nodeClassName).getAsmType())
            ),
            "get",
            methodType(Supplier.class)
        );
    }

    /**
     * Construct a {@link DynamicConfiguration} definition for a configuration schema.
     *
     * @param schemaClass        Configuration schema class.
     * @param internalExtensions Internal extensions of the configuration schema.
     * @param schemaFields       Fields of the schema class.
     * @param internalFields     Fields of internal extensions of the configuration schema.
     * @param polymorphicFields  Fields of polymorphic extensions of the configuration schema.
     * @return Constructed {@link DynamicConfiguration} definition for the configuration schema.
     */
    private ClassDefinition createCfgImplClass(
        Class<?> schemaClass,
        Set<Class<?>> internalExtensions,
        Set<Class<?>> polymorphicExtensions,
        Collection<Field> schemaFields,
        Collection<Field> internalFields,
        Collection<Field> polymorphicFields
    ) {
        SchemaClassesInfo schemaClassInfo = schemasInfo.get(schemaClass);

        Class<?> superClass = schemaClassInfo.direct ? DirectDynamicConfiguration.class : DynamicConfiguration.class;

        // Configuration impl class definition.
        ClassDefinition classDef = new ClassDefinition(
            of(PUBLIC, FINAL),
            internalName(schemaClassInfo.cfgImplClassName),
            type(superClass),
            configClassInterfaces(schemaClass, internalExtensions)
        );

        // Fields.
        Map<String, FieldDefinition> fieldDefs = new HashMap<>();

        // To store the id of the polymorphic configuration instance.
        FieldDefinition polymorphicTypeIdFieldDef = null;

        for (Field schemaField : concat(schemaFields, internalFields, polymorphicFields)) {
            String fieldName = fieldName(schemaField);

            FieldDefinition fieldDef = addConfigurationImplField(classDef, schemaField, fieldName);

            fieldDefs.put(fieldName, fieldDef);

            if (isPolymorphicId(schemaField))
                polymorphicTypeIdFieldDef = fieldDef;
        }

        // Constructor
        addConfigurationImplConstructor(
            classDef,
            schemaClassInfo,
            fieldDefs,
            schemaFields,
            internalFields,
            polymorphicFields
        );

        for (Field schemaField : concat(schemaFields, internalFields)) {
            // Must be skipped, this is an internal special field.
            if (isPolymorphicId(schemaField))
                continue;

            addConfigurationImplGetMethod(classDef, schemaField, fieldDefs.get(fieldName(schemaField)));
        }

        // org.apache.ignite.internal.configuration.DynamicConfiguration#configType
        addCfgImplConfigTypeMethod(classDef, typeFromJavaClassName(schemaClassInfo.cfgClassName));

        if (!polymorphicExtensions.isEmpty())
            addNodeSpecificConfigTreeMethod(classDef, schemaClass, polymorphicExtensions, polymorphicTypeIdFieldDef);

        return classDef;
    }

    /**
     * Declares field that corresponds to configuration value. Depending on the schema, 3 options possible:
     * <ul>
     *     <li>
     *         {@code @Value public type fieldName}<br/>becomes<br/>
     *         {@code public DynamicProperty fieldName}
     *     </li>
     *     <li>
     *         {@code @ConfigValue public MyConfigurationSchema fieldName}<br/>becomes<br/>
     *         {@code public MyConfiguration fieldName}
     *     </li>
     *     <li>
     *         {@code @NamedConfigValue public type fieldName}<br/>becomes<br/>
     *         {@code public NamedListConfiguration fieldName}
     *     </li>
     *     <li>
     *         {@code @PolymorphicId public String fieldName}<br/>becomes<br/>
     *         {@code public String fieldName}
     *     </li>
     * </ul>
     *
     * @param classDef    Configuration impl class definition.
     * @param schemaField Configuration Schema class field.
     * @param fieldName Field name, if {@code null} will be used {@link Field#getName}.
     * @return Declared field definition.
     */
    private FieldDefinition addConfigurationImplField(
        ClassDefinition classDef,
        Field schemaField,
        String fieldName
    ) {
        ParameterizedType fieldType;

        if (isConfigValue(schemaField))
            fieldType = typeFromJavaClassName(schemasInfo.get(schemaField.getType()).cfgImplClassName);
        else if (isNamedConfigValue(schemaField))
            fieldType = type(NamedListConfiguration.class);
        else
            fieldType = type(DynamicProperty.class);

        return classDef.declareField(of(PUBLIC), fieldName, fieldType);
    }

    /**
     * Implements default constructor for the configuration class. It initializes all fields and adds them to members
     * collection.
     *
     * @param classDef         Configuration impl class definition.
     * @param schemaClassInfo  Configuration Schema class info.
     * @param fieldDefs        Field definitions for all fields of configuration impl class.
     * @param schemaFields     Fields of the schema class.
     * @param internalFields Fields of internal extensions of the configuration schema.
     */
    private void addConfigurationImplConstructor(
        ClassDefinition classDef,
        SchemaClassesInfo schemaClassInfo,
        Map<String, FieldDefinition> fieldDefs,
        Collection<Field> schemaFields,
        Collection<Field> internalFields,
        Collection<Field> polymorphicFields
    ) {
        MethodDefinition ctor = classDef.declareConstructor(
            of(PUBLIC),
            arg("prefix", List.class),
            arg("key", String.class),
            arg("rootKey", RootKey.class),
            arg("changer", DynamicConfigurationChanger.class),
            arg("listenOnly", boolean.class)
        );

        Variable rootKeyVar = ctor.getScope().getVariable("rootKey");
        Variable changerVar = ctor.getScope().getVariable("changer");
        Variable listenOnlyVar = ctor.getScope().getVariable("listenOnly");

        Constructor<?> superCtor = schemaClassInfo.direct ?
            DIRECT_DYNAMIC_CONFIGURATION_CTOR : DYNAMIC_CONFIGURATION_CTOR;

        BytecodeBlock ctorBody = ctor.getBody()
            .append(ctor.getThis())
            .append(ctor.getScope().getVariable("prefix"))
            .append(ctor.getScope().getVariable("key"))
            .append(rootKeyVar)
            .append(changerVar)
            .append(listenOnlyVar)
            .invokeConstructor(superCtor);

        BytecodeExpression thisKeysVar = ctor.getThis().getField("keys", List.class);

        int newIdx = 0;
        for (Field schemaField : concat(schemaFields, internalFields, polymorphicFields)) {
            String fieldName = fieldName(schemaField);

            BytecodeExpression newValue;

            if (isValue(schemaField) || isPolymorphicId(schemaField)) {
                Class<?> fieldImplClass = schemaField.isAnnotationPresent(DirectAccess.class) ?
                    DirectDynamicProperty.class : DynamicProperty.class;

                // newValue = new DynamicProperty(super.keys, fieldName, rootKey, changer, listenOnly);
                newValue = newInstance(
                    fieldImplClass,
                    thisKeysVar,
                    constantString(fieldName),
                    rootKeyVar,
                    changerVar,
                    listenOnlyVar
                );
            }
            else {
                SchemaClassesInfo fieldInfo = schemasInfo.get(schemaField.getType());

                ParameterizedType cfgImplParameterizedType = typeFromJavaClassName(fieldInfo.cfgImplClassName);

                if (isConfigValue(schemaField)) {
                    // newValue = new MyConfigurationImpl(super.keys, fieldName, rootKey, changer, listenOnly);
                    newValue = newInstance(
                        cfgImplParameterizedType,
                        thisKeysVar,
                        constantString(fieldName),
                        rootKeyVar,
                        changerVar,
                        listenOnlyVar
                    );
                }
                else {
                    // We have to create method "$new$<idx>" to reference it in lambda expression. That's the way it
                    // works, it'll invoke constructor with all 5 arguments, not just 2 as in BiFunction.
                    MethodDefinition newMtd = classDef.declareMethod(
                        of(PRIVATE, STATIC, SYNTHETIC),
                        "$new$" + newIdx++,
                        typeFromJavaClassName(fieldInfo.cfgClassName),
                        arg("rootKey", RootKey.class),
                        arg("changer", DynamicConfigurationChanger.class),
                        arg("listenOnly", boolean.class),
                        arg("prefix", List.class),
                        arg("key", String.class)
                    );

                    Class<?> fieldImplClass = fieldInfo.direct ?
                        DirectNamedListConfiguration.class : NamedListConfiguration.class;

                    // newValue = new NamedListConfiguration(this.keys, fieldName, rootKey, changer, listenOnly,
                    //      (p, k) -> new ValueConfigurationImpl(p, k, rootKey, changer, listenOnly),
                    //      new ValueConfigurationImpl(this.keys, "any", rootKey, changer, true)
                    // );
                    newValue = newInstance(
                        fieldImplClass,
                        thisKeysVar,
                        constantString(fieldName),
                        rootKeyVar,
                        changerVar,
                        listenOnlyVar,
                        invokeDynamic(
                            LAMBDA_METAFACTORY,
                            asList(
                                getMethodType(getType(Object.class), getType(Object.class), getType(Object.class)),
                                new Handle(
                                    Opcodes.H_INVOKESTATIC,
                                    internalName(schemaClassInfo.cfgImplClassName),
                                    newMtd.getName(),
                                    newMtd.getMethodDescriptor(),
                                    false
                                ),
                                getMethodType(
                                    typeFromJavaClassName(fieldInfo.cfgClassName).getAsmType(),
                                    getType(List.class),
                                    getType(String.class)
                                )
                            ),
                            "apply",
                            BiFunction.class,
                            rootKeyVar,
                            changerVar,
                            listenOnlyVar
                        ),
                        newInstance(
                            cfgImplParameterizedType,
                            thisKeysVar,
                            constantString("any"),
                            rootKeyVar,
                            changerVar,
                            constantBoolean(true)
                        ).cast(ConfigurationProperty.class)
                    );

                    newMtd.getBody()
                        .append(newInstance(
                            cfgImplParameterizedType,
                            newMtd.getScope().getVariable("prefix"),
                            newMtd.getScope().getVariable("key"),
                            newMtd.getScope().getVariable("rootKey"),
                            newMtd.getScope().getVariable("changer"),
                            newMtd.getScope().getVariable("listenOnly")
                        ))
                        .retObject();
                }
            }

            FieldDefinition fieldDef = fieldDefs.get(fieldName);

            // this.field = newValue;
            ctorBody.append(ctor.getThis().setField(fieldDef, newValue));

            // add(this.field);
            ctorBody.append(ctor.getThis().invoke(DYNAMIC_CONFIGURATION_ADD, ctor.getThis().getField(fieldDef)));
        }

        ctorBody.ret();
    }

    /**
     * Implements accessor method in configuration impl class.
     *
     * @param classDef    Configuration impl class definition.
     * @param schemaField Configuration Schema class field.
     * @param fieldDefs Field definitions.
     */
    private void addConfigurationImplGetMethod(
        ClassDefinition classDef,
        Field schemaField,
        FieldDefinition... fieldDefs
    ) {
        assert !nullOrEmpty(fieldDefs);

        Class<?> schemaFieldType = schemaField.getType();

        String fieldName = schemaField.getName();

        ParameterizedType returnType;

        SchemaClassesInfo schemaClassInfo = schemasInfo.get(schemaFieldType);

        if (isConfigValue(schemaField))
            returnType = typeFromJavaClassName(schemaClassInfo.cfgClassName);
        else if (isNamedConfigValue(schemaField))
            returnType = type(NamedConfigurationTree.class);
        else {
            assert isValue(schemaField) : schemaField.getDeclaringClass();

            returnType = type(ConfigurationValue.class);
        }

        // public ConfigurationProperty fieldName()
        MethodDefinition viewMtd = classDef.declareMethod(
            of(PUBLIC),
            fieldName,
            returnType
        );

        // result = this.field;
        BytecodeBlock body = viewMtd.getBody().append(getThisFieldCode(viewMtd, fieldDefs));

        if (schemaFieldType.isAnnotationPresent(PolymorphicConfig.class)) {
            // result = this.field.specificConfigTree();
            body.invokeVirtual(
                type(DynamicConfiguration.class),
                SPECIFIC_CONFIG_TREE_MTD_NAME,
                type(ConfigurationTree.class)
            );
        }

        // return result;
        body.retObject();
    }

    /**
     * Replaces first letter in string with its upper-cased variant.
     *
     * @param name Some string.
     * @return Capitalized version of passed string.
     */
    private static String capitalize(String name) {
        return name.substring(0, 1).toUpperCase() + name.substring(1);
    }

    /**
     * Returns internalized version of class name, replacing dots with slashes.
     *
     * @param className Class name (with package).
     * @return Internal class name.
     * @see Type#getInternalName(Class)
     */
    @NotNull private static String internalName(String className) {
        return className.replace('.', '/');
    }

    /**
     * Creates boxed version of the class. Types that it can box: {@code boolean}, {@code int}, {@code long} and
     * {@code double}. Other primitive types are not supported by configuration framework.
     *
     * @param clazz Maybe primitive class.
     * @return Not primitive class that represents parameter class.
     */
    private static Class<?> box(Class<?> clazz) {
        Class<?> boxed = TypeUtils.boxed(clazz);

        return boxed == null ? clazz : boxed;
    }

    /**
     * Get interfaces for {@link InnerNode} definition for a configuration schema.
     *
     * @param schemaClass Configuration schema class.
     * @param schemaExtensions Internal extensions of the configuration schema.
     * @return Interfaces for {@link InnerNode} definition for a configuration schema.
     */
    private static ParameterizedType[] nodeClassInterfaces(Class<?> schemaClass, Set<Class<?>> schemaExtensions) {
        return Stream.concat(Stream.of(schemaClass), schemaExtensions.stream())
            .flatMap(cls -> Stream.of(viewClassName(cls), changeClassName(cls)))
            .map(ParameterizedType::typeFromJavaClassName)
            .toArray(ParameterizedType[]::new);
    }

    /**
     * Get interfaces for {@link DynamicConfiguration} definition for a configuration schema.
     *
     * @param schemaClass Configuration schema class.
     * @param schemaExtensions Internal extensions of the configuration schema.
     * @return Interfaces for {@link DynamicConfiguration} definition for a configuration schema.
     */
    private ParameterizedType[] configClassInterfaces(Class<?> schemaClass, Set<Class<?>> schemaExtensions) {
        var result = new ArrayList<ParameterizedType>();

        Stream.concat(Stream.of(schemaClass), schemaExtensions.stream())
            .map(cls -> typeFromJavaClassName(configurationClassName(cls)))
            .forEach(result::add);

        if (schemasInfo.get(schemaClass).direct)
            result.add(type(DirectConfigurationProperty.class));

        return result.toArray(new ParameterizedType[0]);
    }

    /**
     * Add {@link DynamicConfiguration#configType} method implementation to the class. It looks like the following code:
     * <pre><code>
     * public Class configType() {
     *     return RootConfiguration.class;
     * }
     * </code></pre>
     * @param classDef Class definition.
     * @param clazz Definition of the configuration interface, for example {@code RootConfiguration}.
     */
    private static void addCfgImplConfigTypeMethod(ClassDefinition classDef, ParameterizedType clazz) {
        classDef.declareMethod(of(PUBLIC), "configType", type(Class.class))
            .getBody()
            .append(constantClass(clazz))
            .retObject();
    }

    /**
     * Create a {@code *Node} for the polymorphic configuration instance schema.
     *
     * @param schemaClass Polymorphic configuration schema (parent).
     * @param polymorphicExtension Polymorphic configuration instance schema (child).
     * @param schemaInnerNodeClassDef {@link InnerNode} definition for the polymorphic configuration schema {@code schemaClass}.
     * @param schemaFields Schema fields of polymorphic configuration {@code schemaClass}.
     * @param polymorphicFields Schema fields of a polymorphic configuration instance {@code polymorphicExtension}.
     */
    private ClassDefinition createPolymorphicExtensionNodeClass(
        Class<?> schemaClass,
        Class<?> polymorphicExtension,
        ClassDefinition schemaInnerNodeClassDef,
        Collection<Field> schemaFields,
        Collection<Field> polymorphicFields
    ) {
        SchemaClassesInfo schemaClassInfo = schemasInfo.get(schemaClass);
        SchemaClassesInfo polymorphicExtensionClassInfo = schemasInfo.get(polymorphicExtension);

        // Node class definition.
        ClassDefinition classDef = new ClassDefinition(
            of(PUBLIC, FINAL),
            internalName(polymorphicExtensionClassInfo.nodeClassName),
            type(Object.class),
            nodeClassInterfaces(polymorphicExtension, Set.of())
        );

        // private final ParentNode parent#innerNode;
        FieldDefinition parentInnerNodeFieldDef = classDef.declareField(
            of(PRIVATE, FINAL),
            "parent#innerNode",
            typeFromJavaClassName(schemaClassInfo.nodeClassName)
        );

        // Constructor.
        MethodDefinition constructorMtd = classDef.declareConstructor(
            of(PUBLIC),
            arg("parent", typeFromJavaClassName(schemaClassInfo.nodeClassName))
        );

        // Constructor body.
        constructorMtd.getBody()
            .append(constructorMtd.getThis())
            .append(constructorMtd.getThis().setField(
                parentInnerNodeFieldDef,
                constructorMtd.getScope().getVariable("parent")
            ))
            .invokeConstructor(OBJECT_CTOR)
            .ret();

        Map<String, FieldDefinition> fieldDefs = schemaInnerNodeClassDef.getFields().stream()
            .collect(toMap(FieldDefinition::getName, identity()));

        // Creates view and change methods for parent schema.
        for (Field schemaField : schemaFields) {
            // Must be skipped, this is an internal special field.
            if (isPolymorphicId(schemaField))
                continue;

            FieldDefinition schemaFieldDef = fieldDefs.get(fieldName(schemaField));

            addNodeViewMethod(classDef, schemaField, parentInnerNodeFieldDef, schemaFieldDef);

            MethodDefinition changeMtd = addNodeChangeMethod(
                classDef,
                schemaField,
                polymorphicExtensionClassInfo.nodeClassName,
                parentInnerNodeFieldDef,
                schemaFieldDef
            );

            addNodeChangeBridgeMethod(classDef, schemaClassInfo.changeClassName, changeMtd);
        }

        // Creates view and change methods for specific polymorphic instance schema.
        for (Field polymorphicField : polymorphicFields) {
            FieldDefinition polymorphicFieldDef = fieldDefs.get(fieldName(polymorphicField));

            addNodeViewMethod(classDef, polymorphicField, parentInnerNodeFieldDef, polymorphicFieldDef);

            MethodDefinition changeMtd = addNodeChangeMethod(
                classDef,
                polymorphicField,
                polymorphicExtensionClassInfo.nodeClassName,
                parentInnerNodeFieldDef,
                polymorphicFieldDef
            );

            addNodeChangeBridgeMethod(classDef, polymorphicExtensionClassInfo.changeClassName, changeMtd);
        }

        MethodDefinition convertMtd = classDef.declareMethod(
            of(PUBLIC),
            CONVERT_MTD_NAME,
            typeFromJavaClassName(schemaClassInfo.changeClassName),
            arg("changeClass", Class.class)
        );

        MethodDefinition parentConvertMtd = schemaInnerNodeClassDef.getMethods().stream()
            .filter(mtd -> CONVERT_MTD_NAME.equals(mtd.getName()))
            .findAny()
            .orElse(null);

        assert parentConvertMtd != null : schemaInnerNodeClassDef.getName();

        convertMtd.getBody()
            .append(getThisFieldCode(convertMtd, parentInnerNodeFieldDef))
            .append(convertMtd.getScope().getVariable("changeClass"))
            .invokeVirtual(parentConvertMtd)
            .retObject();

        return classDef;
    }

    /**
     * Create a {@code *CfgImpl} for the polymorphic configuration instance schema.
     *
     * @param schemaClass Polymorphic configuration schema (parent).
     * @param polymorphicExtension Polymorphic configuration instance schema (child).
     * @param schemaCfgImplClassDef {@link DynamicConfiguration} definition for the polymorphic configuration schema {@code schemaClass}.
     * @param schemaFields Schema fields of polymorphic configuration {@code schemaClass}.
     * @param polymorphicFields Schema fields of a polymorphic configuration instance {@code polymorphicExtension}.
     */
    private ClassDefinition createPolymorphicExtensionCfgImplClass(
        Class<?> schemaClass,
        Class<?> polymorphicExtension,
        ClassDefinition schemaCfgImplClassDef,
        Collection<Field> schemaFields,
        Collection<Field> polymorphicFields
    ) {
        SchemaClassesInfo schemaClassInfo = schemasInfo.get(schemaClass);
        SchemaClassesInfo polymorphicExtensionClassInfo = schemasInfo.get(polymorphicExtension);

        Class<?> superClass = schemaClassInfo.direct || polymorphicExtensionClassInfo.direct
            ? DirectConfigurationTreeWrapper.class : ConfigurationTreeWrapper.class;

        // Configuration impl class definition.
        ClassDefinition classDef = new ClassDefinition(
            of(PUBLIC, FINAL),
            internalName(polymorphicExtensionClassInfo.cfgImplClassName),
            type(superClass),
            configClassInterfaces(polymorphicExtension, Set.of())
        );

        // private final ParentCfgImpl parent#cfgImpl;
        FieldDefinition parentCfgImplFieldDef = classDef.declareField(
            of(PRIVATE, FINAL),
            "parent#cfgImpl",
            typeFromJavaClassName(schemaClassInfo.cfgImplClassName)
        );

        // Constructor.
        MethodDefinition constructorMtd = classDef.declareConstructor(
            of(PUBLIC),
            arg("parent", typeFromJavaClassName(schemaClassInfo.cfgImplClassName))
        );

        // Constructor body.
        // super(parent);
        // this.parent#cfgImpl = parent;
        constructorMtd.getBody()
            .append(constructorMtd.getThis())
            .append(constructorMtd.getScope().getVariable("parent"))
            .invokeConstructor(superClass, ConfigurationTree.class)
            .append(constructorMtd.getThis().setField(
                parentCfgImplFieldDef,
                constructorMtd.getScope().getVariable("parent")
            ))
            .ret();

        Map<String, FieldDefinition> fieldDefs = schemaCfgImplClassDef.getFields().stream()
            .collect(toMap(FieldDefinition::getName, identity()));

        for (Field schemaField : concat(schemaFields, polymorphicFields)) {
            // Must be skipped, this is an internal special field.
            if (isPolymorphicId(schemaField))
                continue;

            addConfigurationImplGetMethod(
                classDef,
                schemaField,
                parentCfgImplFieldDef,
                fieldDefs.get(fieldName(schemaField))
            );
        }

        return classDef;
    }

    /**
     * Adds a {@link InnerNode#specificView} override for the polymorphic configuration case.
     *
     * @param classDef Definition of a polymorphic configuration class {@code schemaClass}.
     * @param schemaClass Polymorphic configuration schema (parent).
     * @param polymorphicExtensions Polymorphic configuration instance schemas (children).
     * @param polymorphicTypeIdFieldDef Identification field for the polymorphic configuration instance.
     */
    private void addNodeSpecificViewMethod(
        ClassDefinition classDef,
        Class<?> schemaClass,
        Set<Class<?>> polymorphicExtensions,
        FieldDefinition polymorphicTypeIdFieldDef
    ) {
        MethodDefinition specificViewMtd = classDef.declareMethod(
            of(PUBLIC),
            SPECIFIC_VIEW_MTD_NAME,
            type(Object.class)
        );

        StringSwitchBuilder switchBuilder = typeIdSwitchBuilder(specificViewMtd, polymorphicTypeIdFieldDef);

        switchBuilder.addCase(
            schemaClass.getAnnotation(PolymorphicConfig.class).id(),
            specificViewMtd.getThis().ret()
        );

        for (Class<?> polymorphicExtension : polymorphicExtensions) {
            SchemaClassesInfo polymorphicExtensionClassInfo = schemasInfo.get(polymorphicExtension);

            switchBuilder.addCase(
                polymorphicExtension.getAnnotation(PolymorphicConfigInstance.class).id(),
                newInstance(
                    typeFromJavaClassName(polymorphicExtensionClassInfo.nodeClassName),
                    specificViewMtd.getThis()
                ).ret()
            );
        }

        specificViewMtd.getBody().append(switchBuilder.build()).ret();
    }

    /**
     * Adds a {@link InnerNode#specificChange} override for the polymorphic configuration case.
     *
     * @param classDef Definition of a polymorphic configuration class {@code schemaClass}.
     * @param schemaClass Polymorphic configuration schema (parent).
     * @param polymorphicExtensions Polymorphic configuration instance schemas (children).
     * @param polymorphicTypeIdFieldDef Identification field for the polymorphic configuration instance.
     */
    private void addNodeSpecificChangeMethod(
        ClassDefinition classDef,
        Class<?> schemaClass,
        Set<Class<?>> polymorphicExtensions,
        FieldDefinition polymorphicTypeIdFieldDef
    ) {
        MethodDefinition specificChangeMtd = classDef.declareMethod(
            of(PUBLIC),
            SPECIFIC_CHANGE_MTD_NAME,
            type(Object.class)
        );

        StringSwitchBuilder switchBuilder = typeIdSwitchBuilder(specificChangeMtd, polymorphicTypeIdFieldDef);

        switchBuilder.addCase(
            schemaClass.getAnnotation(PolymorphicConfig.class).id(),
            specificChangeMtd.getThis().ret()
        );

        for (Class<?> polymorphicExtension : polymorphicExtensions) {
            SchemaClassesInfo polymorphicExtensionClassInfo = schemasInfo.get(polymorphicExtension);

            switchBuilder.addCase(
                polymorphicExtension.getAnnotation(PolymorphicConfigInstance.class).id(),
                newInstance(
                    typeFromJavaClassName(polymorphicExtensionClassInfo.nodeClassName),
                    specificChangeMtd.getThis()
                ).ret()
            );
        }

        specificChangeMtd.getBody().append(switchBuilder.build()).ret();
    }

    /**
     * Adds a {@code *Node#convert} for the polymorphic configuration case.
     *
     * @param classDef Definition of a polymorphic configuration class {@code schemaClass}.
     * @param schemaClass Polymorphic configuration schema (parent).
     * @param polymorphicExtensions Polymorphic configuration instance schemas (children).
     * @param polymorphicTypeIdFieldDef Identification field for the polymorphic configuration instance.
     */
    private void addNodeConvertMethod(
        ClassDefinition classDef,
        Class<?> schemaClass,
        Set<Class<?>> polymorphicExtensions,
        FieldDefinition polymorphicTypeIdFieldDef
    ) {
        SchemaClassesInfo schemaClassInfo = schemasInfo.get(schemaClass);

        MethodDefinition convertMtd = classDef.declareMethod(
            of(PUBLIC),
            CONVERT_MTD_NAME,
            typeFromJavaClassName(schemaClassInfo.changeClassName),
            arg("changeClass", Class.class)
        );

        // changeClass.getName();
        BytecodeExpression changeClassName = convertMtd.getScope()
            .getVariable("changeClass")
            .invoke(CLASS_GET_NAME_MTD);

        StringSwitchBuilder switchBuilder = new StringSwitchBuilder(convertMtd.getScope())
            .expression(changeClassName)
            .defaultCase(throwException(NoSuchElementException.class, changeClassName));

        // TODO: IGNITE-14645 Continue.

        // this.typeId = "base"; -- deleted for now.
        // return this;
        switchBuilder.addCase(
            schemasInfo.get(schemaClass).changeClassName,
            new BytecodeBlock()
//                .append(setThisFieldCode(
//                    convertMtd,
//                    constantString(schemaClass.getAnnotation(PolymorphicConfig.class).id()),
//                    polymorphicTypeIdFieldDef
//                ))
                .append(convertMtd.getThis())
                .retObject()
        );

        for (Class<?> polymorphicExtension : polymorphicExtensions) {
            SchemaClassesInfo polymorphicExtensionClassInfo = schemasInfo.get(polymorphicExtension);

            // this.typeId = "some_name"; -- deleted for now.
            // return new ChildNode(this);
            switchBuilder.addCase(
                polymorphicExtensionClassInfo.changeClassName,
                new BytecodeBlock()
//                    .append(setThisFieldCode(
//                        convertMtd,
//                        constantString(polymorphicExtension.getAnnotation(PolymorphicConfigInstance.class).id()),
//                        polymorphicTypeIdFieldDef
//                    ))
                    .append(newInstance(
                        typeFromJavaClassName(polymorphicExtensionClassInfo.nodeClassName),
                        convertMtd.getThis()
                    ))
                    .retObject()
            );
        }

        convertMtd.getBody()
            .append(convertMtd.getThis())
            .append(switchBuilder.build())
            .ret();
    }

    /**
     * Adds a {@link DynamicConfiguration#specificConfigTree} override for the polymorphic configuration case.
     *
     * @param classDef Definition of a polymorphic configuration class {@code schemaClass}.
     * @param schemaClass Polymorphic configuration schema (parent).
     * @param polymorphicExtensions Polymorphic configuration instance schemas (children).
     * @param polymorphicTypeIdFieldDef Identification field for the polymorphic configuration instance.
     */
    private void addNodeSpecificConfigTreeMethod(
        ClassDefinition classDef,
        Class<?> schemaClass,
        Set<Class<?>> polymorphicExtensions,
        FieldDefinition polymorphicTypeIdFieldDef
    ) {
        SchemaClassesInfo schemaClassInfo = schemasInfo.get(schemaClass);

        MethodDefinition specificConfigMtd = classDef.declareMethod(
            of(PUBLIC),
            SPECIFIC_CONFIG_TREE_MTD_NAME,
            typeFromJavaClassName(schemaClassInfo.cfgClassName)
        );

        // String tmpStr;
        Variable tmpStrVar = specificConfigMtd.getScope().createTempVariable(String.class);

        // tmpStr = (String)this.typeId.value();
        BytecodeExpression setTmpVar = tmpStrVar.set(
            getThisFieldCode(specificConfigMtd, polymorphicTypeIdFieldDef)
                .invoke(DYNAMIC_PROPERTY_VALUE_MTD)
                .cast(String.class)
        );

        StringSwitchBuilder switchBuilder = new StringSwitchBuilder(specificConfigMtd.getScope())
            .expression(tmpStrVar)
            .defaultCase(throwException(NoSuchElementException.class, tmpStrVar));

        // return this;
        switchBuilder.addCase(
            schemaClass.getAnnotation(PolymorphicConfig.class).id(),
            specificConfigMtd.getThis().ret()
        );

        for (Class<?> polymorphicExtension : polymorphicExtensions) {
            // return new SpecialCfgImpl(this);
            switchBuilder.addCase(
                polymorphicExtension.getAnnotation(PolymorphicConfigInstance.class).id(),
                newInstance(
                    typeFromJavaClassName(schemasInfo.get(polymorphicExtension).cfgImplClassName),
                    specificConfigMtd.getThis()
                ).ret()
            );
        }

        specificConfigMtd.getBody()
            .append(setTmpVar)
            .append(switchBuilder.build())
            .ret();
    }

    /**
     * Adds field initialization with {@link NamedConfigValue} for the node constructor.
     *
     * @param ctor Node constructor definition.
     * @param schemaField Schema field with {@link NamedConfigValue}.
     * @param schemaFieldDef Schema field definition.
     */
    private void initNodeNamedConfigValue(
        MethodDefinition ctor,
        Field schemaField,
        FieldDefinition schemaFieldDef
    ) {
        assert isNamedConfigValue(schemaField);

        NamedConfigValue namedCfgAnnotation = schemaField.getAnnotation(NamedConfigValue.class);

        SchemaClassesInfo fieldClassNames = schemasInfo.get(schemaField.getType());

        // this.values = new NamedListNode<>(key, ValueNode::new);
        ctor.getBody().append(ctor.getThis().setField(
            schemaFieldDef,
            newInstance(
                NamedListNode.class,
                constantString(namedCfgAnnotation.syntheticKeyName()),
                newNamedListElementLambda(fieldClassNames.nodeClassName)
            )
        ));
    }

    /**
     * Creates bytecode block that invokes of construct methods for
     *      {@link InnerNode#construct(String, ConfigurationSource, boolean)}.
     *
     * @param constructMtd Method definition {@link InnerNode#construct(String, ConfigurationSource, boolean)}
     *      defined in {@code *Node} class.
     * @param schemaField Schema field.
     * @param schemaFieldDef Schema field definition.
     * @return Bytecode block that invokes of construct method for field.
     */
    private BytecodeBlock treatSourceForConstruct(
        MethodDefinition constructMtd,
        Field schemaField,
        FieldDefinition schemaFieldDef
    ) {
        BytecodeBlock codeBlock = new BytecodeBlock();

        Variable thisVar = constructMtd.getThis();
        Variable srcVar = constructMtd.getScope().getVariable("src");

        if (isValue(schemaField) || isPolymorphicId(schemaField)) {
            // this.field = src == null ? null : src.unwrap(FieldType.class);
            codeBlock.append(thisVar.setField(schemaFieldDef, inlineIf(
                isNull(srcVar),
                constantNull(schemaFieldDef.getType()),
                srcVar.invoke(UNWRAP, constantClass(schemaFieldDef.getType())).cast(schemaFieldDef.getType())
            )));
        }
        else if (isConfigValue(schemaField)) {
            // this.field = src == null ? null : src.descend(field = (field == null ? new FieldType() : field.copy()));
            codeBlock.append(new IfStatement()
                .condition(isNull(srcVar))
                .ifTrue(thisVar.setField(schemaFieldDef, constantNull(schemaFieldDef.getType())))
                .ifFalse(new BytecodeBlock()
                    .append(copyNodeField(constructMtd, schemaFieldDef))
                    .append(srcVar.invoke(DESCEND, thisVar.getField(schemaFieldDef)))
                )
            );
        }
        else {
            // this.field = src == null ? new NamedListNode<>(key, ValueNode::new) : src.descend(field = field.copy()));
            NamedConfigValue namedCfgAnnotation = schemaField.getAnnotation(NamedConfigValue.class);

            String fieldNodeClassName = schemasInfo.get(schemaField.getType()).nodeClassName;

            codeBlock.append(new IfStatement()
                .condition(isNull(srcVar))
                .ifTrue(thisVar.setField(
                    schemaFieldDef,
                    newInstance(
                        NamedListNode.class,
                        constantString(namedCfgAnnotation.syntheticKeyName()),
                        newNamedListElementLambda(fieldNodeClassName)
                    )
                ))
                .ifFalse(new BytecodeBlock()
                    .append(thisVar.setField(
                        schemaFieldDef,
                        thisVar.getField(schemaFieldDef).invoke(COPY).cast(schemaFieldDef.getType())
                    ))
                    .append(srcVar.invoke(DESCEND, thisVar.getField(schemaFieldDef)))
                )
            );
        }

        return codeBlock;
    }

    /**
     * Creates a bytecode block of code that sets the default value for a field from the schema for
     *      {@link InnerNode#constructDefault(String)}.
     *
     * @param constructDfltMtd Method definition {@link InnerNode#constructDefault(String)}
     *      defined in {@code *Node} class.
     * @param schemaField Schema field with {@link Value}.
     * @param schemaFieldDef Schema field definition.
     * @param specFieldDef Definition of the schema field.: {@code _spec#}.
     * @return Bytecode block that sets the default value for a field from the schema.
     */
    private static BytecodeBlock addNodeConstructDefault(
        MethodDefinition constructDfltMtd,
        Field schemaField,
        FieldDefinition schemaFieldDef,
        FieldDefinition specFieldDef
    ) {
        assert isValue(schemaField) : schemaField;

        Variable thisVar = constructDfltMtd.getThis();

        // defaultValue = _spec#.field;
        BytecodeExpression defaultValue = thisVar.getField(specFieldDef).getField(schemaField);

        Class<?> schemaFieldType = schemaField.getType();

        // defaultValue = Box.valueOf(defaultValue); // Boxing.
        if (schemaFieldType.isPrimitive()) {
            defaultValue = invokeStatic(
                schemaFieldDef.getType(),
                "valueOf",
                schemaFieldDef.getType(),
                singleton(defaultValue)
            );
        }

        // defaultValue = defaultValue.clone();
        if (schemaFieldType.isArray())
            defaultValue = defaultValue.invoke("clone", Object.class).cast(schemaFieldType);

        // this.field = defaultValue;
        return new BytecodeBlock().append(thisVar.setField(schemaFieldDef, defaultValue));
    }

    /**
     * Creates the bytecode:
     * <pre><code>
     * throw new Exception(msg);
     * </code></pre>
     *
     * @param throwableClass Exception class.
     * @param parameters Exception constructor parameters.
     * @return Exception throwing bytecode.
     */
    private static BytecodeBlock throwException(
        Class<? extends Throwable> throwableClass,
        BytecodeExpression... parameters
    ) {
        return new BytecodeBlock().append(newInstance(throwableClass, parameters)).throwObject();
    }

    /**
     * Creates the bytecode:
     * <pre><code>
     * Object.getClass();
     * </code></pre>
     *
     * @param expression Bytecode for working with the {@link Class}.
     * @return {@link Object#getClass} bytecode.
     */
    private static BytecodeExpression invokeGetClass(BytecodeExpression expression) {
        return expression.invoke("getClass", Class.class);
    }

    /**
     * Returns the name of the configuration schema field.
     * If the schema contains {@link PolymorphicConfigInstance},
     * it will return "{@link Field#getName()} + {@code "#"} + {@link PolymorphicConfigInstance#id()}"
     * otherwise "{@link Field#getName}".
     *
     * @param f Configuration schema field.
     * @return Field name.
     */
    private static String fieldName(Field f) {
        if (f.getDeclaringClass().isAnnotationPresent(PolymorphicConfigInstance.class))
            return f.getName() + "#" + f.getDeclaringClass().getAnnotation(PolymorphicConfigInstance.class).id();
        else
            return f.getName();
    }

    /**
     * Creates a string switch builder by {@code typeIdFieldDef}.
     *
     * @param mtdDef Method definition.
     * @param typeIdFieldDef Field definition that contains the id of the polymorphic configuration instance.
     * @return String switch builder by {@code typeIdFieldDef}.
     */
    private static StringSwitchBuilder typeIdSwitchBuilder(MethodDefinition mtdDef, FieldDefinition typeIdFieldDef) {
        BytecodeExpression typeIdVar = mtdDef.getThis().getField(typeIdFieldDef);

        return new StringSwitchBuilder(mtdDef.getScope())
            .expression(typeIdVar)
            .defaultCase(throwException(NoSuchElementException.class, typeIdVar));
    }

    /**
     * Generates bytecode to get a class field like {@code this.field;} or {@code this.field.field;}.
     *
     * @param mtdDef Class method definition.
     * @param fieldDefs Field definitions.
     * @return Bytecode for getting the field.
     */
    private static BytecodeExpression getThisFieldCode(MethodDefinition mtdDef, FieldDefinition... fieldDefs) {
        assert !nullOrEmpty(fieldDefs);

        // this.field;
        BytecodeExpression getFieldCode = mtdDef.getThis().getField(fieldDefs[0]);

        // this.field.field; etc.
        for (int i = 1; i < fieldDefs.length; i++)
            getFieldCode = getFieldCode.getField(fieldDefs[i]);

        return getFieldCode;
    }

    /**
     * Generates bytecode to set a class field like {@code this.field = value;} or {@code this.field.field = value;}.
     *
     * @param mtdDef Class method definition.
     * @param value Value of the field to be set.
     * @param fieldDefs Field definitions.
     * @return Bytecode for setting the field value.
     */
    private static BytecodeExpression setThisFieldCode(
        MethodDefinition mtdDef,
        BytecodeExpression value,
        FieldDefinition... fieldDefs
    ) {
        assert !nullOrEmpty(fieldDefs);

        if (fieldDefs.length == 1) {
            // this.field = value;
            return mtdDef.getThis().setField(fieldDefs[0], value);
        }
        else {
            // this.field;
            BytecodeExpression getFieldCode = mtdDef.getThis().getField(fieldDefs[0]);

            // this.field.field; etc.
            for (int i = 1; i < fieldDefs.length - 1; i++)
                getFieldCode = getFieldCode.getField(fieldDefs[i]);

            // this.field.field = value;
            return getFieldCode.setField(fieldDefs[fieldDefs.length - 1], value);
        }
    }
}
