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
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.configuration.asm.SchemaClassesInfo.changeClassName;
import static org.apache.ignite.internal.configuration.asm.SchemaClassesInfo.configurationClassName;
import static org.apache.ignite.internal.configuration.asm.SchemaClassesInfo.viewClassName;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.extensionsFields;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.isConfigValue;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.isNamedConfigValue;
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
        }
        catch (NoSuchMethodException nsme) {
            throw new ExceptionInInitializerError(nsme);
        }
    }

    /** Information about schema classes - bunch of names and dynamically compiled internal classes. */
    public final Map<Class<?>, SchemaClassesInfo> schemasInfo = new HashMap<>();
    // TODO: IGNITE-14645 ^^^

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
        List<ClassDefinition> definitions = new ArrayList<>();

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

            schemas.add(schemaClass);

            definitions.addAll(createNodeClass(
                schemaClass,
                internalExtensions,
                polymorphicExtensions,
                schemaFields,
                internalExtensionsFields,
                polymorphicExtensionsFields
            ));

            definitions.add(createCfgImplClass(
                schemaClass,
                internalExtensions,
                polymorphicExtensions,
                schemaFields,
                internalExtensionsFields,
                polymorphicExtensionsFields
            ));
        }

        Map<String, Class<?>> definedClasses = generator.defineClasses(definitions);

        for (Class<?> schemaClass : schemas) {
            SchemaClassesInfo info = schemasInfo.get(schemaClass);

            info.nodeClass = (Class<? extends InnerNode>)definedClasses.get(info.nodeClassName);
            info.cfgImplClass = (Class<? extends DynamicConfiguration<?, ?>>)definedClasses.get(info.cfgImplClassName);

            for (Class<?> aClass : polymorphicSchemaExtensions.getOrDefault(schemaClass, Set.of())) {
                SchemaClassesInfo schemaClassesInfo = new SchemaClassesInfo(aClass);

                schemaClassesInfo.nodeClass = (Class<? extends InnerNode>)definedClasses.get(schemaClassesInfo.nodeClassName);
                schemaClassesInfo.cfgImplClass = (Class<? extends DynamicConfiguration<?, ?>>)definedClasses.get(schemaClassesInfo.cfgImplClassName);

                schemasInfo.put(aClass, schemaClassesInfo);
            }
            // TODO: IGNITE-14645 ^^^
        }
    }

    /**
     * Construct a {@code *Node} definitions for a configuration schema.
     * Creates {@link InnerNode} for {@code schemaClass} and {@code *Node}'s for {@code polymorphicExtensions}.
     *
     * @param schemaClass           Configuration schema class.
     * @param internalExtensions    Internal extensions of the configuration schema.
     * @param polymorphicExtensions Polymorphic extensions of the configuration schema.
     * @param schemaFields          Fields of the schema class.
     * @param internalFields        Fields of internal extensions of the configuration schema.
     * @param polymorphicFields     Fields of polymorphic extensions of the configuration schema.
     * @return Constructed {@code *Node} definitions for the configuration schema.
     */
    private Collection<ClassDefinition> createNodeClass(
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

        // To store the id of the polymorphic configuration instance.
        FieldDefinition polymorphicTypeIdFieldDef = null;

        // TODO: IGNITE-14645 Maybe change.

        if (schemaClass.isAnnotationPresent(PolymorphicConfig.class)) {
            polymorphicTypeIdFieldDef = classDef.declareField(
                of(PRIVATE),
                schemaClass.getAnnotation(PolymorphicConfig.class).fieldName(),
                String.class
            );
        }

        // org.apache.ignite.internal.configuration.tree.InnerNode#schemaType
        addNodeSchemaTypeMethod(classDef, schemaClass, polymorphicExtensions, specFields, polymorphicTypeIdFieldDef);

        // Define the rest of the fields.
        Map<String, FieldDefinition> fieldDefs = new HashMap<>();

        for (Field schemaField : concat(schemaFields, internalFields))
            fieldDefs.put(schemaField.getName(), addNodeField(classDef, schemaField, null));

        for (Field polymorphicField : polymorphicFields) {
            String fieldName = polymorphicFieldName(polymorphicField, specFields);

            fieldDefs.put(fieldName, addNodeField(classDef, polymorphicField, fieldName));
        }

        // Constructor.
        addNodeConstructor(classDef, specFields, fieldDefs, schemaFields, internalFields, polymorphicFields);

        // VIEW and CHANGE methods.
        for (Field schemaField : concat(schemaFields, internalFields)) {
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
            specFields,
            fieldDefs,
            schemaFields,
            internalFields,
            polymorphicFields,
            polymorphicTypeIdFieldDef
        );

        // traverseChild
        addNodeTraverseChildMethod(
            classDef,
            specFields,
            fieldDefs,
            schemaFields,
            internalFields,
            polymorphicFields,
            polymorphicTypeIdFieldDef
        );

        // construct
        addNodeConstructMethod(
            classDef,
            specFields,
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

        Collection<ClassDefinition> classDefs;

        if (polymorphicExtensions.isEmpty())
            classDefs = List.of(classDef);
        else {
            classDefs = new ArrayList<>(polymorphicExtensions.size() + 1);
            classDefs.add(classDef);

            for (Class<?> polymorphicExtension : polymorphicExtensions) {
                if (schemasInfo.containsKey(polymorphicExtension))
                    continue;

                schemasInfo.put(polymorphicExtension, new SchemaClassesInfo(polymorphicExtension));

                // Only the fields of a specific instance of a polymorphic configuration.
                Collection<Field> polymorphicExtensionFields = polymorphicFields.stream()
                    .filter(f -> f.getDeclaringClass() == polymorphicExtension)
                    .collect(toList());

                classDefs.add(createPolymorphicExtensionNodeClass(
                    schemaClass,
                    polymorphicExtension,
                    specFields,
                    fieldDefs,
                    schemaFields,
                    polymorphicExtensionFields
                ));
            }

            addNodeSpecificViewMethod(classDef, schemaClass, polymorphicExtensions, polymorphicTypeIdFieldDef);
        }

        return classDefs;
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
     *         {@code private BoxedType fieldName}
     *     </li>
     *     <li>
     *         {@code @ConfigValue public MyConfigurationSchema fieldName}<br/>becomes<br/>
     *         {@code private MyNode fieldName}
     *     </li>
     *     <li>
     *         {@code @NamedConfigValue public type fieldName}<br/>becomes<br/>
     *         {@code private NamedListNode fieldName}
     *     </li>
     * </ul>
     *
     * @param classDef    Node class definition.
     * @param schemaField Configuration Schema class field.
     * @param fieldName   Field name, if {@code null} then {@link Field#getName} will be used.
     * @return Declared field definition.
     */
    private FieldDefinition addNodeField(
        ClassDefinition classDef,
        Field schemaField,
        @Nullable String fieldName
    ) {
        Class<?> schemaFieldClass = schemaField.getType();

        ParameterizedType nodeFieldType;

        if (isValue(schemaField))
            nodeFieldType = type(box(schemaFieldClass));
        else if (isConfigValue(schemaField))
            nodeFieldType = typeFromJavaClassName(schemasInfo.get(schemaFieldClass).nodeClassName);
        else
            nodeFieldType = type(NamedListNode.class);

        if (fieldName == null)
            fieldName = schemaField.getName();

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

        for (Field schemaField : concat(schemaFields, internalFields)) {
            if (!isNamedConfigValue(schemaField))
                continue;

            initNodeNamedConfigValue(ctor, schemaField, fieldDefs.get(schemaField.getName()));
        }

        for (Field polymorphicField : polymorphicFields) {
            if (!isNamedConfigValue(polymorphicField))
                continue;

            String fieldName = polymorphicFieldName(polymorphicField, specFields);

            initNodeNamedConfigValue(ctor, polymorphicField, fieldDefs.get(fieldName));
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
                "specificView",
                typeFromJavaClassName(schemaClassInfo.viewClassName)
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
     * @param specFields                Field definitions for the schema and its extensions: {@code _spec#}.
     * @param fieldDefs                 Definitions for all fields in {@code schemaFields}.
     * @param schemaFields              Fields of the schema class.
     * @param internalFields            Fields of internal extensions of the configuration schema.
     * @param polymorphicFields         Fields of polymorphic extensions of the configuration schema.
     * @param polymorphicTypeIdFieldDef Identification field for the polymorphic configuration instance.
     */
    private static void addNodeTraverseChildrenMethod(
        ClassDefinition classDef,
        Class<?> schemaClass,
        Map<Class<?>, FieldDefinition> specFields,
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

        if (polymorphicTypeIdFieldDef != null) {
            // visitor.visitLeafNode("typeId", this.typeId);
            BytecodeBlock invokeVisitTypeId = invokeVisit(
                traverseChildrenMtd,
                polymorphicTypeIdFieldDef.getName(),
                VISIT_LEAF,
                polymorphicTypeIdFieldDef
            );

            mtdBody.append(invokeVisitTypeId.pop());
        }

        for (Field schemaField : schemaFields) {
            String fieldName = schemaField.getName();

            mtdBody.append(
                invokeVisit(traverseChildrenMtd, schemaField, fieldDefs.get(fieldName)).pop()
            );
        }

        if (!internalFields.isEmpty()) {
            BytecodeBlock includeInternalBlock = new BytecodeBlock();

            for (Field internalField : internalFields) {
                String fieldName = internalField.getName();

                includeInternalBlock.append(
                    invokeVisit(traverseChildrenMtd, internalField, fieldDefs.get(fieldName)).pop()
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
                    String fieldName = polymorphicFieldName(polymorphicField, specFields);

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
     * @param specFields                Field definitions for the schema and its extensions: {@code _spec#}.
     * @param fieldDefs                 Definitions for all fields in {@code schemaFields}.
     * @param schemaFields              Fields of the schema class.
     * @param internalFields            Fields of internal extensions of the configuration schema.
     * @param polymorphicFields         Fields of polymorphic extensions of the configuration schema.
     * @param polymorphicTypeIdFieldDef Identification field for the polymorphic configuration instance.
     */
    private static void addNodeTraverseChildMethod(
        ClassDefinition classDef,
        Map<Class<?>, FieldDefinition> specFields,
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
            // visitor.visitLeafNode("typeId", this.typeId);
            BytecodeBlock invokeVisitTypeId = invokeVisit(
                traverseChildMtd,
                polymorphicTypeIdFieldDef.getName(),
                VISIT_LEAF,
                polymorphicTypeIdFieldDef
            );

            switchBuilder.addCase(
                polymorphicTypeIdFieldDef.getName(),
                invokeVisitTypeId.retObject()
            );

            Map<String, List<Field>> groupByName = polymorphicFields.stream().collect(groupingBy(Field::getName));

            for (Map.Entry<String, List<Field>> e : groupByName.entrySet()) {
                StringSwitchBuilder switchBuilderTypeId = typeIdSwitchBuilder(traverseChildMtd, polymorphicTypeIdFieldDef);

                for (Field polymorphicField : e.getValue()) {
                    String fieldName = polymorphicFieldName(polymorphicField, specFields);

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

        if (isValue(schemaField))
            visitMethod = VISIT_LEAF;
        else if (isConfigValue(schemaField))
            visitMethod = VISIT_INNER;
        else
            visitMethod = VISIT_NAMED;

        return invokeVisit(mtd, schemaField.getName(), visitMethod, fieldDef);
    }

    /**
     * Implements {@link ConstructableTreeNode#construct(String, ConfigurationSource, boolean)} method.
     *
     * @param classDef                  Class definition.
     * @param specFields                Field definitions for the schema and its extensions: {@code _spec#}.
     * @param fieldDefs                 Definitions for all fields in {@code schemaFields}.
     * @param schemaFields              Fields of the schema class.
     * @param internalFields            Fields of internal extensions of the configuration schema.
     * @param polymorphicFields         Fields of polymorphic extensions of the configuration schema.
     * @param polymorphicTypeIdFieldDef Identification field for the polymorphic configuration instance.
     */
    private void addNodeConstructMethod(
        ClassDefinition classDef,
        Map<Class<?>, FieldDefinition> specFields,
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
                    String fieldName = polymorphicFieldName(polymorphicField, specFields);

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
                    String fieldName = polymorphicFieldName(polymorphicField, specFields);
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
     * @param schemaClass              Configuration schema class.
     * @param internalExtensions Internal extensions of the configuration schema.
     * @param schemaFields             Fields of the schema class.
     * @param internalFields Fields of internal extensions of the configuration schema.
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

        for (Field schemaField : concat(schemaFields, internalFields))
            fieldDefs.put(schemaField.getName(), addConfigurationImplField(classDef, schemaField));

        // Constructor
        addConfigurationImplConstructor(classDef, schemaClassInfo, fieldDefs, schemaFields, internalFields);

        for (Field schemaField : concat(schemaFields, internalFields))
            addConfigurationImplGetMethod(classDef, schemaClass, fieldDefs, schemaField);

        // org.apache.ignite.internal.configuration.DynamicConfiguration#configType
        addCfgImplConfigTypeMethod(classDef, typeFromJavaClassName(schemaClassInfo.cfgClassName));

        return classDef;
    }

    /**
     * Declares field that corresponds to configuration value. Depending on the schema, 3 options possible:
     * <ul>
     *     <li>
     *         {@code @Value public type fieldName}<br/>becomes<br/>
     *         {@code private DynamicProperty fieldName}
     *     </li>
     *     <li>
     *         {@code @ConfigValue public MyConfigurationSchema fieldName}<br/>becomes<br/>
     *         {@code private MyConfiguration fieldName}
     *     </li>
     *     <li>
     *         {@code @NamedConfigValue public type fieldName}<br/>becomes<br/>
     *         {@code private NamedListConfiguration fieldName}
     *     </li>
     * </ul>
     *
     * @param classDef    Configuration impl class definition.
     * @param schemaField Configuration Schema class field.
     * @return Declared field definition.
     */
    private FieldDefinition addConfigurationImplField(ClassDefinition classDef, Field schemaField) {
        ParameterizedType fieldType;

        if (isConfigValue(schemaField))
            fieldType = typeFromJavaClassName(schemasInfo.get(schemaField.getType()).cfgClassName);
        else if (isNamedConfigValue(schemaField))
            fieldType = type(NamedListConfiguration.class);
        else
            fieldType = type(DynamicProperty.class);

        return classDef.declareField(of(PRIVATE), schemaField.getName(), fieldType);
    }

    /**
     * Implements default constructor for the configuration class. It initializes all fields and adds them to members
     * collection.
     *
     * @param classDef         Configuration impl class definition.
     * @param schemaClassInfo  Configuration Schema class info.
     * @param fieldDefs        Field definitions for all fields of configuration impl class.
     * @param schemaFields     Fields of the schema class.
     * @param extensionsFields Fields of internal extensions of the configuration schema.
     */
    private void addConfigurationImplConstructor(
        ClassDefinition classDef,
        SchemaClassesInfo schemaClassInfo,
        Map<String, FieldDefinition> fieldDefs,
        Collection<Field> schemaFields,
        Collection<Field> extensionsFields
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
        for (Field schemaField : concat(schemaFields, extensionsFields)) {
            FieldDefinition fieldDef = fieldDefs.get(schemaField.getName());

            BytecodeExpression newValue;

            if (isValue(schemaField)) {
                Class<?> fieldImplClass = schemaField.isAnnotationPresent(DirectAccess.class) ?
                    DirectDynamicProperty.class : DynamicProperty.class;

                // newValue = new DynamicProperty(super.keys, fieldName, rootKey, changer, listenOnly);
                newValue = newInstance(
                    fieldImplClass,
                    thisKeysVar,
                    constantString(schemaField.getName()),
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
                        constantString(schemaField.getName()),
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
                        constantString(schemaField.getName()),
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
     * @param schemaClass Configuration Schema class.
     * @param fieldDefs   Field definitions for all fields of configuration impl class.
     * @param schemaField Configuration Schema class field.
     */
    private void addConfigurationImplGetMethod(
        ClassDefinition classDef,
        Class<?> schemaClass,
        Map<String, FieldDefinition> fieldDefs,
        Field schemaField
    ) {
        Class<?> schemaFieldType = schemaField.getType();

        String fieldName = schemaField.getName();
        FieldDefinition fieldDef = fieldDefs.get(fieldName);

        ParameterizedType returnType;

        if (isConfigValue(schemaField))
            returnType = typeFromJavaClassName(schemasInfo.get(schemaFieldType).cfgClassName);
        else if (isNamedConfigValue(schemaField))
            returnType = type(NamedConfigurationTree.class);
        else {
            assert isValue(schemaField) : schemaClass;

            returnType = type(ConfigurationValue.class);
        }

        MethodDefinition viewMtd = classDef.declareMethod(
            of(PUBLIC),
            fieldName,
            returnType
        );

        BytecodeBlock viewBody = viewMtd.getBody();

        viewBody
            .append(viewMtd.getThis())
            .getField(fieldDef)
            .retObject();
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
     * @param specFields Field definitions {@code _spec#} in a {@link InnerNode} of polymorphic configuration
     *      (union {@code schemaClass} and {@code polymorphicExtension}).
     * @param fieldDefs Field definitions in a {@link InnerNode} of polymorphic configuration
     *      (union {@code schemaFields} and {@code polymorphicFields}).
     * @param schemaFields Schema fields of polymorphic configuration {@code schemaClass}.
     * @param polymorphicFields Schema fields of a polymorphic configuration instance {@code polymorphicExtension}.
     */
    private ClassDefinition createPolymorphicExtensionNodeClass(
        Class<?> schemaClass,
        Class<?> polymorphicExtension,
        Map<Class<?>, FieldDefinition> specFields,
        Map<String, FieldDefinition> fieldDefs,
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

        // Creates view and change methods for parent schema.
        for (Field schemaField : schemaFields) {
            FieldDefinition schemaFieldDef = fieldDefs.get(schemaField.getName());

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
            String fieldName = polymorphicFieldName(polymorphicField, specFields);

            FieldDefinition polymorphicFieldDef = fieldDefs.get(fieldName);

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
        SchemaClassesInfo schemaClassInfo = schemasInfo.get(schemaClass);

        MethodDefinition specificViewMtd = classDef.declareMethod(
            of(PUBLIC),
            "specificView",
            typeFromJavaClassName(schemaClassInfo.viewClassName)
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
                    typeFromJavaClassName(polymorphicExtensionClassInfo.viewClassName),
                    specificViewMtd.getThis()
                ).ret()
            );
        }

        specificViewMtd.getBody().append(switchBuilder.build()).ret();
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

        // this.field = src == null ? null : src.unwrap(FieldType.class);
        if (isValue(schemaField)) {
            codeBlock.append(thisVar.setField(schemaFieldDef, inlineIf(
                isNull(srcVar),
                constantNull(schemaFieldDef.getType()),
                srcVar.invoke(UNWRAP, constantClass(schemaFieldDef.getType())).cast(schemaFieldDef.getType())
            )));
        }
        // this.field = src == null ? null : src.descend(field = (field == null ? new FieldType() : field.copy()));
        else if (isConfigValue(schemaField)) {
            codeBlock.append(new IfStatement()
                .condition(isNull(srcVar))
                .ifTrue(thisVar.setField(schemaFieldDef, constantNull(schemaFieldDef.getType())))
                .ifFalse(new BytecodeBlock()
                    .append(copyNodeField(constructMtd, schemaFieldDef))
                    .append(srcVar.invoke(DESCEND, thisVar.getField(schemaFieldDef)))
                )
            );
        }
        // this.field = src == null ? new NamedListNode<>(key, ValueNode::new) : src.descend(field = field.copy()));
        else {
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

        if (!schemaField.getAnnotation(Value.class).hasDefault())
            return new BytecodeBlock();

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
     * Returns the field name of the polymorphic configuration schema instance.
     * For example: {@code port#_spec1}.
     *
     * @param polymorphicField Polymorphic configuration schema instance field.
     * @param specFields Field definitions for the schema and its extensions: {@code _spec#}.
     * @return Field name.
     */
    private static String polymorphicFieldName(Field polymorphicField, Map<Class<?>, FieldDefinition> specFields) {
        return polymorphicField.getName() + "#" + specFields.get(polymorphicField.getDeclaringClass()).getName();
    }

    /**
     * Creates bytecode block that invokes one of {@link ConfigurationVisitor}'s methods.
     *
     * @param mtd Method definition, either {@link InnerNode#traverseChildren(ConfigurationVisitor, boolean)} or
     *      {@link InnerNode#traverseChild(String, ConfigurationVisitor, boolean)} defined in {@code *Node} class.
     * @param fieldName Field name.
     * @param visitMethod Visit method.
     * @param fieldDef Field definition from current class.
     * @return Bytecode block that invokes "visit*" method.
     */
    private static BytecodeBlock invokeVisit(
        MethodDefinition mtd,
        String fieldName,
        Method visitMethod,
        FieldDefinition fieldDef
    ) {
        return new BytecodeBlock().append(mtd.getScope().getVariable("visitor").invoke(
            visitMethod,
            constantString(fieldName),
            mtd.getThis().getField(fieldDef)
        ));
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
