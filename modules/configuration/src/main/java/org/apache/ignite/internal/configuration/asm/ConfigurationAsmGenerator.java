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
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantInt;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantNull;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantString;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.inlineIf;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.invokeDynamic;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.isNull;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.newArray;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.newInstance;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.set;
import static java.lang.invoke.MethodType.methodType;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.EnumSet.of;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.internal.configuration.asm.DirectProxyAsmGenerator.newDirectProxyLambda;
import static org.apache.ignite.internal.configuration.asm.SchemaClassesInfo.changeClassName;
import static org.apache.ignite.internal.configuration.asm.SchemaClassesInfo.configurationClassName;
import static org.apache.ignite.internal.configuration.asm.SchemaClassesInfo.nodeClassName;
import static org.apache.ignite.internal.configuration.asm.SchemaClassesInfo.viewClassName;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.extensionsFields;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.isConfigValue;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.isInjectedName;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.isInternalId;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.isNamedConfigValue;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.isPolymorphicConfig;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.isPolymorphicConfigInstance;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.isPolymorphicId;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.isValue;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.polymorphicInstanceId;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.schemaFields;
import static org.apache.ignite.internal.util.ArrayUtils.nullOrEmpty;
import static org.apache.ignite.internal.util.CollectionUtils.concat;
import static org.objectweb.asm.Opcodes.H_NEWINVOKESPECIAL;
import static org.objectweb.asm.Type.getMethodDescriptor;
import static org.objectweb.asm.Type.getMethodType;
import static org.objectweb.asm.Type.getType;

import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.bytecode.ClassGenerator;
import com.facebook.presto.bytecode.FieldDefinition;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.ParameterizedType;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.expression.BytecodeExpression;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.ignite.configuration.ConfigurationProperty;
import org.apache.ignite.configuration.ConfigurationTree;
import org.apache.ignite.configuration.ConfigurationValue;
import org.apache.ignite.configuration.ConfigurationWrongPolymorphicTypeIdException;
import org.apache.ignite.configuration.NamedConfigurationTree;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.annotation.AbstractConfiguration;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.InternalConfiguration;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.PolymorphicConfig;
import org.apache.ignite.configuration.annotation.PolymorphicConfigInstance;
import org.apache.ignite.configuration.annotation.PolymorphicId;
import org.apache.ignite.internal.configuration.ConfigurationNode;
import org.apache.ignite.internal.configuration.ConfigurationTreeWrapper;
import org.apache.ignite.internal.configuration.DynamicConfiguration;
import org.apache.ignite.internal.configuration.DynamicConfigurationChanger;
import org.apache.ignite.internal.configuration.DynamicProperty;
import org.apache.ignite.internal.configuration.NamedListConfiguration;
import org.apache.ignite.internal.configuration.TypeUtils;
import org.apache.ignite.internal.configuration.direct.DirectPropertyProxy;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.apache.ignite.internal.configuration.tree.NamedListNode;
import org.apache.ignite.internal.configuration.util.ConfigurationUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

/**
 * This class is responsible for generating internal implementation classes for configuration schemas. It uses classes from {@code bytecode}
 * module to achieve this goal, like {@link ClassGenerator}, for examples.
 */
// TODO: IGNITE-17167 Split into classes/methods for regular/internal/polymorphic/abstract configuration
public class ConfigurationAsmGenerator extends Methods {
    /** Information about schema classes - bunch of names and dynamically compiled internal classes. */
    private final Map<Class<?>, SchemaClassesInfo> schemasInfo = new HashMap<>();

    /** Class generator instance. */
    private final ClassGenerator generator = ClassGenerator.classGenerator(getClass().getClassLoader());

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
        } catch (Exception e) {
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
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Generates, defines, loads and initializes all dynamic classes required for the given configuration schema.
     *
     * @param rootSchemaClass             Class of the root configuration schema.
     * @param internalSchemaExtensions    Internal extensions ({@link InternalConfiguration}) of configuration schemas ({@link
     *                                    ConfigurationRoot} and {@link Config}). Mapping: original schema -> extensions.
     * @param polymorphicSchemaExtensions Polymorphic extensions ({@link PolymorphicConfigInstance}) of configuration schemas ({@link
     *                                    PolymorphicConfig}). Mapping: original schema -> extensions.
     */
    public synchronized void compileRootSchema(
            Class<?> rootSchemaClass,
            Map<Class<?>, Set<Class<?>>> internalSchemaExtensions,
            Map<Class<?>, Set<Class<?>>> polymorphicSchemaExtensions
    ) {
        if (schemasInfo.containsKey(rootSchemaClass)) {
            return; // Already compiled.
        }

        Queue<Class<?>> compileQueue = new ArrayDeque<>();
        compileQueue.add(rootSchemaClass);

        schemasInfo.put(rootSchemaClass, new SchemaClassesInfo(rootSchemaClass));

        Set<Class<?>> schemas = new HashSet<>();
        List<ClassDefinition> classDefs = new ArrayList<>();

        while (!compileQueue.isEmpty()) {
            Class<?> schemaClass = compileQueue.poll();

            assert schemaClass.isAnnotationPresent(ConfigurationRoot.class)
                    || schemaClass.isAnnotationPresent(Config.class)
                    || isPolymorphicConfig(schemaClass)
                    : schemaClass + " is not properly annotated";

            assert schemasInfo.containsKey(schemaClass) : schemaClass;

            Set<Class<?>> internalExtensions = internalSchemaExtensions.getOrDefault(schemaClass, Set.of());
            Set<Class<?>> polymorphicExtensions = polymorphicSchemaExtensions.getOrDefault(schemaClass, Set.of());

            assert internalExtensions.isEmpty() || polymorphicExtensions.isEmpty() :
                    "Internal and polymorphic extensions are not allowed at the same time: " + schemaClass;

            if (isPolymorphicConfig(schemaClass) && polymorphicExtensions.isEmpty()) {
                throw new IllegalArgumentException(schemaClass
                        + " is polymorphic but polymorphic extensions are absent");
            }

            Class<?> schemaSuperClass = schemaClass.getSuperclass();

            List<Field> schemaFields = schemaSuperClass.isAnnotationPresent(AbstractConfiguration.class)
                    ? concat(schemaFields(schemaClass), schemaFields(schemaSuperClass))
                    : schemaFields(schemaClass);

            Collection<Field> internalExtensionsFields = extensionsFields(internalExtensions, true);
            Collection<Field> polymorphicExtensionsFields = extensionsFields(polymorphicExtensions, false);

            Field internalIdField = internalIdField(schemaClass, internalExtensions);

            for (Field schemaField : concat(schemaFields, internalExtensionsFields, polymorphicExtensionsFields)) {
                if (isConfigValue(schemaField) || isNamedConfigValue(schemaField)) {
                    Class<?> subSchemaClass = schemaField.getType();

                    if (!schemasInfo.containsKey(subSchemaClass)) {
                        compileQueue.offer(subSchemaClass);

                        schemasInfo.put(subSchemaClass, new SchemaClassesInfo(subSchemaClass));
                    }
                }
            }

            for (Class<?> polymorphicExtension : polymorphicExtensions) {
                schemasInfo.put(polymorphicExtension, new SchemaClassesInfo(polymorphicExtension));
            }

            schemas.add(schemaClass);

            classDefs.addAll(new InnerNodeAsmGenerator(
                    this,
                    schemaClass,
                    internalExtensions,
                    polymorphicExtensions,
                    schemaFields,
                    internalExtensionsFields,
                    polymorphicExtensionsFields,
                    internalIdField
            ).generate());

            ClassDefinition cfgImplClassDef = createCfgImplClass(
                    schemaClass,
                    internalExtensions,
                    polymorphicExtensions,
                    schemaFields,
                    internalExtensionsFields,
                    polymorphicExtensionsFields,
                    internalIdField
            );

            classDefs.add(cfgImplClassDef);

            for (Class<?> polymorphicExtension : polymorphicExtensions) {
                // Only the fields of a specific instance of a polymorphic configuration.
                Collection<Field> polymorphicFields = polymorphicExtensionsFields.stream()
                        .filter(f -> f.getDeclaringClass() == polymorphicExtension)
                        .collect(toList());

                classDefs.add(createPolymorphicExtensionCfgImplClass(
                        schemaClass,
                        polymorphicExtension,
                        cfgImplClassDef,
                        schemaFields,
                        polymorphicFields,
                        internalIdField
                ));
            }

            ClassDefinition directProxyClassDef = new DirectProxyAsmGenerator(
                    this,
                    schemaClass,
                    internalExtensions,
                    schemaFields,
                    internalExtensionsFields,
                    internalIdField
            ).generate();

            classDefs.add(directProxyClassDef);
        }

        Map<String, Class<?>> definedClasses = generator.defineClasses(classDefs);

        for (Class<?> schemaClass : schemas) {
            SchemaClassesInfo info = schemasInfo.get(schemaClass);

            info.nodeClass = (Class<? extends InnerNode>) definedClasses.get(info.nodeClassName);
            info.cfgImplClass = (Class<? extends DynamicConfiguration<?, ?>>) definedClasses.get(info.cfgImplClassName);
        }
    }

    /**
     * Returns info about schema class.
     *
     * @param schemaClass Schema class.
     * @return Schema class info.
     * @see SchemaClassesInfo
     */
    synchronized SchemaClassesInfo schemaInfo(Class<?> schemaClass) {
        return schemasInfo.get(schemaClass);
    }

    /*
     * Returns field, annotated with {@link InternalId}, if it exists. This field may only be present in {@link ConfigurationRoot},
     * {@link Config}, {@link InternalConfiguration} or {@link PolymorphicConfig}.
     *
     * @param schemaClass Base schema class.
     * @param schemaExtensions Extensions for the base schema.
     * @return Internal id field or {@code null} if it's not found.
     */
    @Nullable
    private Field internalIdField(Class<?> schemaClass, Set<Class<?>> schemaExtensions) {
        List<Field> internalIdFields = Stream.concat(Stream.of(schemaClass), schemaExtensions.stream())
                .map(Class::getDeclaredFields)
                .flatMap(Arrays::stream)
                .filter(ConfigurationUtil::isInternalId)
                .collect(toList());

        if (internalIdFields.isEmpty()) {
            return null;
        }

        assert internalIdFields.size() == 1 : internalIdFields;

        return internalIdFields.get(0);
    }

    /**
     * Copies field into itself or instantiates it if the field is null. Code like: {@code this.field == null ? new ValueNode() :
     * (ValueNode)this.field.copy();}.
     *
     * @param schemaField  Configuration schema class field.
     * @param getFieldCode Bytecode of getting the field, for example: {@code this.field} or {@code this.field.field};
     * @return Bytecode expression.
     */
    BytecodeExpression newOrCopyNodeField(Field schemaField, BytecodeExpression getFieldCode) {
        ParameterizedType nodeType = typeFromJavaClassName(schemasInfo.get(schemaField.getType()).nodeClassName);

        // this.field == null ? new ValueNode() : (ValueNode)this.field.copy();
        return inlineIf(
                isNull(getFieldCode),
                newInstance(nodeType),
                copyNodeField(schemaField, getFieldCode)
        );
    }

    /**
     * Copies field into itself. Code like: {@code (ValueNode)this.field.copy();}.
     *
     * @param schemaField  Configuration schema class field.
     * @param getFieldCode Bytecode of getting the field, for example: {@code this.field} or {@code this.field.field};
     * @return Bytecode expression.
     */
    BytecodeExpression copyNodeField(Field schemaField, BytecodeExpression getFieldCode) {
        ParameterizedType nodeType = isNamedConfigValue(schemaField)
                ? type(NamedListNode.class) : typeFromJavaClassName(schemasInfo.get(schemaField.getType()).nodeClassName);

        // (ValueNode)this.field.copy();
        return getFieldCode.invoke(COPY).cast(nodeType);
    }

    /**
     * Creates {@code *Node::new} lambda expression with {@link Supplier} type.
     *
     * @param nodeClassName Name of the {@code *Node} class.
     * @return InvokeDynamic bytecode expression.
     */
    @NotNull
    private static BytecodeExpression newNamedListElementLambda(String nodeClassName) {
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
     * @param internalIdField    Internal id field or {@code null} if it's not present.
     * @return Constructed {@link DynamicConfiguration} definition for the configuration schema.
     */
    private ClassDefinition createCfgImplClass(
            Class<?> schemaClass,
            Set<Class<?>> internalExtensions,
            Set<Class<?>> polymorphicExtensions,
            Collection<Field> schemaFields,
            Collection<Field> internalFields,
            Collection<Field> polymorphicFields,
            @Nullable Field internalIdField
    ) {
        SchemaClassesInfo schemaClassInfo = schemasInfo.get(schemaClass);

        // Configuration impl class definition.
        ClassDefinition classDef = new ClassDefinition(
                of(PUBLIC, FINAL),
                internalName(schemaClassInfo.cfgImplClassName),
                type(DynamicConfiguration.class),
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

            if (isPolymorphicId(schemaField)) {
                polymorphicTypeIdFieldDef = fieldDef;
            }
        }

        if (internalIdField != null) {
            // Internal id dynamic property is stored as a regular field.
            String fieldName = internalIdField.getName();

            FieldDefinition fieldDef = addConfigurationImplField(classDef, internalIdField, fieldName);

            fieldDefs.put(fieldName, fieldDef);
        }

        FieldDefinition internalConfigTypesFieldDef = null;

        if (!internalExtensions.isEmpty()) {
            internalConfigTypesFieldDef = classDef.declareField(
                    of(PRIVATE, FINAL),
                    INTERNAL_CONFIG_TYPES_FIELD_NAME,
                    Class[].class
            );
        }

        // Constructor
        addConfigurationImplConstructor(
                classDef,
                schemaClass,
                internalExtensions,
                fieldDefs,
                schemaFields,
                internalFields,
                polymorphicFields,
                internalIdField,
                internalConfigTypesFieldDef
        );

        // org.apache.ignite.internal.configuration.DynamicProperty#directProxy
        addDirectProxyMethod(schemaClassInfo, classDef);

        // Getter for the internal id.
        if (internalIdField != null) {
            addConfigurationImplGetMethod(classDef, internalIdField, fieldDefs.get(internalIdField.getName()));
        }

        for (Field schemaField : concat(schemaFields, internalFields)) {
            addConfigurationImplGetMethod(classDef, schemaField, fieldDefs.get(fieldName(schemaField)));
        }

        // org.apache.ignite.internal.configuration.DynamicConfiguration#configType
        addCfgImplConfigTypeMethod(classDef, typeFromJavaClassName(schemaClassInfo.cfgClassName));

        if (internalConfigTypesFieldDef != null) {
            addCfgImplInternalConfigTypesMethod(classDef, internalConfigTypesFieldDef);
        }

        if (!polymorphicExtensions.isEmpty()) {
            addCfgSpecificConfigTreeMethod(classDef, schemaClass, polymorphicExtensions, polymorphicTypeIdFieldDef);

            addCfgRemoveMembersMethod(
                    classDef,
                    schemaClass,
                    polymorphicExtensions,
                    fieldDefs,
                    polymorphicFields,
                    polymorphicTypeIdFieldDef
            );

            addCfgAddMembersMethod(
                    classDef,
                    schemaClass,
                    polymorphicExtensions,
                    fieldDefs,
                    polymorphicFields,
                    polymorphicTypeIdFieldDef
            );

            addCfgImplPolymorphicInstanceConfigTypeMethod(
                    classDef,
                    schemaClass,
                    polymorphicExtensions,
                    polymorphicTypeIdFieldDef
            );
        }

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
     * @param fieldName   Field name, if {@code null} will be used {@link Field#getName}.
     * @return Declared field definition.
     */
    private FieldDefinition addConfigurationImplField(
            ClassDefinition classDef,
            Field schemaField,
            String fieldName
    ) {
        ParameterizedType fieldType;

        if (isConfigValue(schemaField)) {
            fieldType = typeFromJavaClassName(schemasInfo.get(schemaField.getType()).cfgImplClassName);
        } else if (isNamedConfigValue(schemaField)) {
            fieldType = type(NamedListConfiguration.class);
        } else {
            fieldType = type(DynamicProperty.class);
        }

        return classDef.declareField(of(PUBLIC), fieldName, fieldType);
    }

    /**
     * Implements default constructor for the configuration class. It initializes all fields and adds them to members collection.
     *
     * @param classDef Configuration impl class definition.
     * @param schemaClass Configuration schema class.
     * @param internalExtensions Internal extensions of the configuration schema.
     * @param fieldDefs Field definitions for all fields of configuration impl class.
     * @param schemaFields Fields of the schema class.
     * @param internalFields Fields of internal extensions of the configuration schema.
     * @param polymorphicFields Fields of polymorphic extensions of the configuration schema.
     * @param internalIdField Internal id field or {@code null} if it's not present.
     * @param internalConfigTypesFieldDef Field definition for {@link DynamicConfiguration#internalConfigTypes},
     *      {@code null} if there are no internal extensions.
     */
    private void addConfigurationImplConstructor(
            ClassDefinition classDef,
            Class<?> schemaClass,
            Set<Class<?>> internalExtensions,
            Map<String, FieldDefinition> fieldDefs,
            Collection<Field> schemaFields,
            Collection<Field> internalFields,
            Collection<Field> polymorphicFields,
            @Nullable Field internalIdField,
            @Nullable FieldDefinition internalConfigTypesFieldDef
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

        SchemaClassesInfo schemaClassInfo = schemasInfo.get(schemaClass);

        Variable thisVar = ctor.getThis();

        BytecodeBlock ctorBody = ctor.getBody()
                .append(thisVar)
                .append(ctor.getScope().getVariable("prefix"))
                .append(ctor.getScope().getVariable("key"))
                .append(rootKeyVar)
                .append(changerVar)
                .append(listenOnlyVar)
                .invokeConstructor(DYNAMIC_CONFIGURATION_CTOR);

        BytecodeExpression thisKeysVar = thisVar.getField("keys", List.class);

        // Wrap object into list to reuse the loop below.
        List<Field> internalIdFieldAsList = internalIdField == null ? emptyList() : List.of(internalIdField);

        int newIdx = 0;
        for (Field schemaField : concat(schemaFields, internalFields, polymorphicFields, internalIdFieldAsList)) {
            String fieldName = schemaField.getName();

            BytecodeExpression newValue;

            if (isValue(schemaField) || isPolymorphicId(schemaField) || isInjectedName(schemaField) || isInternalId(schemaField)) {
                // A field with @InjectedName is special (auxiliary), it is not stored in storages as a regular field, and therefore there
                // is no direct access to it. It is stored in the InnerNode and does not participate in its traversal, so in order to get
                // it we need to get the InnerNode, and only then the value of this field.

                // newValue = new DynamicProperty(this.keys, fieldName, rootKey, changer, listenOnly, readOnly);
                newValue = newInstance(
                        DynamicProperty.class,
                        thisKeysVar,
                        constantString(isInjectedName(schemaField) ? InnerNode.INJECTED_NAME
                                : isInternalId(schemaField) ? InnerNode.INTERNAL_ID : schemaField.getName()),
                        rootKeyVar,
                        changerVar,
                        listenOnlyVar,
                        constantBoolean(isPolymorphicId(schemaField) || isInjectedName(schemaField) || isInternalId(schemaField))
                );
            } else {
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
                } else {
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

                    // newValue = new NamedListConfiguration(this.keys, fieldName, rootKey, changer, listenOnly,
                    //      (p, k) -> new ValueConfigurationImpl(p, k, rootKey, changer, listenOnly),
                    //      (p, c) -> new ValueDirectProxy(p, c),
                    //      new ValueConfigurationImpl(this.keys, "any", rootKey, changer, true)
                    // );
                    newValue = newInstance(
                            NamedListConfiguration.class,
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
                            newDirectProxyLambda(fieldInfo),
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

            FieldDefinition fieldDef = fieldDefs.get(fieldName(schemaField));

            // this.field = newValue;
            ctorBody.append(thisVar.setField(fieldDef, newValue));

            if (!isPolymorphicConfigInstance(schemaField.getDeclaringClass()) && !isInternalId(schemaField)) {
                // add(this.field);
                ctorBody.append(thisVar.invoke(DYNAMIC_CONFIGURATION_ADD_MTD, thisVar.getField(fieldDef)));
            }
        }

        if (internalConfigTypesFieldDef != null) {
            assert !internalExtensions.isEmpty() : classDef;

            // Class[] tmp;
            Variable tmpVar = ctor.getScope().createTempVariable(Class[].class);

            BytecodeBlock initInternalConfigTypesField = new BytecodeBlock();

            // tmp = new Class[size];
            initInternalConfigTypesField.append(tmpVar.set(newArray(type(Class[].class), internalExtensions.size())));

            int i = 0;

            for (Class<?> extension : internalExtensions) {
                // tmp[i] = InternalTableConfiguration.class;
                initInternalConfigTypesField.append(set(
                        tmpVar,
                        constantInt(i++),
                        constantClass(typeFromJavaClassName(configurationClassName(extension)))
                ));
            }

            // this._internalConfigTypes = tmp;
            initInternalConfigTypesField.append(setThisFieldCode(ctor, tmpVar, internalConfigTypesFieldDef));

            ctorBody.append(initInternalConfigTypesField);
        }

        ctorBody.ret();
    }

    /**
     * Generates {@link ConfigurationNode#directProxy()} method that returns new instance every time.
     *
     * @param schemaClassInfo Schema class info.
     * @param classDef Class definition.
     */
    private void addDirectProxyMethod(
            SchemaClassesInfo schemaClassInfo,
            ClassDefinition classDef
    ) {
        MethodDefinition methodDef = classDef.declareMethod(
                of(PUBLIC), "directProxy", type(DirectPropertyProxy.class)
        );

        methodDef.getBody().append(newInstance(
                typeFromJavaClassName(schemaClassInfo.directProxyClassName),
                methodDef.getThis().invoke("keyPath", List.class),
                methodDef.getThis().getField("changer", DynamicConfigurationChanger.class)
        ));

        methodDef.getBody().retObject();
    }

    /**
     * Implements accessor method in configuration impl class.
     *
     * @param classDef    Configuration impl class definition.
     * @param schemaField Configuration Schema class field.
     * @param fieldDefs   Field definitions.
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

        if (isConfigValue(schemaField)) {
            returnType = typeFromJavaClassName(schemaClassInfo.cfgClassName);
        } else if (isNamedConfigValue(schemaField)) {
            returnType = type(NamedConfigurationTree.class);
        } else {
            assert isValue(schemaField) || isPolymorphicId(schemaField) || isInjectedName(schemaField)
                    || isInternalId(schemaField) : schemaField;

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

        if (isPolymorphicConfig(schemaFieldType) && isConfigValue(schemaField)) {
            // result = this.field.specificConfigTree();
            body.invokeVirtual(SPECIFIC_CONFIG_TREE_MTD);
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
    @NotNull
    static String internalName(String className) {
        return className.replace('.', '/');
    }

    /**
     * Creates boxed version of the class.
     *
     * @param clazz Maybe primitive class.
     * @return Not primitive class that represents parameter class.
     */
    public static Class<?> box(Class<?> clazz) {
        Class<?> boxed = TypeUtils.boxed(clazz);

        return boxed == null ? clazz : boxed;
    }

    /**
     * Get interfaces for {@link InnerNode} definition for a configuration schema.
     *
     * @param schemaClass      Configuration schema class.
     * @param schemaExtensions Internal extensions of the configuration schema.
     * @return Interfaces for {@link InnerNode} definition for a configuration schema.
     */
    static ParameterizedType[] nodeClassInterfaces(Class<?> schemaClass, Set<Class<?>> schemaExtensions) {
        Collection<ParameterizedType> res = new ArrayList<>();

        for (Class<?> cls : concat(List.of(schemaClass), schemaExtensions)) {
            res.add(typeFromJavaClassName(viewClassName(cls)));
            res.add(typeFromJavaClassName(changeClassName(cls)));
        }

        return res.toArray(ParameterizedType[]::new);
    }

    /**
     * Get interfaces for {@link DynamicConfiguration} definition for a configuration schema.
     *
     * @param schemaClass      Configuration schema class.
     * @param schemaExtensions Internal extensions of the configuration schema.
     * @return Interfaces for {@link DynamicConfiguration} definition for a configuration schema.
     */
    ParameterizedType[] configClassInterfaces(Class<?> schemaClass, Set<Class<?>> schemaExtensions) {
        List<ParameterizedType> result = Stream.concat(Stream.of(schemaClass), schemaExtensions.stream())
                .map(cls -> typeFromJavaClassName(configurationClassName(cls)))
                .collect(toCollection(ArrayList::new));

        return result.toArray(new ParameterizedType[0]);
    }

    /**
     * Add {@link DynamicConfiguration#configType} method implementation to the class.
     *
     * <p>It looks like the following code:
     * <pre><code>
     * public Class configType() {
     *     return RootConfiguration.class;
     * }
     * </code></pre>
     *
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
     * Add {@link DynamicConfiguration#internalConfigTypes} method implementation to the class.
     *
     * <p>It looks like the following code:
     * <pre><code>
     * public Class<?>[] internalConfigTypes() {
     *     return new Class<?>[]{FirstInternalTableConfiguration.class, SecondInternalTableConfiguration.class};
     * }
     * </code></pre>
     *
     * @param classDef Class definition.
     * @param internalConfigTypesDef Definition of the field in which the interfaces of the internal configuration extensions are stored.
     */
    private static void addCfgImplInternalConfigTypesMethod(ClassDefinition classDef, FieldDefinition internalConfigTypesDef) {
        MethodDefinition internalConfigTypesMtd = classDef.declareMethod(of(PUBLIC), "internalConfigTypes", type(Class[].class));

        internalConfigTypesMtd
                .getBody()
                .append(getThisFieldCode(internalConfigTypesMtd, internalConfigTypesDef))
                .retObject();
    }

    /**
     * Add {@link DynamicConfiguration#polymorphicInstanceConfigType} method implementation to the class.
     *
     * <p>It looks like the following code:
     * <pre><code>
     * public Class polymorphicInstanceConfigType() {
     *      InnerNode val = this.isRemovedFromNamedList() ? this.currentValue() : this.refreshValue();
     *      String typeId = val.polymorphicTypeId;
     *      switch(typeId) {
     *          case "hash":
     *              return HashIndexConfiguration.class;
     *          case "sorted"
     *              return SortedIndexConfiguration.class;
     *          default:
     *              throw new ConfigurationWrongPolymorphicTypeIdException(typeId);
     *      }
     * }
     * </code></pre>
     *
     * @param classDef Class definition.
     * @param schemaClass Polymorphic configuration schema (parent).
     * @param polymorphicExtensions Polymorphic configuration instance schemas (children).
     * @param polymorphicTypeIdFieldDef Identification field for the polymorphic configuration instance.
     */
    private static void addCfgImplPolymorphicInstanceConfigTypeMethod(
            ClassDefinition classDef,
            Class<?> schemaClass,
            Set<Class<?>> polymorphicExtensions,
            FieldDefinition polymorphicTypeIdFieldDef
    ) {
        MethodDefinition polymorphicInstanceConfigTypeMtd = classDef.declareMethod(
                of(PUBLIC),
                "polymorphicInstanceConfigType",
                type(Class.class)
        );

        // String tmpStr;
        Variable tmpStrVar = polymorphicInstanceConfigTypeMtd.getScope().createTempVariable(String.class);

        StringSwitchBuilder switchBuilder = new StringSwitchBuilder(polymorphicInstanceConfigTypeMtd.getScope())
                .expression(tmpStrVar)
                .defaultCase(throwException(ConfigurationWrongPolymorphicTypeIdException.class, tmpStrVar));

        for (Class<?> polymorphicExtension : polymorphicExtensions) {
            switchBuilder.addCase(
                    polymorphicInstanceId(polymorphicExtension),
                    constantClass(typeFromJavaClassName(configurationClassName(polymorphicExtension))).ret()
            );
        }

        // ConfigNode
        ParameterizedType nodeType = typeFromJavaClassName(nodeClassName(schemaClass));

        // Object tmpObj;
        Variable tmpObjVar = polymorphicInstanceConfigTypeMtd.getScope().createTempVariable(Object.class);

        // this;
        Variable thisVar = polymorphicInstanceConfigTypeMtd.getThis();

        // tmpObj = this.isRemovedFromNamedList() ? this.currentValue() : this.refreshValue();
        // tmpStr = ((ConfigNode) tmpObj).typeId;
        // switch(tmpStr) ...
        polymorphicInstanceConfigTypeMtd.getBody()
                .append(tmpObjVar.set(inlineIf(
                        thisVar.invoke(IS_REMOVED_FROM_NAMED_LIST_MTD),
                        thisVar.invoke(CURRENT_VALUE_MTD),
                        thisVar.invoke(REFRESH_VALUE_MTD))
                ))
                .append(tmpStrVar.set(tmpObjVar.cast(nodeType).getField(polymorphicTypeIdFieldDef.getName(), String.class)))
                .append(switchBuilder.build())
                .ret();
    }

    /**
     * Create a {@code *CfgImpl} for the polymorphic configuration instance schema.
     *
     * @param schemaClass           Polymorphic configuration schema (parent).
     * @param polymorphicExtension  Polymorphic configuration instance schema (child).
     * @param schemaCfgImplClassDef {@link DynamicConfiguration} definition for the polymorphic configuration schema {@code schemaClass}.
     * @param schemaFields          Schema fields of polymorphic configuration {@code schemaClass}.
     * @param polymorphicFields     Schema fields of a polymorphic configuration instance {@code polymorphicExtension}.
     * @param internalIdField       Internal id field or {@code null} if it's not present.
     */
    private ClassDefinition createPolymorphicExtensionCfgImplClass(
            Class<?> schemaClass,
            Class<?> polymorphicExtension,
            ClassDefinition schemaCfgImplClassDef,
            Collection<Field> schemaFields,
            Collection<Field> polymorphicFields,
            @Nullable Field internalIdField
    ) {
        SchemaClassesInfo schemaClassInfo = schemasInfo.get(schemaClass);
        SchemaClassesInfo polymorphicExtensionClassInfo = schemasInfo.get(polymorphicExtension);

        // Configuration impl class definition.
        ClassDefinition classDef = new ClassDefinition(
                of(PUBLIC, FINAL),
                internalName(polymorphicExtensionClassInfo.cfgImplClassName),
                type(ConfigurationTreeWrapper.class),
                configClassInterfaces(polymorphicExtension, Set.of())
        );

        // private final ParentCfgImpl this$0;
        FieldDefinition parentCfgImplFieldDef = classDef.declareField(
                of(PRIVATE, FINAL),
                "this$0",
                typeFromJavaClassName(schemaClassInfo.cfgImplClassName)
        );

        // Constructor.
        MethodDefinition constructorMtd = classDef.declareConstructor(
                of(PUBLIC),
                arg("delegate", typeFromJavaClassName(schemaClassInfo.cfgImplClassName))
        );

        Variable delegateVar = constructorMtd.getScope().getVariable("delegate");

        // Constructor body.
        // super(parent);
        // this.this$0 = parent;
        constructorMtd.getBody()
                .append(constructorMtd.getThis())
                .append(delegateVar)
                .invokeConstructor(ConfigurationTreeWrapper.class, ConfigurationTree.class)
                .append(constructorMtd.getThis().setField(
                        parentCfgImplFieldDef,
                        delegateVar
                ))
                .ret();

        Map<String, FieldDefinition> fieldDefs = schemaCfgImplClassDef.getFields().stream()
                .collect(toMap(FieldDefinition::getName, identity()));

        if (internalIdField != null) {
            addConfigurationImplGetMethod(classDef, internalIdField, parentCfgImplFieldDef, fieldDefs.get(internalIdField.getName()));
        }

        for (Field schemaField : concat(schemaFields, polymorphicFields)) {
            addConfigurationImplGetMethod(classDef, schemaField, parentCfgImplFieldDef, fieldDefs.get(fieldName(schemaField)));
        }

        return classDef;
    }

    /**
     * Adds a {@link DynamicConfiguration#specificConfigTree} override for the polymorphic configuration case.
     *
     * @param classDef                  Definition of a polymorphic configuration class (parent).
     * @param schemaClass               Polymorphic configuration schema (parent).
     * @param polymorphicExtensions     Polymorphic configuration instance schemas (children).
     * @param polymorphicTypeIdFieldDef Identification field for the polymorphic configuration instance.
     */
    private void addCfgSpecificConfigTreeMethod(
            ClassDefinition classDef,
            Class<?> schemaClass,
            Set<Class<?>> polymorphicExtensions,
            FieldDefinition polymorphicTypeIdFieldDef
    ) {
        MethodDefinition specificConfigMtd = classDef.declareMethod(
                of(PUBLIC),
                SPECIFIC_CONFIG_TREE_MTD.getName(),
                type(ConfigurationTree.class)
        );

        // String tmpStr;
        Variable tmpStrVar = specificConfigMtd.getScope().createTempVariable(String.class);

        StringSwitchBuilder switchBuilder = new StringSwitchBuilder(specificConfigMtd.getScope())
                .expression(tmpStrVar)
                .defaultCase(throwException(ConfigurationWrongPolymorphicTypeIdException.class, tmpStrVar));

        for (Class<?> polymorphicExtension : polymorphicExtensions) {
            // return new SpecialCfgImpl(this);
            switchBuilder.addCase(
                    polymorphicInstanceId(polymorphicExtension),
                    newInstance(
                            typeFromJavaClassName(schemasInfo.get(polymorphicExtension).cfgImplClassName),
                            specificConfigMtd.getThis()
                    ).ret()
            );
        }

        // ConfigNode
        ParameterizedType nodeType = typeFromJavaClassName(schemasInfo.get(schemaClass).nodeClassName);

        // Object tmpObj;
        Variable tmpObjVar = specificConfigMtd.getScope().createTempVariable(Object.class);

        // tmpObj = this.refreshValue();
        // tmpStr = ((ConfigNode) tmpObj).typeId;
        // switch(tmpStr) ...
        specificConfigMtd.getBody()
                .append(tmpObjVar.set(specificConfigMtd.getThis().invoke(REFRESH_VALUE_MTD)))
                .append(tmpStrVar.set(tmpObjVar.cast(nodeType).getField(polymorphicTypeIdFieldDef.getName(), String.class)))
                .append(switchBuilder.build())
                .ret();
    }

    /**
     * Adds a {@code DynamicConfiguration#removeMembers} override for the polymorphic configuration case.
     *
     * @param classDef                  Definition of a polymorphic configuration class (parent).
     * @param schemaClass               Polymorphic configuration schema (parent).
     * @param polymorphicExtensions     Polymorphic configuration instance schemas (children).
     * @param fieldDefs                 Field definitions for all fields of {@code classDef}.
     * @param polymorphicFields         Fields of polymorphic extensions.
     * @param polymorphicTypeIdFieldDef Identification field for the polymorphic configuration instance.
     */
    private void addCfgRemoveMembersMethod(
            ClassDefinition classDef,
            Class<?> schemaClass,
            Set<Class<?>> polymorphicExtensions,
            Map<String, FieldDefinition> fieldDefs,
            Collection<Field> polymorphicFields,
            FieldDefinition polymorphicTypeIdFieldDef
    ) {
        MethodDefinition removeMembersMtd = classDef.declareMethod(
                of(PUBLIC),
                "removeMembers",
                type(void.class),
                arg("oldValue", type(Object.class)),
                arg("members", type(Map.class))
        );

        // InnerNode oldValue;
        Variable oldValueVar = removeMembersMtd.getScope().getVariable("oldValue");

        // Map members;
        Variable membersVar = removeMembersMtd.getScope().getVariable("members");

        // String tmpStr;
        Variable tmpStrVar = removeMembersMtd.getScope().createTempVariable(String.class);

        StringSwitchBuilder switchBuilder = new StringSwitchBuilder(removeMembersMtd.getScope())
                .expression(tmpStrVar)
                .defaultCase(throwException(ConfigurationWrongPolymorphicTypeIdException.class, tmpStrVar));

        for (Class<?> polymorphicExtension : polymorphicExtensions) {
            Collection<Field> removeFields = polymorphicFields.stream()
                    .filter(f -> !polymorphicExtension.equals(f.getDeclaringClass()))
                    .collect(toList());

            BytecodeBlock blockCode = new BytecodeBlock();

            for (Field removeField : removeFields) {
                // this.removeMember(members, this.field);
                blockCode
                        .append(removeMembersMtd.getThis())
                        .append(membersVar)
                        .append(getThisFieldCode(removeMembersMtd, fieldDefs.get(fieldName(removeField))))
                        .invokeVirtual(REMOVE_MEMBER_MTD);
            }

            switchBuilder.addCase(polymorphicInstanceId(polymorphicExtension), blockCode);
        }

        // ConfigNode
        ParameterizedType nodeType = typeFromJavaClassName(schemasInfo.get(schemaClass).nodeClassName);

        // tmpStr = ((ConfigNode) oldValue).typeId;
        // switch(tmpStr) ...
        removeMembersMtd.getBody()
                .append(tmpStrVar.set(oldValueVar.cast(nodeType).getField(polymorphicTypeIdFieldDef.getName(), String.class)))
                .append(switchBuilder.build())
                .ret();
    }

    /**
     * Adds a {@code DynamicConfiguration#addMembers} override for the polymorphic configuration case.
     *
     * @param classDef                  Definition of a polymorphic configuration class (parent).
     * @param schemaClass               Polymorphic configuration schema (parent).
     * @param polymorphicExtensions     Polymorphic configuration instance schemas (children).
     * @param fieldDefs                 Field definitions for all fields of {@code classDef}.
     * @param polymorphicFields         Fields of polymorphic extensions.
     * @param polymorphicTypeIdFieldDef Identification field for the polymorphic configuration instance.
     */
    private void addCfgAddMembersMethod(
            ClassDefinition classDef,
            Class<?> schemaClass,
            Set<Class<?>> polymorphicExtensions,
            Map<String, FieldDefinition> fieldDefs,
            Collection<Field> polymorphicFields,
            FieldDefinition polymorphicTypeIdFieldDef
    ) {
        MethodDefinition removeMembersMtd = classDef.declareMethod(
                of(PUBLIC),
                "addMembers",
                type(void.class),
                arg("newValue", type(Object.class)),
                arg("members", type(Map.class))
        );

        // InnerNode newValue;
        Variable newValueVar = removeMembersMtd.getScope().getVariable("newValue");

        // Map members;
        Variable membersVar = removeMembersMtd.getScope().getVariable("members");

        // String tmpStr;
        Variable tmpStrVar = removeMembersMtd.getScope().createTempVariable(String.class);

        StringSwitchBuilder switchBuilder = new StringSwitchBuilder(removeMembersMtd.getScope())
                .expression(tmpStrVar)
                .defaultCase(throwException(ConfigurationWrongPolymorphicTypeIdException.class, tmpStrVar));

        for (Class<?> polymorphicExtension : polymorphicExtensions) {
            Collection<Field> addFields = polymorphicFields.stream()
                    .filter(f -> polymorphicExtension.equals(f.getDeclaringClass()))
                    .collect(toList());

            BytecodeBlock blockCode = new BytecodeBlock();

            for (Field addField : addFields) {
                // this.addMember(members, this.field);
                blockCode
                        .append(removeMembersMtd.getThis())
                        .append(membersVar)
                        .append(getThisFieldCode(removeMembersMtd, fieldDefs.get(fieldName(addField))))
                        .invokeVirtual(ADD_MEMBER_MTD);
            }

            switchBuilder.addCase(polymorphicInstanceId(polymorphicExtension), blockCode);
        }

        // ConfigNode
        ParameterizedType nodeType = typeFromJavaClassName(schemasInfo.get(schemaClass).nodeClassName);

        // tmpStr = ((ConfigNode) newValue).typeId;
        // switch(tmpStr) ...
        removeMembersMtd.getBody()
                .append(tmpStrVar.set(newValueVar.cast(nodeType).getField(polymorphicTypeIdFieldDef.getName(), String.class)))
                .append(switchBuilder.build())
                .ret();
    }


    /**
     * Creates the bytecode {@code throw new Exception(msg);}.
     *
     * @param throwableClass Exception class.
     * @param parameters     Exception constructor parameters.
     * @return Exception throwing bytecode.
     */
    static BytecodeBlock throwException(
            Class<? extends Throwable> throwableClass,
            BytecodeExpression... parameters
    ) {
        return new BytecodeBlock().append(newInstance(throwableClass, parameters)).throwObject();
    }

    /**
     * Returns the name of the configuration schema field. If the schema contains {@link PolymorphicConfigInstance}, it will return "{@link
     * Field#getName()} + {@code "#"} + {@link PolymorphicConfigInstance#value()}" otherwise "{@link Field#getName}".
     *
     * @param f Configuration schema field.
     * @return Field name.
     */
    static String fieldName(Field f) {
        return isPolymorphicConfigInstance(f.getDeclaringClass())
                ? f.getName() + "#" + polymorphicInstanceId(f.getDeclaringClass()) : f.getName();
    }

    /**
     * Creates a string switch builder by {@code typeIdFieldDef}.
     *
     * @param mtdDef         Method definition.
     * @param typeIdFieldDef Field definition that contains the id of the polymorphic configuration instance.
     * @return String switch builder by {@code typeIdFieldDef}.
     */
    static StringSwitchBuilder typeIdSwitchBuilder(MethodDefinition mtdDef, FieldDefinition typeIdFieldDef) {
        BytecodeExpression typeIdVar = mtdDef.getThis().getField(typeIdFieldDef);

        return new StringSwitchBuilder(mtdDef.getScope())
                .expression(typeIdVar)
                .defaultCase(throwException(ConfigurationWrongPolymorphicTypeIdException.class, typeIdVar));
    }

    /**
     * Generates bytecode to get a class field like {@code this.field;} or {@code this.field.field;}.
     *
     * @param mtdDef    Class method definition.
     * @param fieldDefs Field definitions.
     * @return Bytecode for getting the field.
     */
    static BytecodeExpression getThisFieldCode(MethodDefinition mtdDef, FieldDefinition... fieldDefs) {
        assert !nullOrEmpty(fieldDefs);

        // this.field;
        BytecodeExpression getFieldCode = mtdDef.getThis().getField(fieldDefs[0]);

        // this.field.field; etc.
        for (int i = 1; i < fieldDefs.length; i++) {
            getFieldCode = getFieldCode.getField(fieldDefs[i]);
        }

        return getFieldCode;
    }

    /**
     * Generates bytecode to set a class field like {@code this.field = value;} or {@code this.field.field = value;}.
     *
     * @param mtdDef    Class method definition.
     * @param value     Value of the field to be set.
     * @param fieldDefs Field definitions.
     * @return Bytecode for setting the field value.
     */
    static BytecodeExpression setThisFieldCode(
            MethodDefinition mtdDef,
            BytecodeExpression value,
            FieldDefinition... fieldDefs
    ) {
        assert !nullOrEmpty(fieldDefs);

        if (fieldDefs.length == 1) {
            // this.field = value;
            return mtdDef.getThis().setField(fieldDefs[0], value);
        } else {
            // this.field;
            BytecodeExpression getFieldCode = mtdDef.getThis().getField(fieldDefs[0]);

            // this.field.field; etc.
            for (int i = 1; i < fieldDefs.length - 1; i++) {
                getFieldCode = getFieldCode.getField(fieldDefs[i]);
            }

            // this.field.field = value;
            return getFieldCode.setField(fieldDefs[fieldDefs.length - 1], value);
        }
    }

    /**
     * Returns configuration schema field with {@link PolymorphicId}.
     *
     * @param schemaClass Configuration schema class.
     * @return Configuration schema field with {@link PolymorphicId}.
     */
    @Nullable
    static Field polymorphicIdField(Class<?> schemaClass) {
        assert isPolymorphicConfig(schemaClass) : schemaClass.getName();

        for (Field f : schemaClass.getDeclaredFields()) {
            if (isPolymorphicId(f)) {
                return f;
            }
        }

        return null;
    }

    /**
     * Returns name of the method to change the field.
     *
     * @param schemaField Configuration schema field.
     * @return Name of the method to change the field.
     */
    static String changeMethodName(String schemaField) {
        return "change" + capitalize(schemaField);
    }

    /**
     * Creates bytecode like: {@link new NamedListNode<>(key, ValueNode::new, "polymorphicIdFieldName");}.
     *
     * @param schemaField Schema field with {@link NamedConfigValue}.
     * @return Bytecode like: new NamedListNode<>(key, ValueNode::new, "polymorphicIdFieldName");
     */
    BytecodeExpression newNamedListNode(Field schemaField) {
        assert isNamedConfigValue(schemaField) : schemaField;

        Class<?> fieldType = schemaField.getType();

        SchemaClassesInfo fieldClassNames = schemasInfo.get(fieldType);

        String syntheticKeyName = Arrays.stream(schemaField.getType().getDeclaredFields())
                .filter(ConfigurationUtil::isInjectedName)
                .map(Field::getName)
                .findFirst()
                .orElse(schemaField.getAnnotation(NamedConfigValue.class).syntheticKeyName());

        return newInstance(
                NamedListNode.class,
                constantString(syntheticKeyName),
                newNamedListElementLambda(fieldClassNames.nodeClassName),
                isPolymorphicConfig(fieldType) ? constantString(polymorphicIdField(fieldType).getName()) : constantNull(String.class)
        );
    }
}
