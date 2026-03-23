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

import static com.facebook.presto.bytecode.Access.BRIDGE;
import static com.facebook.presto.bytecode.Access.FINAL;
import static com.facebook.presto.bytecode.Access.PRIVATE;
import static com.facebook.presto.bytecode.Access.PUBLIC;
import static com.facebook.presto.bytecode.Access.STATIC;
import static com.facebook.presto.bytecode.Access.SYNTHETIC;
import static com.facebook.presto.bytecode.Parameter.arg;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.bytecode.ParameterizedType.typeFromJavaClassName;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantClass;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantInt;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantNull;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantString;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.getStatic;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.inlineIf;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.invokeStatic;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.isNotNull;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.isNull;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.newArray;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.newInstance;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.not;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.set;
import static java.util.Collections.singleton;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.internal.configuration.asm.ConfigurationAsmGenerator.box;
import static org.apache.ignite.internal.configuration.asm.ConfigurationAsmGenerator.changeMethodName;
import static org.apache.ignite.internal.configuration.asm.ConfigurationAsmGenerator.fieldName;
import static org.apache.ignite.internal.configuration.asm.ConfigurationAsmGenerator.getThisFieldCode;
import static org.apache.ignite.internal.configuration.asm.ConfigurationAsmGenerator.internalName;
import static org.apache.ignite.internal.configuration.asm.ConfigurationAsmGenerator.legacyNames;
import static org.apache.ignite.internal.configuration.asm.ConfigurationAsmGenerator.nodeClassInterfaces;
import static org.apache.ignite.internal.configuration.asm.ConfigurationAsmGenerator.polymorphicIdField;
import static org.apache.ignite.internal.configuration.asm.ConfigurationAsmGenerator.publicName;
import static org.apache.ignite.internal.configuration.asm.ConfigurationAsmGenerator.setThisFieldCode;
import static org.apache.ignite.internal.configuration.asm.ConfigurationAsmGenerator.throwException;
import static org.apache.ignite.internal.configuration.asm.ConfigurationAsmGenerator.typeIdSwitchBuilder;
import static org.apache.ignite.internal.configuration.asm.SchemaClassesInfo.changeClassName;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.containsNameAnnotation;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.hasDefault;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.isConfigValue;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.isInjectedName;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.isInjectedValue;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.isNamedConfigValue;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.isPolymorphicConfig;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.isPolymorphicConfigInstance;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.isPolymorphicId;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.isReadOnly;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.isValue;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.polymorphicInstanceId;
import static org.apache.ignite.internal.util.CollectionUtils.concat;

import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.BytecodeNode;
import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.bytecode.FieldDefinition;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.ParameterizedType;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.control.IfStatement;
import com.facebook.presto.bytecode.expression.BytecodeExpression;
import com.facebook.presto.bytecode.expression.BytecodeExpressions;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.configuration.ConfigurationWrongPolymorphicTypeIdException;
import org.apache.ignite.configuration.NamedListChange;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.annotation.AbstractConfiguration;
import org.apache.ignite.configuration.annotation.InjectedName;
import org.apache.ignite.configuration.annotation.Name;
import org.apache.ignite.configuration.annotation.PolymorphicConfig;
import org.apache.ignite.configuration.annotation.PolymorphicId;
import org.apache.ignite.internal.configuration.tree.ConfigurationSource;
import org.apache.ignite.internal.configuration.tree.ConfigurationVisitor;
import org.apache.ignite.internal.configuration.tree.ConstructableTreeNode;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.apache.ignite.internal.configuration.tree.NamedListNode;
import org.apache.ignite.internal.configuration.util.ConfigurationUtil;
import org.apache.ignite.internal.util.ArrayUtils;
import org.jetbrains.annotations.Nullable;

// TODO Simplify code generation process: https://issues.apache.org/jira/browse/IGNITE-18366
class InnerNodeAsmGenerator extends AbstractAsmGenerator {
    /** {@link Consumer#accept(Object)}. */
    private static final Method ACCEPT;

    /** {@link ConfigurationVisitor#visitLeafNode(Field, String, Serializable)}. */
    private static final Method VISIT_LEAF;

    /** {@link ConfigurationVisitor#visitInnerNode(Field, String, InnerNode)}. */
    private static final Method VISIT_INNER;

    /** {@link ConfigurationVisitor#visitNamedListNode(Field, String, NamedListNode)}. */
    private static final Method VISIT_NAMED;

    /** {@link ConfigurationSource#unwrap(Class)}. */
    private static final Method UNWRAP;

    /** {@link ConfigurationSource#descend(ConstructableTreeNode)}. */
    private static final Method DESCEND;

    /** {@link InnerNode#internalId()}. */
    private static final Method INTERNAL_ID;

    /** {@link Objects#requireNonNull(Object, String)}. */
    private static final Method REQUIRE_NON_NULL;

    /** {@link Class#getName} method. */
    private static final Method CLASS_GET_NAME_MTD;

    /** {@link String#equals} method. */
    private static final Method STRING_EQUALS_MTD;

    /** {@link ConfigurationSource#polymorphicTypeId} method. */
    private static final Method POLYMORPHIC_TYPE_ID_MTD;

    /** {@link InnerNode#constructDefault} method. */
    private static final Method CONSTRUCT_DEFAULT_MTD;

    /** {@link InnerNode#specificNode} method. */
    private static final Method SPECIFIC_NODE_MTD;

    /** {@link ConfigurationUtil#addDefaults}. */
    private static final Method ADD_DEFAULTS_MTD;

    /** {@link InnerNode#setInjectedNameFieldValue}. */
    private static final Method SET_INJECTED_NAME_FIELD_VALUE_MTD;

    /** {@link InnerNode#isPolymorphic}. */
    private static final Method IS_POLYMORPHIC_MTD;

    /** {@link InnerNode#extensionSchemaTypes}. */
    private static final Method EXTENSION_SCHEMA_TYPES_MTD;

    /** {@link InnerNode#assertMutability()}. */
    private static final Method ASSERT_MUTABILITY_MTD;

    /** {@link Class#getDeclaredField(String)}. */
    private static final Method GET_DECLARED_FIELD_MTD;

    /** {@code Node#convert} method name. */
    private static final String CONVERT_MTD_NAME = "convert";

    /** {@link ConstructableTreeNode#construct(String, ConfigurationSource, boolean)} method name. */
    private static final String CONSTRUCT_MTD_NAME = "construct";

    /** {@link ConstructableTreeNode#injectedValueFieldName}. */
    private static final String INJECTED_VALUE_FIELD_NAME_MTD_NAME = "injectedValueFieldName";

    /** Mapping for each configuration {@link Field} to a static constant with this {@link Field} as value. */
    private final Map<Field, FieldDefinition> fieldToFieldDefinitionMap = new HashMap<>();

    static {
        try {
            ACCEPT = Consumer.class.getDeclaredMethod("accept", Object.class);

            VISIT_LEAF = ConfigurationVisitor.class
                    .getDeclaredMethod("visitLeafNode", Field.class, String.class, Serializable.class);

            VISIT_INNER = ConfigurationVisitor.class
                    .getDeclaredMethod("visitInnerNode", Field.class, String.class, InnerNode.class);

            VISIT_NAMED = ConfigurationVisitor.class
                    .getDeclaredMethod("visitNamedListNode", Field.class, String.class, NamedListNode.class);

            UNWRAP = ConfigurationSource.class.getDeclaredMethod("unwrap", Class.class);

            DESCEND = ConfigurationSource.class.getDeclaredMethod("descend", ConstructableTreeNode.class);

            INTERNAL_ID = InnerNode.class.getDeclaredMethod("internalId");

            REQUIRE_NON_NULL = Objects.class.getDeclaredMethod("requireNonNull", Object.class, String.class);

            CLASS_GET_NAME_MTD = Class.class.getDeclaredMethod("getName");

            STRING_EQUALS_MTD = String.class.getDeclaredMethod("equals", Object.class);

            POLYMORPHIC_TYPE_ID_MTD = ConfigurationSource.class.getDeclaredMethod("polymorphicTypeId", String.class);

            CONSTRUCT_DEFAULT_MTD = InnerNode.class.getDeclaredMethod("constructDefault", String.class);

            SPECIFIC_NODE_MTD = InnerNode.class.getDeclaredMethod("specificNode");

            ADD_DEFAULTS_MTD = ConfigurationUtil.class.getDeclaredMethod("addDefaults", InnerNode.class);

            SET_INJECTED_NAME_FIELD_VALUE_MTD = InnerNode.class.getDeclaredMethod("setInjectedNameFieldValue", String.class);

            IS_POLYMORPHIC_MTD = InnerNode.class.getDeclaredMethod("isPolymorphic");

            EXTENSION_SCHEMA_TYPES_MTD = InnerNode.class.getDeclaredMethod("extensionSchemaTypes");

            ASSERT_MUTABILITY_MTD = InnerNode.class.getDeclaredMethod("assertMutability");

            GET_DECLARED_FIELD_MTD = Class.class.getDeclaredMethod("getDeclaredField", String.class);
        } catch (NoSuchMethodException nsme) {
            throw new ExceptionInInitializerError(nsme);
        }
    }

    /** Class definition that extends the {@link InnerNode}. */
    private ClassDefinition innerNodeClassDef;

    InnerNodeAsmGenerator(
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
        super(
                cgen,
                schemaClass,
                extensions,
                polymorphicExtensions,
                schemaFields,
                publicExtensionFields,
                internalExtensionFields,
                polymorphicFields,
                internalIdField
        );
    }

    @Override
    public List<ClassDefinition> generate() {
        assert innerNodeClassDef == null;

        List<ClassDefinition> classDefs = new ArrayList<>();

        classDefs.add(createNodeClass());

        for (Class<?> polymorphicExtension : polymorphicExtensions) {
            // Only the fields of a specific instance of a polymorphic configuration.
            Collection<Field> polymorphicFields = this.polymorphicFields.stream()
                    .filter(f -> f.getDeclaringClass() == polymorphicExtension)
                    .collect(toList());

            classDefs.add(createPolymorphicExtensionNodeClass(polymorphicExtension, polymorphicFields));
        }

        return classDefs;
    }

    /**
     * Construct a {@link InnerNode} definition for a configuration schema.
     *
     * @return Constructed {@link InnerNode} definition for the configuration schema.
     */
    private ClassDefinition createNodeClass() {
        SchemaClassesInfo schemaClassInfo = cgen.schemaInfo(schemaClass);

        // Node class definition.
        innerNodeClassDef = new ClassDefinition(
                EnumSet.of(PUBLIC, FINAL),
                internalName(schemaClassInfo.nodeClassName),
                type(InnerNode.class),
                nodeClassInterfaces(schemaClass, extensions)
        );

        // Spec fields.
        Map<Class<?>, FieldDefinition> specFields = new HashMap<>();

        int i = 0;

        for (Class<?> clazz : concat(List.of(schemaClass), extensions, polymorphicExtensions)) {
            specFields.put(clazz, innerNodeClassDef.declareField(EnumSet.of(PRIVATE, FINAL), "_spec" + i++, clazz));
        }

        // Define the rest of the fields.
        Map<String, FieldDefinition> fieldDefs = new HashMap<>();

        // To store the id of the polymorphic configuration instance.
        FieldDefinition polymorphicTypeIdFieldDef = null;

        // Field with @InjectedName.
        FieldDefinition injectedNameFieldDef = null;

        // Field with @InjectedValue.
        Field injectedValueField = null;

        for (Field schemaField : concat(schemaFields, publicExtensionFields, internalExtensionFields, polymorphicFields)) {
            FieldDefinition fieldDef = addInnerNodeField(schemaField);

            fieldDefs.put(fieldDef.getName(), fieldDef);

            if (isPolymorphicId(schemaField)) {
                polymorphicTypeIdFieldDef = fieldDef;
            } else if (isInjectedName(schemaField)) {
                injectedNameFieldDef = fieldDef;
            } else if (isInjectedValue(schemaField)) {
                injectedValueField = schemaField;
            }
        }

        MethodDefinition classInitializer = innerNodeClassDef.getClassInitializer();
        fieldToFieldDefinitionMap.forEach((k, v) -> {
            // Get declared field.
            BytecodeExpression getDeclaredFieldExp = constantClass(k.getDeclaringClass())
                    .invoke(GET_DECLARED_FIELD_MTD, constantString(k.getName()));
            classInitializer.getBody().append(BytecodeExpressions.setStatic(v, getDeclaredFieldExp));
        });

        // org.apache.ignite.internal.configuration.tree.InnerNode#schemaType
        addNodeSchemaTypeMethod(polymorphicTypeIdFieldDef);

        FieldDefinition extensionSchemaTypesFieldDef = null;

        if (!extensions.isEmpty()) {
            extensionSchemaTypesFieldDef = innerNodeClassDef.declareField(
                    EnumSet.of(PRIVATE, FINAL),
                    "_" + EXTENSION_SCHEMA_TYPES_MTD.getName(),
                    Class[].class
            );
        }

        // Constructor.
        addNodeConstructor(
                specFields,
                fieldDefs,
                extensionSchemaTypesFieldDef
        );

        // Add view method for internal id.
        if (internalIdField != null) {
            addNodeInternalIdMethod();
        }

        // VIEW and CHANGE methods.
        for (Field schemaField : concat(schemaFields, publicExtensionFields, internalExtensionFields)) {
            String fieldName = schemaField.getName();

            FieldDefinition fieldDef = fieldDefs.get(fieldName);

            addNodeViewMethod(
                    innerNodeClassDef,
                    schemaField,
                    viewMtd -> getThisFieldCode(viewMtd, fieldDef),
                    null
            );

            // Read only.
            if (isPolymorphicId(schemaField) || isInjectedName(schemaField)) {
                continue;
            }

            // Add change methods.
            List<MethodDefinition> changeMethods = addNodeChangeMethod(
                    innerNodeClassDef,
                    schemaField,
                    changeMtd -> getThisFieldCode(changeMtd, fieldDef),
                    (changeMtd, newValue) -> setThisFieldCode(changeMtd, newValue, fieldDef),
                    null
            );

            // Only first element requires a bridge. Please refer to "addNodeChangeMethod" for explanation.
            addNodeChangeBridgeMethod(innerNodeClassDef, changeClassName(schemaField.getDeclaringClass()), changeMethods.get(0));
        }

        Map<Class<?>, List<Field>> polymorphicFieldsByExtension = Map.of();

        MethodDefinition changePolymorphicTypeIdMtd = null;

        if (!polymorphicExtensions.isEmpty()) {
            assert polymorphicTypeIdFieldDef != null : schemaClass.getName();

            addNodeSpecificNodeMethod(polymorphicTypeIdFieldDef);

            changePolymorphicTypeIdMtd = addNodeChangePolymorphicTypeIdMethod(fieldDefs, polymorphicTypeIdFieldDef);

            addNodeConvertMethods(changePolymorphicTypeIdMtd);

            polymorphicFieldsByExtension = new LinkedHashMap<>();

            for (Class<?> polymorphicExtension : polymorphicExtensions) {
                polymorphicFieldsByExtension.put(
                        polymorphicExtension,
                        polymorphicFields.stream()
                                .filter(f -> polymorphicExtension.equals(f.getDeclaringClass()))
                                .collect(toList())
                );
            }
        }

        // traverseChildren
        addNodeTraverseChildrenMethod(
                fieldDefs,
                polymorphicFieldsByExtension,
                polymorphicTypeIdFieldDef
        );

        // traverseChild
        addNodeTraverseChildMethod(
                fieldDefs,
                polymorphicFieldsByExtension,
                polymorphicTypeIdFieldDef
        );

        // construct
        addNodeConstructMethod(
                fieldDefs,
                polymorphicFieldsByExtension,
                polymorphicTypeIdFieldDef,
                changePolymorphicTypeIdMtd
        );

        // constructDefault
        addNodeConstructDefaultMethod(
                specFields,
                fieldDefs,
                polymorphicFieldsByExtension,
                polymorphicTypeIdFieldDef
        );

        if (injectedNameFieldDef != null) {
            addInjectedNameFieldMethods(injectedNameFieldDef);
        }

        if (injectedValueField != null) {
            implementInjectedValueFieldNameMethod(injectedValueField);
        }

        if (polymorphicTypeIdFieldDef != null) {
            addIsPolymorphicMethod();
        }

        if (extensionSchemaTypesFieldDef != null) {
            addExtensionSchemaTypesMethod(extensionSchemaTypesFieldDef);
        }

        if (schemaClass.getSuperclass().isAnnotationPresent(AbstractConfiguration.class)) {
            addIsExtendAbstractConfigurationMethod();
        }

        return innerNodeClassDef;
    }

    /**
     * Add {@link InnerNode#schemaType} method implementation to the class.
     *
     * @param polymorphicTypeIdFieldDef Identification field for the polymorphic configuration instance.
     */
    private void addNodeSchemaTypeMethod(@Nullable FieldDefinition polymorphicTypeIdFieldDef) {
        MethodDefinition schemaTypeMtd = innerNodeClassDef.declareMethod(
                EnumSet.of(PUBLIC),
                "schemaType",
                type(Class.class)
        );

        BytecodeBlock mtdBody = schemaTypeMtd.getBody();

        if (polymorphicExtensions.isEmpty()) {
            mtdBody.append(constantClass(schemaClass)).retObject();
        } else {
            assert polymorphicTypeIdFieldDef != null : innerNodeClassDef.getName();

            StringSwitchBuilder switchBuilderTypeId = typeIdSwitchBuilder(schemaTypeMtd, polymorphicTypeIdFieldDef);

            for (Class<?> polymorphicExtension : polymorphicExtensions) {
                switchBuilderTypeId.addCase(
                        polymorphicInstanceId(polymorphicExtension),
                        constantClass(polymorphicExtension).ret()
                );
            }

            mtdBody.append(switchBuilderTypeId.build());
        }
    }

    /**
     * Declares a field that corresponds to configuration value. Depending on the schema, 5 options are possible:
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
     *     <li>
     *         {@code @InjectedName public String fieldName}<br/>becomes<br/>
     *         {@code public String fieldName}
     *     </li>
     * </ul>
     *
     * @param schemaField Configuration Schema class field.
     * @return Declared field definition.
     * @throws IllegalArgumentException If an unsupported {@code schemaField} was passed.
     */
    private FieldDefinition addInnerNodeField(Field schemaField) {
        String fieldName = fieldName(schemaField);

        Class<?> schemaFieldClass = schemaField.getType();

        ParameterizedType nodeFieldType;

        if (isValue(schemaField) || isPolymorphicId(schemaField) || isInjectedName(schemaField)) {
            nodeFieldType = type(box(schemaFieldClass));
        } else if (isConfigValue(schemaField)) {
            nodeFieldType = typeFromJavaClassName(cgen.schemaInfo(schemaFieldClass).nodeClassName);
        } else if (isNamedConfigValue(schemaField)) {
            nodeFieldType = type(NamedListNode.class);
        } else {
            throw new IllegalArgumentException("Unsupported field: " + schemaField);
        }

        fieldToFieldDefinitionMap.put(
                schemaField,
                innerNodeClassDef.declareField(
                        EnumSet.of(PUBLIC, STATIC, FINAL),
                        fieldName.toUpperCase(Locale.ROOT) + "_SCHEMA_FIELD",
                        Field.class
                )
        );

        return innerNodeClassDef.declareField(EnumSet.of(PUBLIC), fieldName, nodeFieldType);
    }

    /**
     * Implements default constructor for the node class. It initializes {@code _spec} field and every other field that represents named
     * list configuration.
     *
     * @param specFields Definition of fields for the {@code _spec#} fields of the node class. Mapping: configuration schema class -> {@code
     * _spec#} field.
     * @param fieldDefs Field definitions for all fields of node class excluding {@code _spec}.
     * @param extensionSchemaTypesFieldDef Final field which stores configuration {@code extensions}.
     */
    private void addNodeConstructor(
            Map<Class<?>, FieldDefinition> specFields,
            Map<String, FieldDefinition> fieldDefs,
            @Nullable FieldDefinition extensionSchemaTypesFieldDef
    ) {
        MethodDefinition ctor = innerNodeClassDef.declareConstructor(EnumSet.of(PUBLIC));

        BytecodeBlock ctorBody = ctor.getBody();

        // super();
        ctorBody
                .append(ctor.getThis())
                .invokeConstructor(InnerNode.class);

        // this._spec# = new MyConfigurationSchema();
        for (Map.Entry<Class<?>, FieldDefinition> e : specFields.entrySet()) {
            ctorBody.append(ctor.getThis().setField(e.getValue(), newInstance(e.getKey())));
        }

        for (Field schemaField : concat(schemaFields, publicExtensionFields, internalExtensionFields, polymorphicFields)) {
            if (!isNamedConfigValue(schemaField)) {
                continue;
            }

            FieldDefinition fieldDef = fieldDefs.get(fieldName(schemaField));

            // this.values = new NamedListNode<>(key, ValueNode::new, "polymorphicIdFieldName");
            ctorBody.append(setThisFieldCode(ctor, cgen.newNamedListNode(schemaField), fieldDef));
        }

        if (!extensions.isEmpty()) {
            assert extensionSchemaTypesFieldDef != null : innerNodeClassDef;

            // Class[] tmp;
            Variable tmpVar = ctor.getScope().createTempVariable(Class[].class);

            BytecodeBlock initExtensionSchemaTypesField = new BytecodeBlock();

            // tmp = new Class[size];
            initExtensionSchemaTypesField.append(tmpVar.set(newArray(type(Class[].class), extensions.size())));

            int i = 0;

            for (Class<?> extension : extensions) {
                // tmp[i] = InternalTableConfigurationSchema.class;
                initExtensionSchemaTypesField.append(set(
                        tmpVar,
                        constantInt(i++),
                        constantClass(extension)
                ));
            }

            // this._extensionSchemaTypes = tmp;
            initExtensionSchemaTypesField.append(setThisFieldCode(ctor, tmpVar, extensionSchemaTypesFieldDef));

            ctorBody.append(initExtensionSchemaTypesField);
        }

        // return;
        ctorBody.ret();
    }

    /**
     * Generates method with the same name as the {@link #internalIdField} field, that calls {@link InnerNode#internalId()}.
     */
    private void addNodeInternalIdMethod() {
        MethodDefinition internalIdMtd = innerNodeClassDef.declareMethod(
                EnumSet.of(PUBLIC),
                internalIdField.getName(),
                type(UUID.class)
        );

        // return this.internalId();
        internalIdMtd.getBody().append(internalIdMtd.getThis().invoke(INTERNAL_ID)).retObject();
    }

    /**
     * Implements getter method from {@code VIEW} interface. It returns field value, possibly unboxed or cloned, depending on type.
     *
     * @param classDef                     Node class definition.
     * @param schemaField                  Configuration Schema class field.
     * @param getFieldCodeFun              Function for creating bytecode to get a field, for example: {@code this.field} or {@code
     *                                     this.field.field}.
     * @param getPolymorphicTypeIdFieldFun Function for creating bytecode to get the field that stores the identifier of the polymorphic
     *                                     configuration instance is needed to add a polymorphicTypeId check, for example: {@code
     *                                     this.typeId} or {@code this.field.typeId}.
     */
    private void addNodeViewMethod(
            ClassDefinition classDef,
            Field schemaField,
            Function<MethodDefinition, BytecodeExpression> getFieldCodeFun,
            @Nullable Function<MethodDefinition, BytecodeExpression> getPolymorphicTypeIdFieldFun
    ) {
        Class<?> schemaFieldType = schemaField.getType();

        ParameterizedType returnType;

        SchemaClassesInfo schemaClassInfo = cgen.schemaInfo(schemaFieldType);

        // Return type is either corresponding VIEW type or the same type as declared in schema.
        if (isConfigValue(schemaField)) {
            returnType = typeFromJavaClassName(schemaClassInfo.viewClassName);
        } else if (isNamedConfigValue(schemaField)) {
            returnType = type(NamedListView.class);
        } else {
            returnType = type(schemaFieldType);
        }

        String fieldName = schemaField.getName();

        MethodDefinition viewMtd = classDef.declareMethod(
                EnumSet.of(PUBLIC),
                fieldName,
                returnType
        );

        BytecodeBlock bytecodeBlock = new BytecodeBlock();

        // result = this.field; OR this.field.field.
        bytecodeBlock.append(getFieldCodeFun.apply(viewMtd));

        if (schemaFieldType.isPrimitive()) {
            // result = Box.boxValue(result); // Unboxing.
            bytecodeBlock.invokeVirtual(
                    box(schemaFieldType),
                    schemaFieldType.getSimpleName() + "Value",
                    schemaFieldType
            );
        } else if (schemaFieldType.isArray()) {
            // result = result.clone();
            bytecodeBlock.invokeVirtual(schemaFieldType, "clone", Object.class).checkCast(schemaFieldType);
        } else if (isPolymorphicConfig(schemaFieldType) && isConfigValue(schemaField)) {
            // result = result.specificNode();
            bytecodeBlock.invokeVirtual(SPECIFIC_NODE_MTD);
        }

        // return result;
        bytecodeBlock.ret(schemaFieldType);

        enrichWithPolymorphicTypeCheck(schemaField, getPolymorphicTypeIdFieldFun, viewMtd, bytecodeBlock);
    }

    /**
     * Implements changer method from {@code CHANGE} interface.
     *
     * @param classDef    Node class definition.
     * @param schemaField Configuration schema class field.
     * @return List of method definition. First element is the "default" change method that accepts closure or a value. Second, optional
     *      element, is a change methods with no parameters.
     */
    private List<MethodDefinition> addNodeChangeMethod(
            ClassDefinition classDef,
            Field schemaField,
            Function<MethodDefinition, BytecodeExpression> getFieldCodeFun,
            BiFunction<MethodDefinition, BytecodeExpression, BytecodeExpression> setFieldCodeFun,
            @Nullable Function<MethodDefinition, BytecodeExpression> getPolymorphicTypeIdFieldFun
    ) {
        Class<?> schemaFieldType = schemaField.getType();

        MethodDefinition changeMtd = classDef.declareMethod(
                EnumSet.of(PUBLIC),
                changeMethodName(schemaField.getName()),
                classDef.getType(),
                // Change argument type is a Consumer for all inner or named fields.
                arg("change", isValue(schemaField) ? type(schemaFieldType) : type(Consumer.class))
        );

        MethodDefinition shortChangeMtd = null;

        // var change;
        BytecodeExpression changeVar = changeMtd.getScope().getVariable("change");

        BytecodeBlock bytecodeBlock = new BytecodeBlock();

        addAssertMutabilityMethodCall(classDef, changeMtd, bytecodeBlock);

        if (!schemaFieldType.isPrimitive()) {
            // Objects.requireNonNull(newValue, "change");
            bytecodeBlock.append(invokeStatic(REQUIRE_NON_NULL, changeVar, constantString("change")));
        }

        if (isValue(schemaField)) {
            BytecodeExpression newValue;

            if (schemaFieldType.isPrimitive()) {
                ParameterizedType type = type(box(schemaFieldType));

                // newValue = Box.valueOf(newValue); // Boxing.
                newValue = invokeStatic(type, "valueOf", type, singleton(changeVar));
            } else if (schemaFieldType.isArray()) {
                // newValue = newValue.clone();
                newValue = changeVar.invoke("clone", Object.class).cast(schemaFieldType);
            } else {
                newValue = changeVar;
            }

            // this.field = newValue;
            bytecodeBlock.append(setFieldCodeFun.apply(changeMtd, newValue));
        } else {
            shortChangeMtd = createShortChangeMethod(classDef, schemaField, getFieldCodeFun, setFieldCodeFun, getPolymorphicTypeIdFieldFun);

            // change.accept(this.field); OR change.accept(this.field.specificNode());
            bytecodeBlock.append(changeVar.invoke(ACCEPT, changeMtd.getThis().invoke(shortChangeMtd, List.of())));
        }

        // return this;
        bytecodeBlock.append(changeMtd.getThis()).retObject();

        enrichWithPolymorphicTypeCheck(schemaField, getPolymorphicTypeIdFieldFun, changeMtd, bytecodeBlock);

        if (shortChangeMtd == null) {
            return List.of(changeMtd);
        }

        return List.of(changeMtd, shortChangeMtd);
    }

    /**
     * Creates a "short" method to return a changed field instance. Name is the same as for {@code changeFoo(Consumer<FooChange> change)"}.
     * This method will be reused to create the value that is passed to "default" change method.
     * Method's signature is {@code FooChange changeFoo()}, it returns a mutable configuration value to be used for configuration updates.
     */
    private MethodDefinition createShortChangeMethod(
            ClassDefinition classDef,
            Field schemaField,
            Function<MethodDefinition, BytecodeExpression> getFieldCodeFun,
            BiFunction<MethodDefinition, BytecodeExpression, BytecodeExpression> setFieldCodeFun,
            @Nullable Function<MethodDefinition, BytecodeExpression> getPolymorphicTypeIdFieldFun
    ) {
        MethodDefinition shortChangeMtd = classDef.declareMethod(
                EnumSet.of(PUBLIC),
                changeMethodName(schemaField.getName()),
                isConfigValue(schemaField)
                    ? typeFromJavaClassName(changeClassName(schemaField.getType()))
                    : type(NamedListChange.class)
        );

        BytecodeBlock shortBytecodeBlock = new BytecodeBlock();

        addAssertMutabilityMethodCall(classDef, shortChangeMtd, shortBytecodeBlock);

        BytecodeExpression newValue;

        if (isConfigValue(schemaField)) {
            // newValue = (this.field == null) ? new ValueNode() : (ValueNode)this.field.copy();
            newValue = cgen.newOrCopyNodeField(schemaField, getFieldCodeFun.apply(shortChangeMtd));
        } else {
            assert isNamedConfigValue(schemaField) : schemaField;

            // newValue = (ValueNode)this.field.copy();
            newValue = cgen.copyNodeField(schemaField, getFieldCodeFun.apply(shortChangeMtd));
        }

        // this.field = newValue;
        shortBytecodeBlock.append(setFieldCodeFun.apply(shortChangeMtd, newValue));

        // this.field;
        BytecodeExpression getFieldCode = getFieldCodeFun.apply(shortChangeMtd);

        if (isPolymorphicConfig(schemaField.getType()) && isConfigValue(schemaField)) {
            // this.field.specificNode();
            getFieldCode = getFieldCode.invoke(SPECIFIC_NODE_MTD);
        }

        shortBytecodeBlock.append(getFieldCode).retObject();

        enrichWithPolymorphicTypeCheck(schemaField, getPolymorphicTypeIdFieldFun, shortChangeMtd, shortBytecodeBlock);

        shortChangeMtd.getBody().append(shortBytecodeBlock);

        return shortChangeMtd;
    }

    /**
     * Adds a call of {@link InnerNode#assertMutability()} to a body.
     * Should be done for every method that mutates the node instance.
     */
    private void addAssertMutabilityMethodCall(ClassDefinition classDef, MethodDefinition changeMtd, BytecodeBlock body) {
        if (classDef == innerNodeClassDef) {
            // this.assertMutability();
            body.append(changeMtd.getThis().invoke(ASSERT_MUTABILITY_MTD));
        } else {
            // this.this$0.assertMutability();
            body.append(
                    changeMtd.getThis()
                            .getField(classDef.getType(), "this$0", innerNodeClassDef.getType())
                            .invoke(ASSERT_MUTABILITY_MTD)
            );
        }
    }

    /**
     * Adds a check that expected polymorphic type ID matches the real one. Throws exception otherwise. Simply adds {@code bytecodeBlock}
     * into methods body if {@code getPolymorphicTypeIdFieldFun} is {@code null} (this means that class is not polymorphic).
     */
    private void enrichWithPolymorphicTypeCheck(
            Field schemaField,
            @Nullable Function<MethodDefinition, BytecodeExpression> getPolymorphicTypeIdFieldFun,
            MethodDefinition method,
            BytecodeBlock bytecodeBlock
    ) {
        if (getPolymorphicTypeIdFieldFun != null) {
            assert isPolymorphicConfigInstance(schemaField.getDeclaringClass()) : schemaField;

            // tmpVar = this.typeId; OR this.field.typeId.
            BytecodeExpression getPolymorphicTypeIdFieldValue = getPolymorphicTypeIdFieldFun.apply(method);
            String polymorphicInstanceId = polymorphicInstanceId(schemaField.getDeclaringClass());

            // if (!"first".equals(tmpVar)) throw Ex;
            // else change_value;
            method.getBody().append(
                    new IfStatement()
                            .condition(not(constantString(polymorphicInstanceId).invoke(STRING_EQUALS_MTD, getPolymorphicTypeIdFieldValue)))
                            .ifTrue(throwException(ConfigurationWrongPolymorphicTypeIdException.class, getPolymorphicTypeIdFieldValue))
                            .ifFalse(bytecodeBlock)
            );
        } else {
            method.getBody().append(bytecodeBlock);
        }
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
                EnumSet.of(PUBLIC, SYNTHETIC, BRIDGE),
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
     * @param fieldDefs                    Definitions for all fields in {@code schemaFields}.
     * @param polymorphicFieldsByExtension Fields of polymorphic configuration instances grouped by them.
     * @param polymorphicTypeIdFieldDef    Identification field for the polymorphic configuration instance.
     */
    private void addNodeTraverseChildrenMethod(
            Map<String, FieldDefinition> fieldDefs,
            Map<Class<?>, List<Field>> polymorphicFieldsByExtension,
            @Nullable FieldDefinition polymorphicTypeIdFieldDef
    ) {
        MethodDefinition traverseChildrenMtd = innerNodeClassDef.declareMethod(
                EnumSet.of(PUBLIC),
                "traverseChildren",
                type(void.class),
                arg("visitor", type(ConfigurationVisitor.class)),
                arg("includeInternal", type(boolean.class))
        ).addException(NoSuchElementException.class);

        BytecodeBlock mtdBody = traverseChildrenMtd.getBody();

        // invokeVisit for public (common in case polymorphic config) fields.
        for (Field schemaField : concat(schemaFields, publicExtensionFields)) {
            if (isInjectedName(schemaField)) {
                continue;
            }

            mtdBody.append(
                    invokeVisit(traverseChildrenMtd, schemaField, fieldDefs.get(schemaField.getName())).pop()
            );
        }

        if (!internalExtensionFields.isEmpty()) {
            BytecodeBlock includeInternalBlock = new BytecodeBlock();

            for (Field internalField : internalExtensionFields) {
                includeInternalBlock.append(
                        invokeVisit(traverseChildrenMtd, internalField, fieldDefs.get(internalField.getName())).pop()
                );
            }

            // if (includeInternal) invokeVisit for internal fields.
            mtdBody.append(
                    new IfStatement()
                            .condition(traverseChildrenMtd.getScope().getVariable("includeInternal"))
                            .ifTrue(includeInternalBlock)
            );
        } else if (!polymorphicFieldsByExtension.isEmpty()) {
            assert polymorphicTypeIdFieldDef != null : schemaClass.getName();
            assert schemaFields.stream().anyMatch(ConfigurationUtil::isPolymorphicId) :
                    "Missing field with @PolymorphicId in " + schemaClass.getName();

            // Create switch by polymorphicTypeIdField.
            StringSwitchBuilder switchBuilderTypeId = typeIdSwitchBuilder(traverseChildrenMtd, polymorphicTypeIdFieldDef);

            for (Map.Entry<Class<?>, List<Field>> e : polymorphicFieldsByExtension.entrySet()) {
                BytecodeBlock codeBlock = new BytecodeBlock();

                for (Field polymorphicField : e.getValue()) {
                    String fieldName = fieldName(polymorphicField);

                    // invokeVisit for specific polymorphic config fields.
                    codeBlock.append(invokeVisit(traverseChildrenMtd, polymorphicField, fieldDefs.get(fieldName)).pop());
                }

                switchBuilderTypeId.addCase(polymorphicInstanceId(e.getKey()), codeBlock);
            }

            // if (polymorphicTypeIdField != null) switch_by_polymorphicTypeIdField
            mtdBody.append(
                    new IfStatement()
                            .condition(isNotNull(getThisFieldCode(traverseChildrenMtd, polymorphicTypeIdFieldDef)))
                            .ifTrue(switchBuilderTypeId.build())
            );
        }

        mtdBody.ret();
    }

    /**
     * Implements {@link InnerNode#traverseChild(String, ConfigurationVisitor, boolean)} method.
     *
     * @param fieldDefs                    Definitions for all fields in {@code schemaFields}.
     * @param polymorphicFieldsByExtension Fields of polymorphic configuration instances grouped by them.
     * @param polymorphicTypeIdFieldDef    Identification field for the polymorphic configuration instance.
     */
    private void addNodeTraverseChildMethod(
            Map<String, FieldDefinition> fieldDefs,
            Map<Class<?>, List<Field>> polymorphicFieldsByExtension,
            @Nullable FieldDefinition polymorphicTypeIdFieldDef
    ) {
        MethodDefinition traverseChildMtd = innerNodeClassDef.declareMethod(
                EnumSet.of(PUBLIC),
                "traverseChild",
                type(Object.class),
                arg("key", type(String.class)),
                arg("visitor", type(ConfigurationVisitor.class)),
                arg("includeInternal", type(boolean.class))
        ).addException(NoSuchElementException.class);

        Variable keyVar = traverseChildMtd.getScope().getVariable("key");

        // Create switch for public (common in case polymorphic config) fields only.
        StringSwitchBuilder switchBuilder = new StringSwitchBuilder(traverseChildMtd.getScope()).expression(keyVar);

        for (Field schemaField : concat(schemaFields, publicExtensionFields)) {
            if (isInjectedName(schemaField)) {
                continue;
            }

            String fieldName = fieldName(schemaField);
            String publicName = publicName(schemaField);

            switchBuilder.addCase(
                    publicName,
                    invokeVisit(traverseChildMtd, schemaField, fieldDefs.get(fieldName)).retObject()
            );
        }

        if (!internalExtensionFields.isEmpty()) {
            // Create switch for public + internal fields.
            StringSwitchBuilder switchBuilderAllFields = new StringSwitchBuilder(traverseChildMtd.getScope())
                    .expression(keyVar)
                    .defaultCase(throwException(NoSuchElementException.class, keyVar));

            for (Field schemaField : concat(schemaFields, publicExtensionFields, internalExtensionFields)) {
                if (isInjectedName(schemaField)) {
                    continue;
                }

                String fieldName = fieldName(schemaField);
                String publicName = publicName(schemaField);

                switchBuilderAllFields.addCase(
                        publicName,
                        invokeVisit(traverseChildMtd, schemaField, fieldDefs.get(fieldName)).retObject()
                );
            }

            // if (includeInternal) switch_by_all_fields
            // else switch_only_public_fields
            traverseChildMtd.getBody().append(
                    new IfStatement()
                            .condition(traverseChildMtd.getScope().getVariable("includeInternal"))
                            .ifTrue(switchBuilderAllFields.build())
                            .ifFalse(switchBuilder.defaultCase(throwException(NoSuchElementException.class, keyVar)).build())
            );
        } else if (!polymorphicFieldsByExtension.isEmpty()) {
            assert polymorphicTypeIdFieldDef != null : innerNodeClassDef.getName();

            // Create switch by polymorphicTypeIdField.
            StringSwitchBuilder switchBuilderTypeId = typeIdSwitchBuilder(traverseChildMtd, polymorphicTypeIdFieldDef);

            for (Map.Entry<Class<?>, List<Field>> e : polymorphicFieldsByExtension.entrySet()) {
                // Create switch for specific polymorphic instance.
                StringSwitchBuilder switchBuilderPolymorphicExtension = new StringSwitchBuilder(traverseChildMtd.getScope())
                        .expression(keyVar)
                        .defaultCase(throwException(NoSuchElementException.class, keyVar));

                for (Field polymorphicField : e.getValue()) {
                    String fieldName = fieldName(polymorphicField);
                    String publicName = publicName(polymorphicField);

                    switchBuilderPolymorphicExtension.addCase(
                            publicName,
                            invokeVisit(traverseChildMtd, polymorphicField, fieldDefs.get(fieldName)).retObject()
                    );
                }

                switchBuilderTypeId.addCase(polymorphicInstanceId(e.getKey()), switchBuilderPolymorphicExtension.build());
            }

            // switch_by_common_fields
            // switch_by_polymorphicTypeIdField
            //      switch_by_polymorphic_0_fields
            //      switch_by_polymorphic_1_fields
            //      ...
            traverseChildMtd.getBody()
                    .append(switchBuilder.defaultCase(new BytecodeBlock()).build())
                    .append(switchBuilderTypeId.build());
        } else {
            traverseChildMtd.getBody()
                    .append(switchBuilder.defaultCase(throwException(NoSuchElementException.class, keyVar)).build());
        }
    }

    /**
     * Creates bytecode block that invokes one of {@link ConfigurationVisitor}'s methods.
     *
     * @param mtd         Method definition, either {@link InnerNode#traverseChildren(ConfigurationVisitor, boolean)} or {@link
     *                    InnerNode#traverseChild(String, ConfigurationVisitor, boolean)} defined in {@code *Node} class.
     * @param schemaField Configuration Schema field to visit.
     * @param fieldDef    Field definition from current class.
     * @return Bytecode block that invokes "visit*" method.
     */
    private BytecodeBlock invokeVisit(MethodDefinition mtd, Field schemaField, FieldDefinition fieldDef) {
        Method visitMethod;

        if (isValue(schemaField) || isPolymorphicId(schemaField)) {
            visitMethod = VISIT_LEAF;
        } else if (isConfigValue(schemaField)) {
            visitMethod = VISIT_INNER;
        } else {
            visitMethod = VISIT_NAMED;
        }

        FieldDefinition definition = fieldToFieldDefinitionMap.get(schemaField);

        return new BytecodeBlock().append(mtd.getScope().getVariable("visitor").invoke(
                visitMethod,
                getStatic(definition),
                constantString(publicName(schemaField)),
                mtd.getThis().getField(fieldDef)
        ));
    }

    /**
     * Implements {@link ConstructableTreeNode#construct(String, ConfigurationSource, boolean)} method.
     *
     * @param fieldDefs                    Definitions for all fields in {@code schemaFields}.
     * @param polymorphicFieldsByExtension Fields of polymorphic configuration instances grouped by them.
     * @param polymorphicTypeIdFieldDef    Identification field for the polymorphic configuration instance.
     * @param changePolymorphicTypeIdMtd   Method for changing the type of polymorphic configuration.
     */
    private void addNodeConstructMethod(
            Map<String, FieldDefinition> fieldDefs,
            Map<Class<?>, List<Field>> polymorphicFieldsByExtension,
            @Nullable FieldDefinition polymorphicTypeIdFieldDef,
            @Nullable MethodDefinition changePolymorphicTypeIdMtd
    ) {
        MethodDefinition constructMtd = innerNodeClassDef.declareMethod(
                EnumSet.of(PUBLIC),
                CONSTRUCT_MTD_NAME,
                type(void.class),
                arg("key", type(String.class)),
                arg("src", type(ConfigurationSource.class)),
                arg("includeInternal", type(boolean.class))
        ).addException(NoSuchElementException.class);

        Variable keyVar = constructMtd.getScope().getVariable("key");
        Variable srcVar = constructMtd.getScope().getVariable("src");

        addAssertMutabilityMethodCall(innerNodeClassDef, constructMtd, constructMtd.getBody());

        // Create switch for public (common in case polymorphic config) fields only.
        StringSwitchBuilder switchBuilder = new StringSwitchBuilder(constructMtd.getScope()).expression(keyVar);

        for (Field schemaField : concat(schemaFields, publicExtensionFields)) {
            if (isInjectedName(schemaField)) {
                continue;
            }

            String fieldName = fieldName(schemaField);
            String publicName = publicName(schemaField);

            FieldDefinition fieldDef = fieldDefs.get(fieldName);

            if (isPolymorphicId(schemaField)) {
                // src == null ? null : src.unwrap(FieldType.class);
                BytecodeExpression getTypeIdFromSrcVar = inlineIf(
                        isNull(srcVar),
                        constantNull(fieldDef.getType()),
                        srcVar.invoke(UNWRAP, constantClass(fieldDef.getType())).cast(fieldDef.getType())
                );

                // this.changePolymorphicTypeId(src == null ? null : src.unwrap(FieldType.class));

                switchBuilder.addCase(
                        publicName,
                        new BytecodeBlock()
                                .append(constructMtd.getThis())
                                .append(getTypeIdFromSrcVar)
                                .invokeVirtual(changePolymorphicTypeIdMtd)
                                .ret()
                );

                for (String legacyName : legacyNames(schemaField)) {
                    switchBuilder.addCase(
                            legacyName,
                            new BytecodeBlock()
                                    .append(constructMtd.getThis())
                                    .append(getTypeIdFromSrcVar)
                                    .invokeVirtual(changePolymorphicTypeIdMtd)
                                    .ret()
                    );
                }
            } else {
                switchBuilder.addCase(
                        publicName,
                        treatSourceForConstruct(constructMtd, schemaField, fieldDef).ret()
                );

                for (String legacyName : legacyNames(schemaField)) {
                    switchBuilder.addCase(
                            legacyName,
                            treatSourceForConstruct(constructMtd, schemaField, fieldDef).ret()
                    );
                }
            }
        }

        if (!internalExtensionFields.isEmpty()) {
            // Create switch for public + internal fields.
            StringSwitchBuilder switchBuilderAllFields = new StringSwitchBuilder(constructMtd.getScope())
                    .expression(keyVar)
                    .defaultCase(throwException(NoSuchElementException.class, keyVar));

            for (Field schemaField : concat(schemaFields, publicExtensionFields, internalExtensionFields)) {
                if (isInjectedName(schemaField)) {
                    continue;
                }

                String fieldName = fieldName(schemaField);
                String publicName = publicName(schemaField);

                switchBuilderAllFields.addCase(
                        publicName,
                        treatSourceForConstruct(constructMtd, schemaField, fieldDefs.get(fieldName)).ret()
                );

                for (String legacyName : legacyNames(schemaField)) {
                    switchBuilderAllFields.addCase(
                            legacyName,
                            treatSourceForConstruct(constructMtd, schemaField, fieldDefs.get(fieldName)).ret()
                    );
                }
            }

            // if (includeInternal) switch_by_all_fields
            // else switch_only_public_fields
            constructMtd.getBody().append(
                    new IfStatement().condition(constructMtd.getScope().getVariable("includeInternal"))
                            .ifTrue(switchBuilderAllFields.build())
                            .ifFalse(switchBuilder.defaultCase(throwException(NoSuchElementException.class, keyVar)).build())
            ).ret();
        } else if (!polymorphicFieldsByExtension.isEmpty()) {
            assert polymorphicTypeIdFieldDef != null : innerNodeClassDef.getName();

            // Create switch by polymorphicTypeIdField.
            StringSwitchBuilder switchBuilderTypeId = typeIdSwitchBuilder(constructMtd, polymorphicTypeIdFieldDef);

            for (Map.Entry<Class<?>, List<Field>> e : polymorphicFieldsByExtension.entrySet()) {
                // Create switch for specific polymorphic instance.
                StringSwitchBuilder switchBuilderPolymorphicExtension = new StringSwitchBuilder(constructMtd.getScope())
                        .expression(keyVar)
                        .defaultCase(throwException(NoSuchElementException.class, keyVar));

                for (Field polymorphicField : e.getValue()) {
                    String fieldName = fieldName(polymorphicField);
                    String publicName = publicName(polymorphicField);

                    FieldDefinition fieldDef = fieldDefs.get(fieldName);

                    switchBuilderPolymorphicExtension.addCase(
                            publicName,
                            treatSourceForConstruct(constructMtd, polymorphicField, fieldDef).ret()
                    );

                    for (String legacyName : legacyNames(polymorphicField)) {
                        switchBuilderPolymorphicExtension.addCase(
                                legacyName,
                                treatSourceForConstruct(constructMtd, polymorphicField, fieldDef).ret()
                        );
                    }
                }

                switchBuilderTypeId.addCase(polymorphicInstanceId(e.getKey()), switchBuilderPolymorphicExtension.build());
            }

            // switch_by_common_fields
            // switch_by_polymorphicTypeIdField
            //      switch_by_polymorphic_0_fields
            //      switch_by_polymorphic_1_fields
            //      ...
            constructMtd.getBody()
                    .append(switchBuilder.defaultCase(new BytecodeBlock()).build())
                    .append(switchBuilderTypeId.build())
                    .ret();
        } else {
            constructMtd.getBody()
                    .append(switchBuilder.defaultCase(throwException(NoSuchElementException.class, keyVar)).build())
                    .ret();
        }
    }

    /**
     * Creates bytecode block that invokes of construct methods for {@link InnerNode#construct(String, ConfigurationSource, boolean)}.
     *
     * @param constructMtd   Method definition {@link InnerNode#construct(String, ConfigurationSource, boolean)} defined in {@code *Node}
     *                       class.
     * @param schemaField    Schema field.
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

        if (isValue(schemaField)) {
            // this.field = src == null ? null : src.unwrap(FieldType.class);
            codeBlock.append(thisVar.setField(schemaFieldDef, inlineIf(
                    isNull(srcVar),
                    constantNull(schemaFieldDef.getType()),
                    srcVar.invoke(UNWRAP, constantClass(schemaFieldDef.getType())).cast(schemaFieldDef.getType())
            )));
        } else if (isConfigValue(schemaField)) {
            BytecodeNode setField;

            ParameterizedType fieldDefType = schemaFieldDef.getType();

            if (isPolymorphicConfig(schemaField.getType())) {
                Field polymorphicIdField = polymorphicIdField(schemaField.getType());

                assert polymorphicIdField != null : schemaField.getType().getName();

                // this.field;
                BytecodeExpression thisField = getThisFieldCode(constructMtd, schemaFieldDef);

                // String tmpStr;
                Variable tmpStrVar = constructMtd.getScope().createTempVariable(String.class);

                // this.field = (FieldType) this.field.copy();
                // if(tmpStr != null) this.field.changeTypeId(tmpStr);
                BytecodeBlock copyWithChange = new BytecodeBlock()
                        .append(setThisFieldCode(constructMtd, thisField.invoke(COPY).cast(fieldDefType), schemaFieldDef))
                        .append(new IfStatement()
                                .condition(isNotNull(tmpStrVar))
                                .ifTrue(thisField.invoke(changeMethodName(polymorphicIdField.getName()), void.class, tmpStrVar))
                        );

                // this.field = new FieldType();
                // if(tmpStr != null) this.field.changeTypeId(tmpStr);
                // else {
                //      this.field.constructDefault("typeId");
                //      if(this.field.typeId == null) throw new IllegalStateException();
                // }
                BytecodeBlock newInstanceWithChange = new BytecodeBlock()
                        .append(setThisFieldCode(constructMtd, newInstance(fieldDefType), schemaFieldDef))
                        .append(new IfStatement()
                                .condition(isNotNull(tmpStrVar))
                                .ifTrue(thisField.invoke(changeMethodName(polymorphicIdField.getName()), void.class, tmpStrVar))
                                .ifFalse(new BytecodeBlock()
                                        .append(thisField.invoke(CONSTRUCT_DEFAULT_MTD, constantString(polymorphicIdField.getName())))
                                        .append(new IfStatement()
                                                .condition(isNull(thisField.getField(polymorphicIdField.getName(), String.class)))
                                                .ifTrue(throwException(
                                                        IllegalStateException.class,
                                                        constantString(polymorphicTypeNotDefinedErrorMessage(
                                                                polymorphicIdField))
                                                ))
                                        )
                                )
                        );

                // tmpStr = src.polymorphicTypeId("typeId");
                // if(this.field == null)
                setField = new BytecodeBlock()
                        .append(tmpStrVar.set(srcVar.invoke(POLYMORPHIC_TYPE_ID_MTD, constantString(polymorphicIdField.getName()))))
                        .append(new IfStatement()
                                .condition(isNull(thisField))
                                .ifTrue(newInstanceWithChange)
                                .ifFalse(copyWithChange)
                        );
            } else {
                // newValue = this.field == null ? new ValueNode() : field.copy());
                BytecodeExpression newValue = cgen.newOrCopyNodeField(schemaField, getThisFieldCode(constructMtd, schemaFieldDef));

                // this.field = newValue;
                setField = setThisFieldCode(constructMtd, newValue, schemaFieldDef);
            }

            if (containsNameAnnotation(schemaField)) {
                setField = new BytecodeBlock()
                        .append(setField)
                        .append(getThisFieldCode(constructMtd, schemaFieldDef).invoke(
                                SET_INJECTED_NAME_FIELD_VALUE_MTD,
                                constantString(schemaField.getAnnotation(Name.class).value())
                        ));
            }

            codeBlock.append(
                    new IfStatement()
                            .condition(isNull(srcVar))
                            .ifTrue(setThisFieldCode(constructMtd, constantNull(fieldDefType), schemaFieldDef))
                            .ifFalse(new BytecodeBlock()
                                    .append(setField)
                                    .append(srcVar.invoke(DESCEND, thisVar.getField(schemaFieldDef)))
                            )
            );
        } else {
            // this.field = src == null ? new NamedListNode<>(key, ValueNode::new, "polymorphicIdFieldName")
            // : src.descend(field = field.copy()));
            codeBlock.append(new IfStatement()
                    .condition(isNull(srcVar))
                    .ifTrue(setThisFieldCode(constructMtd, cgen.newNamedListNode(schemaField), schemaFieldDef))
                    .ifFalse(new BytecodeBlock()
                            .append(setThisFieldCode(
                                    constructMtd,
                                    thisVar.getField(schemaFieldDef).invoke(COPY).cast(schemaFieldDef.getType()),
                                    schemaFieldDef
                            )).append(srcVar.invoke(DESCEND, thisVar.getField(schemaFieldDef)))
                    )
            );
        }

        return codeBlock;
    }

    /**
     * Implements {@link InnerNode#constructDefault(String)} method.
     *
     * @param specFields                   Field definitions for the schema and its extensions: {@code _spec#}.
     * @param fieldDefs                    Definitions for all fields in {@code schemaFields}.
     * @param polymorphicFieldsByExtension Fields of polymorphic configuration instances grouped by them.
     * @param polymorphicTypeIdFieldDef    Identification field for the polymorphic configuration instance.
     */
    private void addNodeConstructDefaultMethod(
            Map<Class<?>, FieldDefinition> specFields,
            Map<String, FieldDefinition> fieldDefs,
            Map<Class<?>, List<Field>> polymorphicFieldsByExtension,
            @Nullable FieldDefinition polymorphicTypeIdFieldDef
    ) {
        MethodDefinition constructDfltMtd = innerNodeClassDef.declareMethod(
                EnumSet.of(PUBLIC),
                "constructDefault",
                type(void.class),
                arg("key", String.class)
        ).addException(NoSuchElementException.class);

        addAssertMutabilityMethodCall(innerNodeClassDef, constructDfltMtd, constructDfltMtd.getBody());

        Variable keyVar = constructDfltMtd.getScope().getVariable("key");

        // Create switch for public (common in case polymorphic config) + internal fields.
        StringSwitchBuilder switchBuilder = new StringSwitchBuilder(constructDfltMtd.getScope()).expression(keyVar);

        for (Field schemaField : concat(schemaFields, publicExtensionFields, internalExtensionFields)) {
            if (isInjectedName(schemaField)) {
                continue;
            }

            if (isValue(schemaField) || isPolymorphicId(schemaField)) {
                String fieldName = schemaField.getName();
                String publicName = publicName(schemaField);

                if (isValue(schemaField) && !hasDefault(schemaField)
                        || isPolymorphicId(schemaField) && !schemaField.getAnnotation(PolymorphicId.class).hasDefault()) {
                    // return;
                    switchBuilder.addCase(publicName, new BytecodeBlock().ret());

                    for (String legacyName : legacyNames(schemaField)) {
                        switchBuilder.addCase(legacyName, new BytecodeBlock().ret());
                    }
                } else {
                    FieldDefinition fieldDef = fieldDefs.get(fieldName);

                    Class<?> fieldType = schemaField.getDeclaringClass();

                    FieldDefinition specFieldDef = fieldType.isAnnotationPresent(AbstractConfiguration.class)
                            ? specFields.get(schemaClass)
                            : specFields.get(fieldType);

                    // this.field = spec_#.field;
                    switchBuilder.addCase(
                            publicName,
                            addNodeConstructDefault(constructDfltMtd, schemaField, fieldDef, specFieldDef).ret()
                    );

                    for (String legacyName : legacyNames(schemaField)) {
                        switchBuilder.addCase(
                                legacyName,
                                addNodeConstructDefault(constructDfltMtd, schemaField, fieldDef, specFieldDef).ret()
                        );
                    }
                }
            }
        }

        if (!polymorphicFieldsByExtension.isEmpty()) {
            // Create switch by polymorphicTypeIdField.
            StringSwitchBuilder switchBuilderTypeId = typeIdSwitchBuilder(constructDfltMtd, polymorphicTypeIdFieldDef);

            for (Map.Entry<Class<?>, List<Field>> e : polymorphicFieldsByExtension.entrySet()) {
                // Create switch for specific polymorphic instance.
                StringSwitchBuilder switchBuilderPolymorphicExtension = new StringSwitchBuilder(constructDfltMtd.getScope())
                        .expression(keyVar)
                        .defaultCase(throwException(NoSuchElementException.class, keyVar));

                for (Field polymorphicField : e.getValue()) {
                    if (isValue(polymorphicField)) {
                        String publicName = publicName(polymorphicField);

                        if (!hasDefault(polymorphicField)) {
                            // return;
                            switchBuilderPolymorphicExtension.addCase(publicName, new BytecodeBlock().ret());

                            for (String legacyName : legacyNames(polymorphicField)) {
                                switchBuilderPolymorphicExtension.addCase(legacyName, new BytecodeBlock().ret());
                            }
                        } else {
                            FieldDefinition fieldDef = fieldDefs.get(fieldName(polymorphicField));
                            FieldDefinition specFieldDef = specFields.get(polymorphicField.getDeclaringClass());

                            // this.field = spec_#.field;
                            switchBuilderPolymorphicExtension.addCase(
                                    publicName,
                                    addNodeConstructDefault(constructDfltMtd, polymorphicField, fieldDef, specFieldDef).ret()
                            );

                            for (String legacyName : legacyNames(polymorphicField)) {
                                switchBuilderPolymorphicExtension.addCase(
                                        legacyName,
                                        addNodeConstructDefault(constructDfltMtd, polymorphicField, fieldDef, specFieldDef).ret()
                                );
                            }
                        }
                    }
                }

                switchBuilderTypeId.addCase(
                        polymorphicInstanceId(e.getKey()),
                        switchBuilderPolymorphicExtension.build()
                );
            }

            // switch_by_common_fields
            // switch_by_polymorphicTypeIdField
            //      switch_by_polymorphic_0_fields
            //      switch_by_polymorphic_1_fields
            //      ...
            constructDfltMtd.getBody()
                    .append(switchBuilder.defaultCase(new BytecodeBlock()).build())
                    .append(switchBuilderTypeId.build())
                    .ret();
        } else {
            constructDfltMtd.getBody()
                    .append(switchBuilder.defaultCase(throwException(NoSuchElementException.class, keyVar)).build())
                    .ret();
        }
    }

    /**
     * Creates a bytecode block of code that sets the default value for a field from the schema for {@link
     * InnerNode#constructDefault(String)}.
     *
     * @param constructDfltMtd Method definition {@link InnerNode#constructDefault(String)} defined in {@code *Node} class.
     * @param schemaField      Schema field.
     * @param schemaFieldDef   Schema field definition.
     * @param specFieldDef     Definition of the schema field.: {@code _spec#}.
     * @return Bytecode block that sets the default value for a field from the schema.
     */
    private static BytecodeBlock addNodeConstructDefault(
            MethodDefinition constructDfltMtd,
            Field schemaField,
            FieldDefinition schemaFieldDef,
            FieldDefinition specFieldDef
    ) {
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
        if (schemaFieldType.isArray()) {
            defaultValue = defaultValue.invoke("clone", Object.class).cast(schemaFieldType);
        }

        // this.field = defaultValue;
        return new BytecodeBlock().append(thisVar.setField(schemaFieldDef, defaultValue));
    }

    /**
     * Adds method overrides {@link InnerNode#getInjectedNameFieldValue} and {@link InnerNode#setInjectedNameFieldValue}.
     *
     * @param injectedNameFieldDef Field definition with {@link InjectedName}.
     */
    private void addInjectedNameFieldMethods(FieldDefinition injectedNameFieldDef) {
        MethodDefinition getInjectedNameFieldValueMtd = innerNodeClassDef.declareMethod(
                EnumSet.of(PUBLIC),
                "getInjectedNameFieldValue",
                type(String.class)
        );

        getInjectedNameFieldValueMtd.getBody()
                .append(getThisFieldCode(getInjectedNameFieldValueMtd, injectedNameFieldDef))
                .retObject();

        MethodDefinition setInjectedNameFieldValueMtd = innerNodeClassDef.declareMethod(
                EnumSet.of(PUBLIC),
                "setInjectedNameFieldValue",
                type(void.class),
                arg("value", String.class)
        );

        addAssertMutabilityMethodCall(innerNodeClassDef, setInjectedNameFieldValueMtd, setInjectedNameFieldValueMtd.getBody());

        Variable valueVar = setInjectedNameFieldValueMtd.getScope().getVariable("value");

        setInjectedNameFieldValueMtd.getBody()
                .append(invokeStatic(REQUIRE_NON_NULL, valueVar, constantString("value")))
                .append(setThisFieldCode(
                        setInjectedNameFieldValueMtd,
                        valueVar,
                        injectedNameFieldDef
                )).ret();
    }

    private void implementInjectedValueFieldNameMethod(Field injectedValueField) {
        MethodDefinition method = innerNodeClassDef.declareMethod(
                EnumSet.of(PUBLIC),
                INJECTED_VALUE_FIELD_NAME_MTD_NAME,
                type(String.class)
        );

        method.getBody().append(constantString(publicName(injectedValueField))).retObject();
    }

    /**
     * Adds an override for the {@link InnerNode#isPolymorphic} method that returns {@code true}.
     */
    private void addIsPolymorphicMethod() {
        MethodDefinition mtd = innerNodeClassDef.declareMethod(
                EnumSet.of(PUBLIC),
                IS_POLYMORPHIC_MTD.getName(),
                type(boolean.class)
        );

        mtd.getBody()
                .push(true)
                .retBoolean();
    }

    /**
     * Adds an override for the {@link InnerNode#extensionSchemaTypes} method that returns field {@code schemaTypesFieldDef}.
     *
     * @param schemaTypesFieldDef Final field of {@link InnerNode}, which stores all schemes for internal configuration extensions.
     */
    private void addExtensionSchemaTypesMethod(
            FieldDefinition schemaTypesFieldDef
    ) {
        MethodDefinition mtd = innerNodeClassDef.declareMethod(
                EnumSet.of(PUBLIC),
                EXTENSION_SCHEMA_TYPES_MTD.getName(),
                type(Class[].class)
        );

        mtd.getBody()
                .append(getThisFieldCode(mtd, schemaTypesFieldDef))
                .retObject();
    }

    /**
     * Adds an override for the {@link InnerNode#extendsAbstractConfiguration()} method that returns {@code true}.
     */
    private void addIsExtendAbstractConfigurationMethod() {
        MethodDefinition mtd = innerNodeClassDef.declareMethod(
                EnumSet.of(PUBLIC),
                "extendsAbstractConfiguration",
                type(boolean.class)
        );

        mtd.getBody()
                .push(true)
                .retBoolean();
    }

    /**
     * Create a {@code *Node} for the polymorphic configuration instance schema.
     *
     * @param polymorphicExtension    Polymorphic configuration instance schema (child).
     * @param polymorphicFields       Schema fields of a polymorphic configuration instance {@code polymorphicExtension}.
     */
    private ClassDefinition createPolymorphicExtensionNodeClass(
            Class<?> polymorphicExtension,
            Collection<Field> polymorphicFields
    ) {
        SchemaClassesInfo schemaClassInfo = cgen.schemaInfo(schemaClass);
        SchemaClassesInfo polymorphicExtensionClassInfo = cgen.schemaInfo(polymorphicExtension);

        // Node class definition.
        ClassDefinition classDef = new ClassDefinition(
                EnumSet.of(PUBLIC, FINAL),
                internalName(polymorphicExtensionClassInfo.nodeClassName),
                type(Object.class),
                ArrayUtils.concat(nodeClassInterfaces(polymorphicExtension, Set.of()), type(ConstructableTreeNode.class))
        );

        // private final ParentNode this$0;
        FieldDefinition parentInnerNodeFieldDef = classDef.declareField(
                EnumSet.of(PRIVATE, FINAL),
                "this$0",
                typeFromJavaClassName(schemaClassInfo.nodeClassName)
        );

        // Constructor.
        MethodDefinition constructorMtd = classDef.declareConstructor(
                EnumSet.of(PUBLIC),
                arg("delegate", typeFromJavaClassName(schemaClassInfo.nodeClassName))
        );

        Variable delegateVar = constructorMtd.getScope().getVariable("delegate");

        // Constructor body.
        constructorMtd.getBody()
                .append(constructorMtd.getThis())
                .invokeConstructor(Object.class)
                .append(constructorMtd.getThis().setField(
                        parentInnerNodeFieldDef,
                        delegateVar
                ))
                .ret();

        Map<String, FieldDefinition> fieldDefs = innerNodeClassDef.getFields().stream()
                .collect(toMap(FieldDefinition::getName, identity()));

        // Creates method to get the internal id. Almost the same as regular view, but with method invocation instead of field access.
        if (internalIdField != null) {
            addNodeViewMethod(
                    classDef,
                    internalIdField,
                    viewMtd -> getThisFieldCode(viewMtd, parentInnerNodeFieldDef).invoke(INTERNAL_ID),
                    null
            );
        }

        // Creates view and change methods for parent schema.
        for (Field schemaField : schemaFields) {
            FieldDefinition schemaFieldDef = fieldDefs.get(fieldName(schemaField));

            addNodeViewMethod(
                    classDef,
                    schemaField,
                    viewMtd -> getThisFieldCode(viewMtd, parentInnerNodeFieldDef, schemaFieldDef),
                    null
            );

            // Read only.
            if (isReadOnly(schemaField)) {
                continue;
            }

            List<MethodDefinition> changeMethods = addNodeChangeMethod(
                    classDef,
                    schemaField,
                    changeMtd -> getThisFieldCode(changeMtd, parentInnerNodeFieldDef, schemaFieldDef),
                    (changeMtd, newValue) -> setThisFieldCode(changeMtd, newValue, parentInnerNodeFieldDef, schemaFieldDef),
                    null
            );

            // Only first element requires a bridge. Please refer to "addNodeChangeMethod" for explanation.
            addNodeChangeBridgeMethod(classDef, schemaClassInfo.changeClassName, changeMethods.get(0));
        }

        FieldDefinition polymorphicTypeIdFieldDef = fieldDefs.get(polymorphicIdField(schemaClass).getName());

        // Creates view and change methods for specific polymorphic instance schema.
        for (Field polymorphicField : polymorphicFields) {
            FieldDefinition polymorphicFieldDef = fieldDefs.get(fieldName(polymorphicField));

            addNodeViewMethod(
                    classDef,
                    polymorphicField,
                    viewMtd -> getThisFieldCode(viewMtd, parentInnerNodeFieldDef, polymorphicFieldDef),
                    viewMtd -> getThisFieldCode(viewMtd, parentInnerNodeFieldDef, polymorphicTypeIdFieldDef)
            );

            List<MethodDefinition> changeMethods = addNodeChangeMethod(
                    classDef,
                    polymorphicField,
                    changeMtd -> getThisFieldCode(changeMtd, parentInnerNodeFieldDef, polymorphicFieldDef),
                    (changeMtd, newValue) -> setThisFieldCode(changeMtd, newValue, parentInnerNodeFieldDef, polymorphicFieldDef),
                    changeMtd -> getThisFieldCode(changeMtd, parentInnerNodeFieldDef, polymorphicTypeIdFieldDef)
            );

            // Only first element requires a bridge. Please refer to "addNodeChangeMethod" for explanation.
            addNodeChangeBridgeMethod(classDef, polymorphicExtensionClassInfo.changeClassName, changeMethods.get(0));
        }

        ParameterizedType returnType = typeFromJavaClassName(schemaClassInfo.changeClassName);

        // Creates Node#convert(Class<T> changeClass).
        MethodDefinition convertByChangeClassMtd = classDef.declareMethod(
                EnumSet.of(PUBLIC),
                CONVERT_MTD_NAME,
                returnType,
                arg("changeClass", Class.class)
        );

        // return this.this$0.convert(changeClass);
        convertByChangeClassMtd.getBody()
                .append(getThisFieldCode(convertByChangeClassMtd, parentInnerNodeFieldDef))
                .append(convertByChangeClassMtd.getScope().getVariable("changeClass"))
                .invokeVirtual(innerNodeClassDef.getType(), CONVERT_MTD_NAME, returnType, type(Class.class))
                .retObject();

        // Creates Node#convert(String polymorphicId).
        MethodDefinition convertByPolymorphicTypeIdMtd = classDef.declareMethod(
                EnumSet.of(PUBLIC),
                CONVERT_MTD_NAME,
                returnType,
                arg("polymorphicTypeId", String.class)
        );

        // return this.this$0.convert(polymorphicTypeId);
        convertByPolymorphicTypeIdMtd.getBody()
                .append(getThisFieldCode(convertByPolymorphicTypeIdMtd, parentInnerNodeFieldDef))
                .append(convertByPolymorphicTypeIdMtd.getScope().getVariable("polymorphicTypeId"))
                .invokeVirtual(innerNodeClassDef.getType(), CONVERT_MTD_NAME, returnType, type(String.class))
                .retObject();

        // Creates ConstructableTreeNode#construct.
        MethodDefinition constructMtd = classDef.declareMethod(
                EnumSet.of(PUBLIC),
                CONSTRUCT_MTD_NAME,
                type(void.class),
                arg("key", type(String.class)),
                arg("src", type(ConfigurationSource.class)),
                arg("includeInternal", type(boolean.class))
        ).addException(NoSuchElementException.class);

        // return this.this$0.construct(key, src, includeInternal);
        constructMtd.getBody()
                .append(getThisFieldCode(constructMtd, parentInnerNodeFieldDef))
                .append(constructMtd.getScope().getVariable("key"))
                .append(constructMtd.getScope().getVariable("src"))
                .append(constructMtd.getScope().getVariable("includeInternal"))
                .invokeVirtual(
                        innerNodeClassDef.getType(),
                        CONSTRUCT_MTD_NAME,
                        type(void.class),
                        type(String.class), type(ConfigurationSource.class), type(boolean.class)
                )
                .ret();

        // Creates ConstructableTreeNode#copy.
        MethodDefinition copyMtd = classDef.declareMethod(
                EnumSet.of(PUBLIC),
                "copy",
                type(ConstructableTreeNode.class)
        );

        // return this.this$0.copy();
        copyMtd.getBody()
                .append(getThisFieldCode(copyMtd, parentInnerNodeFieldDef))
                .invokeVirtual(innerNodeClassDef.getType(), "copy", type(ConstructableTreeNode.class))
                .retObject();

        return classDef;
    }

    /**
     * Adds a {@link InnerNode#specificNode} override for the polymorphic configuration case.
     *
     * @param polymorphicTypeIdFieldDef Identification field for the polymorphic configuration instance.
     */
    private void addNodeSpecificNodeMethod(FieldDefinition polymorphicTypeIdFieldDef) {
        MethodDefinition specificNodeMtd = innerNodeClassDef.declareMethod(
                EnumSet.of(PUBLIC),
                SPECIFIC_NODE_MTD.getName(),
                type(Object.class)
        );

        StringSwitchBuilder switchBuilder = typeIdSwitchBuilder(specificNodeMtd, polymorphicTypeIdFieldDef);

        for (Class<?> polymorphicExtension : polymorphicExtensions) {
            SchemaClassesInfo polymorphicExtensionClassInfo = cgen.schemaInfo(polymorphicExtension);

            switchBuilder.addCase(
                    polymorphicInstanceId(polymorphicExtension),
                    newInstance(
                            typeFromJavaClassName(polymorphicExtensionClassInfo.nodeClassName),
                            specificNodeMtd.getThis()
                    ).ret()
            );
        }

        specificNodeMtd.getBody().append(switchBuilder.build());
    }

    /**
     * Adds a {@code *Node#convert(Class changeClass)} and {@code *Node#convert(String polymorphicTypeId)} for the polymorphic configuration
     * case.
     *
     * @param changePolymorphicTypeIdMtd Method for changing the type of polymorphic configuration.
     */
    private void addNodeConvertMethods(MethodDefinition changePolymorphicTypeIdMtd) {
        SchemaClassesInfo schemaClassInfo = cgen.schemaInfo(schemaClass);

        MethodDefinition convertByChangeClassMtd = innerNodeClassDef.declareMethod(
                EnumSet.of(PUBLIC),
                CONVERT_MTD_NAME,
                typeFromJavaClassName(schemaClassInfo.changeClassName),
                arg("changeClass", Class.class)
        );

        MethodDefinition convertByPolymorphicTypeIdMtd = innerNodeClassDef.declareMethod(
                EnumSet.of(PUBLIC),
                CONVERT_MTD_NAME,
                typeFromJavaClassName(schemaClassInfo.changeClassName),
                arg("polymorphicTypeId", String.class)
        );

        // changeClass.getName();
        BytecodeExpression changeClassName = convertByChangeClassMtd.getScope()
                .getVariable("changeClass")
                .invoke(CLASS_GET_NAME_MTD);

        StringSwitchBuilder switchByChangeClassBuilder = new StringSwitchBuilder(convertByChangeClassMtd.getScope())
                .expression(changeClassName)
                .defaultCase(throwException(ConfigurationWrongPolymorphicTypeIdException.class, changeClassName));

        Variable polymorphicTypeId = convertByPolymorphicTypeIdMtd.getScope()
                .getVariable("polymorphicTypeId");

        StringSwitchBuilder switchByPolymorphicTypeIdBuilder = new StringSwitchBuilder(convertByPolymorphicTypeIdMtd.getScope())
                .expression(polymorphicTypeId)
                .defaultCase(throwException(ConfigurationWrongPolymorphicTypeIdException.class, polymorphicTypeId));

        for (Class<?> polymorphicExtension : polymorphicExtensions) {
            SchemaClassesInfo polymorphicExtensionClassInfo = cgen.schemaInfo(polymorphicExtension);

            String polymorphicInstanceId = polymorphicInstanceId(polymorphicExtension);

            // case "HashIndexChange":
            //     this.changePolymorphicTypeId("hashIndex");
            //     return new HashIndexNode(this);
            switchByChangeClassBuilder.addCase(
                    polymorphicExtensionClassInfo.changeClassName,
                    new BytecodeBlock()
                            .append(constantString(polymorphicInstanceId))
                            .invokeVirtual(changePolymorphicTypeIdMtd)
                            .append(newInstance(
                                    typeFromJavaClassName(polymorphicExtensionClassInfo.nodeClassName),
                                    convertByChangeClassMtd.getThis()
                            ))
                            .retObject()
            );

            // case "hashIndex":
            //     this.changePolymorphicTypeId("hashIndex");
            //     return new HashIndexNode(this);
            switchByPolymorphicTypeIdBuilder.addCase(
                    polymorphicInstanceId,
                    new BytecodeBlock()
                            .append(constantString(polymorphicInstanceId))
                            .invokeVirtual(changePolymorphicTypeIdMtd)
                            .append(newInstance(
                                    typeFromJavaClassName(polymorphicExtensionClassInfo.nodeClassName),
                                    convertByPolymorphicTypeIdMtd.getThis()
                            ))
                            .retObject()
            );
        }

        convertByChangeClassMtd.getBody()
                .append(convertByChangeClassMtd.getThis())
                .append(switchByChangeClassBuilder.build())
                .ret();

        convertByPolymorphicTypeIdMtd.getBody()
                .append(convertByPolymorphicTypeIdMtd.getThis())
                .append(switchByPolymorphicTypeIdBuilder.build())
                .ret();
    }

    /**
     * Adds a {@code Node#changeTypeId} for the polymorphic configuration case.
     *
     * @param fieldDefs                 Definitions for all fields in {@code innerNodeClassDef}.
     * @param polymorphicTypeIdFieldDef Identification field for the polymorphic configuration instance.
     * @return Method definition.
     */
    private MethodDefinition addNodeChangePolymorphicTypeIdMethod(
            Map<String, FieldDefinition> fieldDefs,
            FieldDefinition polymorphicTypeIdFieldDef
    ) {
        MethodDefinition changePolymorphicTypeIdMtd = innerNodeClassDef.declareMethod(
                EnumSet.of(PUBLIC),
                changeMethodName(polymorphicTypeIdFieldDef.getName()),
                type(void.class),
                arg("typeId", String.class)
        );

        Variable typeIdVar = changePolymorphicTypeIdMtd.getScope().getVariable("typeId");

        StringSwitchBuilder switchBuilder = new StringSwitchBuilder(changePolymorphicTypeIdMtd.getScope())
                .expression(typeIdVar)
                .defaultCase(throwException(ConfigurationWrongPolymorphicTypeIdException.class, typeIdVar));

        for (Class<?> polymorphicExtension : polymorphicExtensions) {
            // Fields that need to be cleared when changing the type of the polymorphic configuration instance.
            Collection<Field> resetFields = polymorphicFields.stream()
                    .filter(f -> !polymorphicExtension.equals(f.getDeclaringClass()))
                    .collect(toList());

            // this.typeId = typeId;
            BytecodeBlock codeBlock = new BytecodeBlock()
                    .append(setThisFieldCode(changePolymorphicTypeIdMtd, typeIdVar, polymorphicTypeIdFieldDef));

            // Reset fields.
            for (Field resetField : resetFields) {
                FieldDefinition fieldDef = fieldDefs.get(fieldName(resetField));

                if (isValue(resetField) || isConfigValue(resetField)) {
                    // this.field = null;
                    codeBlock.append(setThisFieldCode(
                            changePolymorphicTypeIdMtd,
                            constantNull(fieldDef.getType()),
                            fieldDef
                    ));
                } else {
                    // this.field = new NamedListNode<>(key, ValueNode::new, "polymorphicIdFieldName");
                    codeBlock.append(setThisFieldCode(changePolymorphicTypeIdMtd, cgen.newNamedListNode(resetField), fieldDef));
                }
            }

            // ConfigurationUtil.addDefaults(this);
            codeBlock
                    .append(changePolymorphicTypeIdMtd.getThis())
                    .invokeStatic(ADD_DEFAULTS_MTD);

            switchBuilder.addCase(polymorphicInstanceId(polymorphicExtension), codeBlock);
        }

        // if(typeId.equals(this.typeId)) return;
        // else switch(typeId)...
        changePolymorphicTypeIdMtd.getBody()
                .append(typeIdVar)
                .append(getThisFieldCode(changePolymorphicTypeIdMtd, polymorphicTypeIdFieldDef))
                .append(
                        new IfStatement()
                                .condition(new BytecodeBlock().invokeVirtual(STRING_EQUALS_MTD))
                                .ifTrue(new BytecodeBlock().ret())
                                .ifFalse(switchBuilder.build().ret())
                );

        return changePolymorphicTypeIdMtd;
    }

    private static String polymorphicTypeNotDefinedErrorMessage(Field polymorphicIdField) {
        return "Polymorphic configuration type is not defined: "
                + polymorphicIdField.getDeclaringClass().getName()
                + ". See @" + PolymorphicConfig.class.getSimpleName() + " documentation.";
    }
}
