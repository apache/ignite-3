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
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantString;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.invokeDynamic;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.newArray;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.newInstance;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.set;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.internal.configuration.asm.ConfigurationAsmGenerator.fieldName;
import static org.apache.ignite.internal.configuration.asm.ConfigurationAsmGenerator.getThisFieldCode;
import static org.apache.ignite.internal.configuration.asm.ConfigurationAsmGenerator.internalName;
import static org.apache.ignite.internal.configuration.asm.ConfigurationAsmGenerator.setThisFieldCode;
import static org.apache.ignite.internal.configuration.asm.ConfigurationAsmGenerator.throwException;
import static org.apache.ignite.internal.configuration.asm.DirectProxyAsmGenerator.newDirectProxyLambda;
import static org.apache.ignite.internal.configuration.asm.SchemaClassesInfo.configurationClassName;
import static org.apache.ignite.internal.configuration.asm.SchemaClassesInfo.nodeClassName;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.isConfigValue;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.isInjectedName;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.isInternalId;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.isNamedConfigValue;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.isPolymorphicConfig;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.isPolymorphicConfigInstance;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.isPolymorphicId;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.isValue;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.polymorphicInstanceId;
import static org.apache.ignite.internal.util.ArrayUtils.nullOrEmpty;
import static org.apache.ignite.internal.util.CollectionUtils.concat;
import static org.objectweb.asm.Type.getMethodType;
import static org.objectweb.asm.Type.getType;

import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.bytecode.FieldDefinition;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.ParameterizedType;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.expression.BytecodeExpression;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import org.apache.ignite.configuration.ConfigurationProperty;
import org.apache.ignite.configuration.ConfigurationTree;
import org.apache.ignite.configuration.ConfigurationValue;
import org.apache.ignite.configuration.ConfigurationWrongPolymorphicTypeIdException;
import org.apache.ignite.configuration.NamedConfigurationTree;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.internal.configuration.ConfigurationNode;
import org.apache.ignite.internal.configuration.ConfigurationTreeWrapper;
import org.apache.ignite.internal.configuration.DynamicConfiguration;
import org.apache.ignite.internal.configuration.DynamicConfigurationChanger;
import org.apache.ignite.internal.configuration.DynamicProperty;
import org.apache.ignite.internal.configuration.NamedListConfiguration;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.jetbrains.annotations.Nullable;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Opcodes;

class ConfigurationImplAsmGenerator extends AbstractAsmGenerator {
    /** {@link DynamicConfiguration#DynamicConfiguration} constructor. */
    private static final Constructor<?> DYNAMIC_CONFIGURATION_CTOR;

    /** {@code DynamicConfiguration#add} method. */
    private static final Method DYNAMIC_CONFIGURATION_ADD_MTD;

    /** {@code ConfigurationNode#refreshValue} method. */
    private static final Method REFRESH_VALUE_MTD;

    /** {@code DynamicConfiguration#addMember} method. */
    private static final Method ADD_MEMBER_MTD;

    /** {@code DynamicConfiguration#removeMember} method. */
    private static final Method REMOVE_MEMBER_MTD;

    /** {@link DynamicConfiguration#specificConfigTree} method. */
    private static final Method SPECIFIC_CONFIG_TREE_MTD;

    /** Field name for method {@link DynamicConfiguration#extensionConfigTypes}. */
    private static final String EXTENSION_CONFIG_TYPES_FIELD_NAME = "_extensionConfigTypes";

    static {
        try {
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

            REFRESH_VALUE_MTD = ConfigurationNode.class.getDeclaredMethod("refreshValue");

            ADD_MEMBER_MTD = DynamicConfiguration.class.getDeclaredMethod("addMember", Map.class, ConfigurationProperty.class);

            REMOVE_MEMBER_MTD = DynamicConfiguration.class.getDeclaredMethod("removeMember", Map.class, ConfigurationProperty.class);

            SPECIFIC_CONFIG_TREE_MTD = DynamicConfiguration.class.getDeclaredMethod("specificConfigTree");
        } catch (NoSuchMethodException nsme) {
            throw new ExceptionInInitializerError(nsme);
        }
    }

    /** Class definition that extends the {@link DynamicConfiguration}. */
    private ClassDefinition cfgImplClassDef;

    ConfigurationImplAsmGenerator(
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
        assert cfgImplClassDef == null;

        List<ClassDefinition> classDefs = new ArrayList<>();

        classDefs.add(createCfgImplClass());

        for (Class<?> polymorphicExtension : polymorphicExtensions) {
            // Only the fields of a specific instance of a polymorphic configuration.
            Collection<Field> polymorphicFields = this.polymorphicFields.stream()
                    .filter(f -> f.getDeclaringClass() == polymorphicExtension)
                    .collect(toList());

            classDefs.add(createPolymorphicExtensionCfgImplClass(polymorphicExtension, polymorphicFields));
        }

        return classDefs;
    }

    /**
     * Construct a {@link DynamicConfiguration} definition for a configuration schema.
     *
     * @return Constructed {@link DynamicConfiguration} definition for the configuration schema.
     */
    private ClassDefinition createCfgImplClass() {
        SchemaClassesInfo schemaClassInfo = cgen.schemaInfo(schemaClass);

        // Configuration impl class definition.
        cfgImplClassDef = new ClassDefinition(
                EnumSet.of(PUBLIC, FINAL),
                internalName(schemaClassInfo.cfgImplClassName),
                type(DynamicConfiguration.class),
                cgen.configClassInterfaces(schemaClass, extensions)
        );

        Map<String, FieldDefinition> fieldDefs = new HashMap<>();

        // To store the id of the polymorphic configuration instance.
        FieldDefinition polymorphicTypeIdFieldDef = null;

        for (Field schemaField : concat(schemaFields, publicExtensionFields, internalExtensionFields, polymorphicFields)) {
            String fieldName = fieldName(schemaField);

            FieldDefinition fieldDef = addConfigurationImplField(schemaField, fieldName);

            fieldDefs.put(fieldName, fieldDef);

            if (isPolymorphicId(schemaField)) {
                polymorphicTypeIdFieldDef = fieldDef;
            }
        }

        if (internalIdField != null) {
            // Internal id dynamic property is stored as a regular field.
            String fieldName = internalIdField.getName();

            FieldDefinition fieldDef = addConfigurationImplField(internalIdField, fieldName);

            fieldDefs.put(fieldName, fieldDef);
        }

        FieldDefinition extensionConfigTypesFieldDef = null;

        if (!extensions.isEmpty()) {
            extensionConfigTypesFieldDef = cfgImplClassDef.declareField(
                    EnumSet.of(PRIVATE, FINAL),
                    EXTENSION_CONFIG_TYPES_FIELD_NAME,
                    Class[].class
            );
        }

        // Constructor
        addConfigurationImplConstructor(fieldDefs, extensionConfigTypesFieldDef);

        // org.apache.ignite.internal.configuration.DynamicProperty#directProxy
        addDirectProxyMethod(schemaClassInfo);

        // Getter for the internal id.
        if (internalIdField != null) {
            addConfigurationImplGetMethod(cfgImplClassDef, internalIdField, fieldDefs.get(internalIdField.getName()));
        }

        for (Field schemaField : concat(schemaFields, publicExtensionFields, internalExtensionFields)) {
            addConfigurationImplGetMethod(cfgImplClassDef, schemaField, fieldDefs.get(fieldName(schemaField)));
        }

        // org.apache.ignite.internal.configuration.DynamicConfiguration#configType
        addCfgImplConfigTypeMethod(typeFromJavaClassName(schemaClassInfo.cfgClassName));

        if (extensionConfigTypesFieldDef != null) {
            addCfgImplInternalConfigTypesMethod(cfgImplClassDef, extensionConfigTypesFieldDef);
        }

        if (!polymorphicExtensions.isEmpty()) {
            addCfgSpecificConfigTreeMethod(polymorphicTypeIdFieldDef);

            addCfgRemoveMembersMethod(fieldDefs, polymorphicTypeIdFieldDef);

            addCfgAddMembersMethod(fieldDefs, polymorphicTypeIdFieldDef);

            addCfgImplPolymorphicInstanceConfigTypeMethod(polymorphicTypeIdFieldDef);
        }

        return cfgImplClassDef;
    }

    /**
     * Declares a field that corresponds to configuration value. Depending on the schema, the following options are possible:
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
     * @param schemaField Configuration Schema class field.
     * @param fieldName   Field name, if {@code null} will be used {@link Field#getName}.
     * @return Declared field definition.
     */
    private FieldDefinition addConfigurationImplField(
            Field schemaField,
            String fieldName
    ) {
        ParameterizedType fieldType;

        if (isConfigValue(schemaField)) {
            fieldType = typeFromJavaClassName(cgen.schemaInfo(schemaField.getType()).cfgImplClassName);
        } else if (isNamedConfigValue(schemaField)) {
            fieldType = type(NamedListConfiguration.class);
        } else {
            fieldType = type(DynamicProperty.class);
        }

        return cfgImplClassDef.declareField(EnumSet.of(PUBLIC), fieldName, fieldType);
    }

    /**
     * Implements default constructor for the configuration class. It initializes all fields and adds them to members collection.
     *
     * @param fieldDefs Field definitions for all fields of configuration impl class.
     * @param extensionConfigTypesFieldDef Field definition for {@link DynamicConfiguration#extensionConfigTypes},
     *      {@code null} if there are no extensions.
     */
    private void addConfigurationImplConstructor(
            Map<String, FieldDefinition> fieldDefs,
            @Nullable FieldDefinition extensionConfigTypesFieldDef
    ) {
        MethodDefinition ctor = cfgImplClassDef.declareConstructor(
                EnumSet.of(PUBLIC),
                arg("prefix", List.class),
                arg("key", String.class),
                arg("rootKey", RootKey.class),
                arg("changer", DynamicConfigurationChanger.class),
                arg("listenOnly", boolean.class)
        );

        Variable rootKeyVar = ctor.getScope().getVariable("rootKey");
        Variable changerVar = ctor.getScope().getVariable("changer");
        Variable listenOnlyVar = ctor.getScope().getVariable("listenOnly");

        SchemaClassesInfo schemaClassInfo = cgen.schemaInfo(schemaClass);

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
        for (Field schemaField :
                concat(schemaFields, publicExtensionFields, internalExtensionFields, polymorphicFields, internalIdFieldAsList)) {
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
                SchemaClassesInfo fieldInfo = cgen.schemaInfo(schemaField.getType());

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
                    MethodDefinition newMtd = cfgImplClassDef.declareMethod(
                            EnumSet.of(PRIVATE, STATIC, SYNTHETIC),
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

        if (extensionConfigTypesFieldDef != null) {
            assert !extensions.isEmpty() : cfgImplClassDef;

            // Class[] tmp;
            Variable tmpVar = ctor.getScope().createTempVariable(Class[].class);

            BytecodeBlock initExtensionConfigTypesField = new BytecodeBlock();

            // tmp = new Class[size];
            initExtensionConfigTypesField.append(tmpVar.set(newArray(type(Class[].class), extensions.size())));

            int i = 0;

            for (Class<?> extension : extensions) {
                // tmp[i] = InternalTableConfiguration.class;
                initExtensionConfigTypesField.append(set(
                        tmpVar,
                        constantInt(i++),
                        constantClass(typeFromJavaClassName(configurationClassName(extension)))
                ));
            }

            // this._extensionConfigTypes = tmp;
            initExtensionConfigTypesField.append(setThisFieldCode(ctor, tmpVar, extensionConfigTypesFieldDef));

            ctorBody.append(initExtensionConfigTypesField);
        }

        ctorBody.ret();
    }

    /**
     * Generates {@link ConfigurationNode#directProxy()} method that returns new instance every time.
     *
     * @param schemaClassInfo Schema class info.
     */
    private void addDirectProxyMethod(SchemaClassesInfo schemaClassInfo) {
        MethodDefinition methodDef = cfgImplClassDef.declareMethod(
                EnumSet.of(PUBLIC), "directProxy", typeFromJavaClassName(schemaClassInfo.cfgClassName)
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
     * @param fieldDefs   A chain of field definitions to access. For example, for fields "a", "b" and "c" the access would be
     *      {@code this.a.b.c}.
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

        SchemaClassesInfo schemaClassInfo = cgen.schemaInfo(schemaFieldType);

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
                EnumSet.of(PUBLIC),
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
     * Add {@link DynamicConfiguration#configType} method implementation to the class.
     *
     * <p>It looks like the following code:
     * <pre><code>
     * public Class configType() {
     *     return RootConfiguration.class;
     * }
     * </code></pre>
     *
     * @param clazz Definition of the configuration interface, for example {@code RootConfiguration}.
     */
    private void addCfgImplConfigTypeMethod(ParameterizedType clazz) {
        cfgImplClassDef.declareMethod(EnumSet.of(PUBLIC), "configType", type(Class.class))
                .getBody()
                .append(constantClass(clazz))
                .retObject();
    }

    /**
     * Add {@link DynamicConfiguration#extensionConfigTypes} method implementation to the class.
     *
     * <p>It looks like the following code:
     * <pre><code>
     * public Class<?>[] internalConfigTypes() {
     *     return new Class<?>[]{FirstInternalTableConfiguration.class, SecondInternalTableConfiguration.class};
     * }
     * </code></pre>
     *
     * @param classDef Class definition.
     * @param extensionConfigTypesDef Definition of the field in which the interfaces of the internal configuration extensions are stored.
     */
    private static void addCfgImplInternalConfigTypesMethod(ClassDefinition classDef, FieldDefinition extensionConfigTypesDef) {
        MethodDefinition extensionConfigTypesMtd = classDef.declareMethod(EnumSet.of(PUBLIC), "extensionConfigTypes", type(Class[].class));

        extensionConfigTypesMtd
                .getBody()
                .append(getThisFieldCode(extensionConfigTypesMtd, extensionConfigTypesDef))
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
     * @param polymorphicTypeIdFieldDef Identification field for the polymorphic configuration instance.
     */
    private void addCfgImplPolymorphicInstanceConfigTypeMethod(FieldDefinition polymorphicTypeIdFieldDef) {
        MethodDefinition polymorphicInstanceConfigTypeMtd = cfgImplClassDef.declareMethod(
                EnumSet.of(PUBLIC),
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

        // tmpObj = this.refreshValue();
        // tmpStr = ((ConfigNode) tmpObj).typeId;
        // switch(tmpStr) ...
        polymorphicInstanceConfigTypeMtd.getBody()
                .append(tmpObjVar.set(thisVar.invoke(REFRESH_VALUE_MTD)))
                .append(tmpStrVar.set(tmpObjVar.cast(nodeType).getField(polymorphicTypeIdFieldDef.getName(), String.class)))
                .append(switchBuilder.build())
                .ret();
    }

    /**
     * Create a {@code *CfgImpl} for the polymorphic configuration instance schema.
     *
     * @param polymorphicExtension  Polymorphic configuration instance schema (child).
     * @param polymorphicFields     Schema fields of a polymorphic configuration instance {@code polymorphicExtension}.
     */
    private ClassDefinition createPolymorphicExtensionCfgImplClass(
            Class<?> polymorphicExtension,
            Collection<Field> polymorphicFields
    ) {
        SchemaClassesInfo schemaClassInfo = cgen.schemaInfo(schemaClass);
        SchemaClassesInfo polymorphicExtensionClassInfo = cgen.schemaInfo(polymorphicExtension);

        // Configuration impl class definition.
        ClassDefinition classDef = new ClassDefinition(
                EnumSet.of(PUBLIC, FINAL),
                internalName(polymorphicExtensionClassInfo.cfgImplClassName),
                type(ConfigurationTreeWrapper.class),
                cgen.configClassInterfaces(polymorphicExtension, Set.of())
        );

        // private final ParentCfgImpl this$0;
        FieldDefinition parentCfgImplFieldDef = classDef.declareField(
                EnumSet.of(PRIVATE, FINAL),
                "this$0",
                typeFromJavaClassName(schemaClassInfo.cfgImplClassName)
        );

        // Constructor.
        MethodDefinition constructorMtd = classDef.declareConstructor(
                EnumSet.of(PUBLIC),
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

        Map<String, FieldDefinition> fieldDefs = cfgImplClassDef.getFields().stream()
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
     * @param polymorphicTypeIdFieldDef Identification field for the polymorphic configuration instance.
     */
    private void addCfgSpecificConfigTreeMethod(FieldDefinition polymorphicTypeIdFieldDef) {
        MethodDefinition specificConfigMtd = cfgImplClassDef.declareMethod(
                EnumSet.of(PUBLIC),
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
                            typeFromJavaClassName(cgen.schemaInfo(polymorphicExtension).cfgImplClassName),
                            specificConfigMtd.getThis()
                    ).ret()
            );
        }

        // ConfigNode
        ParameterizedType nodeType = typeFromJavaClassName(cgen.schemaInfo(schemaClass).nodeClassName);

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
     * @param fieldDefs                 Field definitions for all fields of {@code cfgImplClassDef}.
     * @param polymorphicTypeIdFieldDef Identification field for the polymorphic configuration instance.
     */
    private void addCfgRemoveMembersMethod(Map<String, FieldDefinition> fieldDefs, FieldDefinition polymorphicTypeIdFieldDef) {
        MethodDefinition removeMembersMtd = cfgImplClassDef.declareMethod(
                EnumSet.of(PUBLIC),
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
        ParameterizedType nodeType = typeFromJavaClassName(cgen.schemaInfo(schemaClass).nodeClassName);

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
     * @param fieldDefs                 Field definitions for all fields of {@code cfgImplClassDef}.
     * @param polymorphicTypeIdFieldDef Identification field for the polymorphic configuration instance.
     */
    private void addCfgAddMembersMethod(
            Map<String, FieldDefinition> fieldDefs,
            FieldDefinition polymorphicTypeIdFieldDef
    ) {
        MethodDefinition removeMembersMtd = cfgImplClassDef.declareMethod(
                EnumSet.of(PUBLIC),
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
        ParameterizedType nodeType = typeFromJavaClassName(cgen.schemaInfo(schemaClass).nodeClassName);

        // tmpStr = ((ConfigNode) newValue).typeId;
        // switch(tmpStr) ...
        removeMembersMtd.getBody()
                .append(tmpStrVar.set(newValueVar.cast(nodeType).getField(polymorphicTypeIdFieldDef.getName(), String.class)))
                .append(switchBuilder.build())
                .ret();
    }
}
