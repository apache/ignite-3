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

import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.bytecode.ParameterizedType.typeFromJavaClassName;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantNull;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantString;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.inlineIf;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.invokeDynamic;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.isNull;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.newInstance;
import static java.lang.invoke.MethodType.methodType;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.configuration.asm.AbstractAsmGenerator.COPY;
import static org.apache.ignite.internal.configuration.asm.AbstractAsmGenerator.LAMBDA_METAFACTORY;
import static org.apache.ignite.internal.configuration.asm.SchemaClassesInfo.changeClassName;
import static org.apache.ignite.internal.configuration.asm.SchemaClassesInfo.configurationClassName;
import static org.apache.ignite.internal.configuration.asm.SchemaClassesInfo.viewClassName;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.extensionsFields;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.isConfigValue;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.isNamedConfigValue;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.isPolymorphicConfig;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.isPolymorphicConfigInstance;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.isPolymorphicId;
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
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.ignite.configuration.ConfigurationWrongPolymorphicTypeIdException;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.annotation.AbstractConfiguration;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigurationExtension;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.PolymorphicConfig;
import org.apache.ignite.configuration.annotation.PolymorphicConfigInstance;
import org.apache.ignite.configuration.annotation.PolymorphicId;
import org.apache.ignite.internal.configuration.DynamicConfiguration;
import org.apache.ignite.internal.configuration.DynamicConfigurationChanger;
import org.apache.ignite.internal.configuration.TypeUtils;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.apache.ignite.internal.configuration.tree.NamedListNode;
import org.apache.ignite.internal.configuration.util.ConfigurationUtil;
import org.jetbrains.annotations.Nullable;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Type;

/**
 * This class is responsible for generating internal implementation classes for configuration schemas. It uses classes from {@code bytecode}
 * module to achieve this goal, like {@link ClassGenerator}, for example.
 */
public class ConfigurationAsmGenerator {
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
     * @param schemaExtensions            Extensions (public and internal) ({@link ConfigurationExtension}) of configuration schemas
     *                                    {@link ConfigurationRoot} and {@link Config}). Mapping: original schema -> extensions.
     * @param polymorphicSchemaExtensions Polymorphic extensions ({@link PolymorphicConfigInstance}) of configuration schemas ({@link
     *                                    PolymorphicConfig}). Mapping: original schema -> extensions.
     */
    public synchronized void compileRootSchema(
            Class<?> rootSchemaClass,
            Map<Class<?>, Set<Class<?>>> schemaExtensions,
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

            Set<Class<?>> extensions = schemaExtensions.getOrDefault(schemaClass, Set.of());
            Set<Class<?>> polymorphicExtensions = polymorphicSchemaExtensions.getOrDefault(schemaClass, Set.of());

            assert extensions.isEmpty() || polymorphicExtensions.isEmpty() :
                    "Configuration and polymorphic extensions are not allowed at the same time: " + schemaClass;

            if (isPolymorphicConfig(schemaClass) && polymorphicExtensions.isEmpty()) {
                throw new IllegalArgumentException(schemaClass
                        + " is polymorphic but polymorphic extensions are absent");
            }

            Class<?> schemaSuperClass = schemaClass.getSuperclass();

            List<Field> schemaFields = schemaSuperClass.isAnnotationPresent(AbstractConfiguration.class)
                    ? concat(schemaFields(schemaClass), schemaFields(schemaSuperClass))
                    : schemaFields(schemaClass);

            Set<Class<?>> publicExtensions = extensions.stream()
                    .filter(ConfigurationUtil::isPublicExtension)
                    .collect(toSet());

            Set<Class<?>> internalExtensions = extensions.stream()
                    .filter(ConfigurationUtil::isInternalExtension)
                    .collect(toSet());

            Collection<Field> publicExtensionFields = extensionsFields(publicExtensions, true);

            Collection<Field> internalExtensionFields = extensionsFields(internalExtensions, true);

            Collection<Field> polymorphicExtensionsFields = extensionsFields(polymorphicExtensions, false);

            Field internalIdField = internalIdField(schemaClass, extensions);

            for (Field schemaField : concat(schemaFields, publicExtensionFields, internalExtensionFields, polymorphicExtensionsFields)) {
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
                    extensions,
                    polymorphicExtensions,
                    schemaFields,
                    publicExtensionFields,
                    internalExtensionFields,
                    polymorphicExtensionsFields,
                    internalIdField
            ).generate());

            classDefs.addAll(new ConfigurationImplAsmGenerator(
                    this,
                    schemaClass,
                    extensions,
                    polymorphicExtensions,
                    schemaFields,
                    publicExtensionFields,
                    internalExtensionFields,
                    polymorphicExtensionsFields,
                    internalIdField
            ).generate());

            classDefs.addAll(new DirectProxyAsmGenerator(
                    this,
                    schemaClass,
                    extensions,
                    schemaFields,
                    publicExtensionFields,
                    internalExtensionFields,
                    internalIdField
            ).generate());
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
     * Replaces first letter in string with its upper-cased variant.
     *
     * @param name Some string.
     * @return Capitalized version of passed string.
     */
    private static String capitalize(String name) {
        return name.substring(0, 1).toUpperCase() + name.substring(1);
    }

    /**
     * Converts a public class name into an internal class name, replacing dots with slashes.
     *
     * @param className Class name (with package).
     * @return Internal class name.
     * @see Type#getInternalName(Class)
     */
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
