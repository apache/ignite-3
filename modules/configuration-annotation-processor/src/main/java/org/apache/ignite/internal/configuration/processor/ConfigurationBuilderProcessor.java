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

package org.apache.ignite.internal.configuration.processor;

import static javax.lang.model.element.Modifier.ABSTRACT;
import static javax.lang.model.element.Modifier.FINAL;
import static javax.lang.model.element.Modifier.PRIVATE;
import static javax.lang.model.element.Modifier.PUBLIC;
import static javax.lang.model.element.Modifier.STATIC;
import static org.apache.ignite.internal.configuration.processor.ConfigurationProcessorUtils.CONFIGURATION_SCHEMA_POSTFIX;
import static org.apache.ignite.internal.configuration.processor.ConfigurationProcessorUtils.capitalize;
import static org.apache.ignite.internal.configuration.processor.ConfigurationProcessorUtils.fields;
import static org.apache.ignite.internal.configuration.processor.ConfigurationProcessorUtils.getBuilderImplName;
import static org.apache.ignite.internal.configuration.processor.ConfigurationProcessorUtils.getBuilderName;
import static org.apache.ignite.internal.configuration.processor.ConfigurationProcessorUtils.getChangeName;
import static org.apache.ignite.internal.configuration.processor.ConfigurationProcessorUtils.getViewName;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.ArrayType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import org.apache.ignite.configuration.NamedListChange;
import org.apache.ignite.configuration.annotation.AbstractConfiguration;
import org.apache.ignite.configuration.annotation.ConfigurationExtension;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.InjectedName;
import org.apache.ignite.configuration.annotation.InternalId;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.PolymorphicConfigInstance;
import org.apache.ignite.configuration.annotation.PolymorphicId;
import org.apache.ignite.configuration.annotation.Value;
import org.jetbrains.annotations.Nullable;

class ConfigurationBuilderProcessor {
    private final ProcessingEnvironment processingEnv;

    ConfigurationBuilderProcessor(ProcessingEnvironment processingEnv) {
        this.processingEnv = processingEnv;
    }

    /**
     * Create Builder classes and methods.
     *
     * @param fields Collection of configuration fields.
     * @param schemaClassName Class name of schema.
     * @param extendBaseSchema {@code true} if extending base schema interfaces.
     * @param realSchemaClass Class descriptor.
     * @param isPolymorphicInstanceConfig Is an instance of polymorphic configuration.
     */
    void createBuilders(
            List<VariableElement> fields,
            ClassName schemaClassName,
            boolean extendBaseSchema,
            TypeElement realSchemaClass,
            boolean isPolymorphicInstanceConfig
    ) {
        ClassName changeClsName = getChangeName(schemaClassName);
        ClassName builderClsName = getBuilderName(schemaClassName);
        ClassName builderImplClsName = getBuilderImplName(schemaClassName);

        @Nullable TypeName changeBaseSchemaInterfaceType;
        @Nullable TypeName builderBaseSchemaInterfaceType;
        @Nullable TypeName builderImplBaseSchemaClassType;

        TypeElement superClass = superClass(realSchemaClass);

        boolean isSuperClassAbstractConfiguration = !isClass(superClass.asType(), Object.class)
                && superClass.getAnnotation(AbstractConfiguration.class) != null;

        if (extendBaseSchema || isSuperClassAbstractConfiguration) {
            ClassName superClassSchemaClassName = ClassName.get(superClass);

            changeBaseSchemaInterfaceType = getChangeName(superClassSchemaClassName);
            builderBaseSchemaInterfaceType = getBuilderName(superClassSchemaClassName);
            builderImplBaseSchemaClassType = getBuilderImplName(superClassSchemaClassName);
        } else {
            changeBaseSchemaInterfaceType = null;
            builderBaseSchemaInterfaceType = null;
            builderImplBaseSchemaClassType = null;
        }

        TypeSpec.Builder builderClsBuilder = TypeSpec.interfaceBuilder(builderClsName)
                .addModifiers(PUBLIC);

        if (builderBaseSchemaInterfaceType != null) {
            builderClsBuilder.addSuperinterface(builderBaseSchemaInterfaceType);
        }

        TypeSpec.Builder builderImplClsBuilder = TypeSpec.classBuilder(builderImplClsName)
                .addModifiers(PUBLIC)
                .addSuperinterface(builderClsName);

        if (builderImplBaseSchemaClassType != null) {
            builderImplClsBuilder.superclass(builderImplBaseSchemaClassType);
        }

        generateOverrides(realSchemaClass, builderClsName, builderClsBuilder, builderImplClsBuilder);

        List<FieldSpec> configFields = new ArrayList<>();
        List<FieldSpec> configListFields = new ArrayList<>();
        boolean hasValueFields = false;

        ClassName consumerClsName = ClassName.get(Consumer.class);

        for (VariableElement field : fields) {
            // Read only.
            if (field.getAnnotation(PolymorphicId.class) != null || field.getAnnotation(InjectedName.class) != null
                    || field.getAnnotation(InternalId.class) != null) {
                continue;
            }

            Value valAnnotation = field.getAnnotation(Value.class);

            String fieldName = field.getSimpleName().toString();
            TypeMirror schemaFieldType = field.asType();
            TypeName schemaFieldTypeName = TypeName.get(schemaFieldType);

            boolean namedListField = field.getAnnotation(NamedConfigValue.class) != null;

            builderClsBuilder.addMethod(createSetMethodBuilder(field, builderClsName)
                    .addModifiers(ABSTRACT)
                    .build());

            MethodSpec.Builder setImplMtdBuilder = createSetMethodBuilder(field, builderClsName)
                    .addAnnotation(Override.class);
            String setParameterName = setParameterName(field);

            if (valAnnotation != null) {
                hasValueFields = true;
                String changeMtdName = "change" + capitalize(fieldName);
                setImplMtdBuilder.addStatement("changes.add(change -> change.$L($L))", changeMtdName, fieldName);

                if (schemaFieldType.getKind() == TypeKind.ARRAY) {
                    setImplMtdBuilder.varargs(true);
                }
            } else {
                ClassName builderImplFieldType = getBuilderImplName((ClassName) schemaFieldTypeName);
                if (namedListField) {
                    ParameterizedTypeName mapOfBuilders = ParameterizedTypeName.get(
                            ClassName.get(Map.class),
                            ClassName.get(String.class),
                            builderImplFieldType
                    );
                    FieldSpec configList = FieldSpec.builder(mapOfBuilders, fieldName, PRIVATE, FINAL)
                            .initializer("new $T<>()", ClassName.get(LinkedHashMap.class))
                            .build();
                    configListFields.add(configList);
                    builderImplClsBuilder.addField(configList);

                    setImplMtdBuilder.addStatement("this.$N.put(key, ($T) $L)", configList, builderImplFieldType, setParameterName);
                } else {
                    FieldSpec configValue = FieldSpec.builder(builderImplFieldType, fieldName, PRIVATE).build();
                    configFields.add(configValue);
                    builderImplClsBuilder.addField(configValue);

                    setImplMtdBuilder.addStatement("this.$L = ($T) $L", fieldName, builderImplFieldType, fieldName);
                }
            }

            setImplMtdBuilder
                    .addStatement("return this")
                    .returns(builderClsName);
            builderImplClsBuilder.addMethod(setImplMtdBuilder.build());
        }

        MethodSpec.Builder createMtd = MethodSpec.methodBuilder("create")
                .addModifiers(PUBLIC, STATIC)
                .returns(builderClsName)
                .addStatement("return new $T()", builderImplClsName);
        builderClsBuilder.addMethod(createMtd.build());

        if (hasValueFields) {
            TypeName changeConsumer = ParameterizedTypeName.get(consumerClsName, changeClsName);
            TypeName listOfConsumers = ParameterizedTypeName.get(ClassName.get(List.class), changeConsumer);
            FieldSpec.Builder changesFld = FieldSpec.builder(listOfConsumers, "changes", PRIVATE, FINAL)
                    .initializer("new $T<>()", ClassName.get(ArrayList.class));
            builderImplClsBuilder.addField(changesFld.build());
        }

        TypeName changeTypeName = changeBaseSchemaInterfaceType != null ? changeBaseSchemaInterfaceType : changeClsName;

        MethodSpec.Builder changeImplMtdBuilder = MethodSpec.methodBuilder("change")
                .addModifiers(PUBLIC)
                .addParameter(changeTypeName, "change");

        String variableName;
        if (builderImplBaseSchemaClassType != null) {
            changeImplMtdBuilder.addStatement("super.change(change)");
            variableName = getVariableName(changeClsName.simpleName());
            boolean needLocalVariable = hasValueFields || !configFields.isEmpty() || !configListFields.isEmpty();
            CodeBlock.Builder convertStatement = CodeBlock.builder();
            if (needLocalVariable) {
                convertStatement.add("var $L = ", variableName);
            }
            if (isPolymorphicInstanceConfig) {
                convertStatement.add("change.convert($T.class)", changeClsName);
            } else {
                convertStatement.add("($T) change", changeClsName);
            }
            changeImplMtdBuilder.addStatement(convertStatement.build());
        } else {
            variableName = "change";
        }

        if (hasValueFields) {
            changeImplMtdBuilder.addStatement("changes.forEach(consumer -> consumer.accept($L))", variableName);
        }

        configFields.forEach(configField -> {
            changeImplMtdBuilder.beginControlFlow("if ($N != null)", configField);
            changeImplMtdBuilder.addStatement("$L.change$L($N::change)", variableName, capitalize(configField.name), configField);
            changeImplMtdBuilder.endControlFlow();
        });

        configListFields.forEach(configListField -> {
            changeImplMtdBuilder.addStatement(
                    "var $NChange = $L.change$L()",
                    configListField,
                    variableName,
                    capitalize(configListField.name)
            );
            changeImplMtdBuilder.addStatement(
                    "$N.forEach((key, it) -> $NChange.create(key, it::change))",
                    configListField,
                    configListField
            );
        });

        builderImplClsBuilder.addMethod(changeImplMtdBuilder.build());

        buildClass(builderClsName.packageName(), builderClsBuilder.build());
        buildClass(builderImplClsName.packageName(), builderImplClsBuilder.build());
    }

    private void buildClass(String packageName, TypeSpec cls) {
        ConfigurationProcessorUtils.buildClass(processingEnv, packageName, cls);
    }

    /**
     * Checks that the type of the field with {@link Value} is valid: primitive, {@link String}, or {@link UUID} (or an array of one of
     * these).
     *
     * @param type Field type with {@link Value}.
     * @return {@code True} if the field type is valid.
     */
    private boolean isValidValueAnnotationFieldType(TypeMirror type) {
        if (type.getKind() == TypeKind.ARRAY) {
            type = ((ArrayType) type).getComponentType();
        }

        if (type.getKind().isPrimitive()) {
            return true;
        }

        return isClass(type, String.class) || isClass(type, UUID.class);
    }

    /**
     * Getting a superclass.
     *
     * @param clazz Class type.
     */
    private TypeElement superClass(TypeElement clazz) {
        return processingEnv.getElementUtils().getTypeElement(clazz.getSuperclass().toString());
    }

    /**
     * Check if a {@code type} is a {@code clazz} Class.
     */
    private boolean isClass(TypeMirror type, Class<?> clazz) {
        TypeMirror classType = processingEnv
                .getElementUtils()
                .getTypeElement(clazz.getCanonicalName())
                .asType();

        return classType.equals(type);
    }

    private static String setMethodName(VariableElement field) {
        String fieldName = field.getSimpleName().toString();
        boolean namedListField = field.getAnnotation(NamedConfigValue.class) != null;
        return namedListField ? "add" + capitalize(setParameterName(field)) : fieldName;
    }

    private static String setParameterName(VariableElement field) {
        String fieldName = field.getSimpleName().toString();
        boolean namedListField = field.getAnnotation(NamedConfigValue.class) != null;
        return namedListField && fieldName.endsWith("s")
                ? fieldName.substring(0, fieldName.length() - 1)
                : fieldName;
    }

    private static String getVariableName(String name) {
        return name.substring(0, 1).toLowerCase() + name.substring(1);
    }

    private boolean isSuperClassAbstractConfiguration(TypeElement superClass) {
        return !isClass(superClass.asType(), Object.class) && superClass.getAnnotation(AbstractConfiguration.class) != null;
    }

    private static boolean isExtendingBaseSchema(TypeElement clazz) {
        // Is root of the configuration.
        boolean isRootConfig = clazz.getAnnotation(ConfigurationRoot.class) != null;

        // Is the extending configuration.
        boolean isExtendingConfig = clazz.getAnnotation(ConfigurationExtension.class) != null;

        // Is an instance of a polymorphic configuration.
        boolean isPolymorphicInstance = clazz.getAnnotation(PolymorphicConfigInstance.class) != null;

        return (isExtendingConfig && !isRootConfig) || isPolymorphicInstance;
    }

    /**
     * Generates methods which override super class's setters and change return type to this builder class for proper chaining.
     *
     * @param realSchemaClass Class descriptor.
     * @param builderClsName Builder interface name.
     * @param builderClsBuilder Builder interface builder.
     * @param builderImplClsBuilder BuilderImpl class builder.
     */
    private void generateOverrides(
            TypeElement realSchemaClass,
            ClassName builderClsName,
            TypeSpec.Builder builderClsBuilder,
            TypeSpec.Builder builderImplClsBuilder
    ) {
        List<VariableElement> superFields = collectSuperFields(realSchemaClass);
        for (VariableElement field : superFields) {
            builderClsBuilder.addMethod(createSetMethodBuilder(field, builderClsName)
                    .addModifiers(ABSTRACT)
                    .addAnnotation(Override.class)
                    .build());

            String setMethodName = setMethodName(field);
            String setParameterName = setParameterName(field);
            boolean namedListField = field.getAnnotation(NamedConfigValue.class) != null;
            MethodSpec.Builder setMethodBuilder = createSetMethodBuilder(field, builderClsName)
                    .addAnnotation(Override.class);
            if (namedListField) {
                setMethodBuilder.addStatement("super.$L(key, $L)", setMethodName, setParameterName);
            } else {
                setMethodBuilder.addStatement("super.$L($L)", setMethodName, setParameterName);
            }

            builderImplClsBuilder.addMethod(setMethodBuilder
                    .addStatement("return this")
                    .build());
        }
    }

    /**
     * Collects writable fields from super classes.
     *
     * @param clazz Type element.
     * @return List of fields.
     */
    private List<VariableElement> collectSuperFields(TypeElement clazz) {
        List<VariableElement> superFields = new ArrayList<>();

        TypeElement superClass = superClass(clazz);

        while (isExtendingBaseSchema(clazz) || isSuperClassAbstractConfiguration(superClass)) {
            List<VariableElement> fields = fields(superClass);
            for (VariableElement field : fields) {
                // Writable field.
                if (field.getAnnotation(PolymorphicId.class) == null
                        && field.getAnnotation(InjectedName.class) == null
                        && field.getAnnotation(InternalId.class) == null
                ) {
                    superFields.add(field);
                }
            }
            clazz = superClass;
            superClass = superClass(clazz);
        }
        return superFields;
    }

    private MethodSpec.Builder createSetMethodBuilder(VariableElement field, TypeName returnTypeName) {
        Value valAnnotation = field.getAnnotation(Value.class);

        TypeMirror schemaFieldType = field.asType();
        TypeName schemaFieldTypeName = TypeName.get(schemaFieldType);

        boolean leafField = isValidValueAnnotationFieldType(schemaFieldType)
                || !((ClassName) schemaFieldTypeName).simpleName().contains(CONFIGURATION_SCHEMA_POSTFIX);

        boolean namedListField = field.getAnnotation(NamedConfigValue.class) != null;

        TypeName viewFieldType =
                leafField ? schemaFieldTypeName : getViewName((ClassName) schemaFieldTypeName);

        TypeName changeFieldType =
                leafField ? schemaFieldTypeName : getChangeName((ClassName) schemaFieldTypeName);

        TypeName builderFieldType =
                leafField ? schemaFieldTypeName : getBuilderName((ClassName) schemaFieldTypeName);

        if (namedListField) {
            changeFieldType = ParameterizedTypeName.get(
                    ClassName.get(NamedListChange.class),
                    viewFieldType,
                    changeFieldType
            );
        }

        String setMethodName = setMethodName(field);
        String setParameterName = setParameterName(field);
        MethodSpec.Builder setMtdBuilder = MethodSpec.methodBuilder(setMethodName)
                .addModifiers(PUBLIC)
                .returns(returnTypeName);

        if (namedListField) {
            setMtdBuilder.addParameter(String.class, "key");
        }
        if (valAnnotation != null) {
            if (field.asType().getKind() == TypeKind.ARRAY) {
                setMtdBuilder.varargs(true);
            }

            setMtdBuilder.addParameter(changeFieldType, setParameterName);
        } else {
            setMtdBuilder.addParameter(builderFieldType, setParameterName);
        }

        return setMtdBuilder;
    }
}
