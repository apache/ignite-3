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

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static javax.lang.model.element.Modifier.ABSTRACT;
import static javax.lang.model.element.Modifier.FINAL;
import static javax.lang.model.element.Modifier.PUBLIC;
import static javax.lang.model.element.Modifier.STATIC;
import static org.apache.ignite.internal.configuration.processor.ConfigurationProcessorUtils.CONFIGURATION_SCHEMA_POSTFIX;
import static org.apache.ignite.internal.configuration.processor.ConfigurationProcessorUtils.capitalize;
import static org.apache.ignite.internal.configuration.processor.ConfigurationProcessorUtils.collectFieldsWithAnnotation;
import static org.apache.ignite.internal.configuration.processor.ConfigurationProcessorUtils.fields;
import static org.apache.ignite.internal.configuration.processor.ConfigurationProcessorUtils.findFirstPresentAnnotation;
import static org.apache.ignite.internal.configuration.processor.ConfigurationProcessorUtils.getChangeName;
import static org.apache.ignite.internal.configuration.processor.ConfigurationProcessorUtils.getConfigurationInterfaceName;
import static org.apache.ignite.internal.configuration.processor.ConfigurationProcessorUtils.getViewName;
import static org.apache.ignite.internal.configuration.processor.ConfigurationProcessorUtils.joinSimpleName;
import static org.apache.ignite.internal.configuration.processor.ConfigurationProcessorUtils.simpleName;
import static org.apache.ignite.internal.util.ArrayUtils.nullOrEmpty;
import static org.apache.ignite.internal.util.CollectionUtils.concat;
import static org.apache.ignite.internal.util.CollectionUtils.difference;

import com.google.auto.service.AutoService;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import com.squareup.javapoet.TypeVariableName;
import com.squareup.javapoet.WildcardTypeName;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.Name;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.ArrayType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.tools.Diagnostic;
import org.apache.ignite.configuration.NamedConfigurationTree;
import org.apache.ignite.configuration.NamedListChange;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.PolymorphicChange;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.annotation.AbstractConfiguration;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.ConfigurationExtension;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.InjectedName;
import org.apache.ignite.configuration.annotation.InternalId;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.PolymorphicConfig;
import org.apache.ignite.configuration.annotation.PolymorphicConfigInstance;
import org.apache.ignite.configuration.annotation.PolymorphicId;
import org.apache.ignite.configuration.annotation.Secret;
import org.apache.ignite.configuration.annotation.Value;
import org.jetbrains.annotations.Nullable;

/**
 * Annotation processor that produces configuration classes.
 */
// TODO: IGNITE-17166 Split into classes/methods for regular/internal/polymorphic/abstract configuration
@AutoService(Processor.class)
public class ConfigurationProcessor extends AbstractProcessor {
    /** {@link RootKey} class name. */
    private static final ClassName ROOT_KEY_CLASSNAME = ClassName.get("org.apache.ignite.configuration", "RootKey");

    /** {@link PolymorphicChange} class name. */
    private static final ClassName POLYMORPHIC_CHANGE_CLASSNAME = ClassName.get(PolymorphicChange.class);

    /** Error format for the superclass missing annotation. */
    private static final String SUPERCLASS_MISSING_ANNOTATION_ERROR_FORMAT = "Superclass must have %s: %s";

    /** Error format for an empty field. */
    private static final String EMPTY_FIELD_ERROR_FORMAT = "Field %s cannot be empty: %s";

    /** Error format is that the field must be a specific class. */
    private static final String FIELD_MUST_BE_SPECIFIC_CLASS_ERROR_FORMAT = "%s %s.%s field must be a %s";

    private static final String SECRET_FIELD_MUST_BE_STRING = "%s.%s must be String. Only String field can be annotated with @Secret";

    /** {@inheritDoc} */
    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnvironment) {
        try {
            return process0(roundEnvironment);
        } catch (Throwable t) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            t.printStackTrace(pw);
            processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, "Failed to process configuration: " + sw);
        }
        return false;
    }

    /**
     * Processes a set of annotation types on type elements.
     *
     * @param roundEnvironment Processing environment.
     * @return Whether the set of annotation types are claimed by this processor.
     */
    private boolean process0(RoundEnvironment roundEnvironment) {
        Elements elementUtils = processingEnv.getElementUtils();

        // All classes annotated with {@link #supportedAnnotationTypes}.
        List<TypeElement> annotatedConfigs = roundEnvironment
                .getElementsAnnotatedWithAny(supportedAnnotationTypes())
                .stream()
                .filter(element -> element.getKind() == ElementKind.CLASS)
                .map(TypeElement.class::cast)
                .collect(toList());

        if (annotatedConfigs.isEmpty()) {
            return false;
        }

        ConfigurationBuilderProcessor configurationBuilderProcessor = new ConfigurationBuilderProcessor(processingEnv);

        for (TypeElement clazz : annotatedConfigs) {
            // Find all the fields of the schema.
            List<VariableElement> fields = fields(clazz);

            validateConfigurationSchemaClass(clazz, fields);

            // Get package name of the schema class
            String packageName = elementUtils.getPackageOf(clazz).getQualifiedName().toString();

            ClassName schemaClassName = ClassName.get(packageName, clazz.getSimpleName().toString());

            // Get name for generated configuration interface.
            ClassName configInterface = getConfigurationInterfaceName(schemaClassName);

            TypeSpec.Builder configurationInterfaceBuilder = TypeSpec.interfaceBuilder(configInterface)
                    .addModifiers(PUBLIC);

            for (VariableElement field : fields) {
                if (!field.getModifiers().contains(PUBLIC)) {
                    throw new ConfigurationProcessorException("Field " + clazz.getQualifiedName() + "." + field + " must be public");
                }

                final String fieldName = field.getSimpleName().toString();

                // Get configuration types (VIEW, CHANGE and so on)
                final TypeName interfaceGetMethodType = getInterfaceGetMethodType(field);

                if (field.getAnnotation(ConfigValue.class) != null) {
                    checkConfigField(field, ConfigValue.class);

                    checkMissingNameForInjectedName(field);
                }

                if (field.getAnnotation(NamedConfigValue.class) != null) {
                    checkConfigField(field, NamedConfigValue.class);
                }

                Value valueAnnotation = field.getAnnotation(Value.class);
                if (valueAnnotation != null) {
                    // Must be a primitive or an array of the primitives (including java.lang.String, java.util.UUID).
                    if (!isValidValueAnnotationFieldType(field.asType())) {
                        throw new ConfigurationProcessorException(String.format(
                                "%s %s.%s field must have one of the following types: "
                                        + "boolean, int, long, double, String, UUID or an array of aforementioned type.",
                                simpleName(Value.class),
                                clazz.getQualifiedName(),
                                field.getSimpleName()
                        ));
                    }
                }

                PolymorphicId polymorphicId = field.getAnnotation(PolymorphicId.class);
                if (polymorphicId != null) {
                    if (!isClass(field.asType(), String.class)) {
                        throw new ConfigurationProcessorException(String.format(
                                FIELD_MUST_BE_SPECIFIC_CLASS_ERROR_FORMAT,
                                simpleName(PolymorphicId.class),
                                clazz.getQualifiedName(),
                                field.getSimpleName(),
                                String.class.getSimpleName()
                        ));
                    }
                }

                if (field.getAnnotation(InternalId.class) != null) {
                    if (!isClass(field.asType(), UUID.class)) {
                        throw new ConfigurationProcessorException(String.format(
                                FIELD_MUST_BE_SPECIFIC_CLASS_ERROR_FORMAT,
                                simpleName(InternalId.class),
                                clazz.getQualifiedName(),
                                field.getSimpleName(),
                                UUID.class.getSimpleName()
                        ));
                    }
                }

                if (field.getAnnotation(Secret.class) != null) {
                    if (!isClass(field.asType(), String.class)) {
                        throw new ConfigurationProcessorException(
                                String.format(SECRET_FIELD_MUST_BE_STRING, clazz.getQualifiedName(), field.getSimpleName())
                        );
                    }
                }

                createGetters(configurationInterfaceBuilder, fieldName, interfaceGetMethodType);
            }

            // Is root of the configuration.
            boolean isRootConfig = clazz.getAnnotation(ConfigurationRoot.class) != null;

            // Is the extending configuration.
            boolean isExtendingConfig = clazz.getAnnotation(ConfigurationExtension.class) != null;

            // Is a polymorphic configuration.
            boolean isPolymorphicConfig = clazz.getAnnotation(PolymorphicConfig.class) != null;

            // Is an instance of a polymorphic configuration.
            boolean isPolymorphicInstance = clazz.getAnnotation(PolymorphicConfigInstance.class) != null;

            // Create VIEW and CHANGE classes.
            createPojoBindings(
                    fields,
                    schemaClassName,
                    configurationInterfaceBuilder,
                    (isExtendingConfig && !isRootConfig) || isPolymorphicInstance,
                    clazz,
                    isPolymorphicConfig,
                    isPolymorphicInstance
            );

            configurationBuilderProcessor.createBuilders(
                    fields,
                    schemaClassName,
                    (isExtendingConfig && !isRootConfig) || isPolymorphicInstance,
                    clazz,
                    isPolymorphicInstance
            );

            if (isRootConfig) {
                createRootKeyField(configInterface, configurationInterfaceBuilder, schemaClassName, clazz);
            }

            // Generates "public FooConfiguration directProxy();" in the configuration interface.
            configurationInterfaceBuilder.addMethod(
                    MethodSpec.methodBuilder("directProxy").addModifiers(PUBLIC, ABSTRACT).returns(configInterface).build()
            );

            // Write configuration interface.
            buildClass(packageName, configurationInterfaceBuilder.build());
        }

        return true;
    }

    private static void createRootKeyField(
            ClassName configInterface,
            TypeSpec.Builder configurationClassBuilder,
            ClassName schemaClassName,
            TypeElement realSchemaClass
    ) {
        ClassName viewClassName = getViewName(schemaClassName);

        ParameterizedTypeName fieldTypeName = ParameterizedTypeName.get(ROOT_KEY_CLASSNAME, configInterface, viewClassName);

        FieldSpec keyField = FieldSpec.builder(fieldTypeName, "KEY", PUBLIC, STATIC, FINAL)
                .initializer(
                        "new $T($T.class)",
                        ROOT_KEY_CLASSNAME,
                        realSchemaClass
                )
                .build();

        configurationClassBuilder.addField(keyField);
    }

    /**
     * Create getters for configuration class.
     *
     * @param configurationInterfaceBuilder Interface builder.
     * @param fieldName Field name.
     * @param interfaceGetMethodType Return type.
     */
    private static void createGetters(
            TypeSpec.Builder configurationInterfaceBuilder,
            String fieldName,
            TypeName interfaceGetMethodType
    ) {
        MethodSpec interfaceGetMethod = MethodSpec.methodBuilder(fieldName)
                .addModifiers(PUBLIC, ABSTRACT)
                .returns(interfaceGetMethodType)
                .build();

        configurationInterfaceBuilder.addMethod(interfaceGetMethod);
    }

    /**
     * Get types for configuration classes generation.
     *
     * @param field Field.
     * @return Bundle with all types for configuration
     */
    private static TypeName getInterfaceGetMethodType(VariableElement field) {
        TypeName interfaceGetMethodType = null;

        TypeName baseType = TypeName.get(field.asType());

        ConfigValue confAnnotation = field.getAnnotation(ConfigValue.class);
        if (confAnnotation != null) {
            interfaceGetMethodType = getConfigurationInterfaceName((ClassName) baseType);
        }

        NamedConfigValue namedConfigAnnotation = field.getAnnotation(NamedConfigValue.class);
        if (namedConfigAnnotation != null) {
            ClassName interfaceGetType = getConfigurationInterfaceName((ClassName) baseType);

            TypeName viewClassType = getViewName((ClassName) baseType);
            TypeName changeClassType = getChangeName((ClassName) baseType);

            interfaceGetMethodType = ParameterizedTypeName.get(
                    ClassName.get(NamedConfigurationTree.class),
                    interfaceGetType,
                    viewClassType,
                    changeClassType
            );
        }

        Value valueAnnotation = field.getAnnotation(Value.class);
        PolymorphicId polymorphicIdAnnotation = field.getAnnotation(PolymorphicId.class);
        InjectedName injectedNameAnnotation = field.getAnnotation(InjectedName.class);
        InternalId internalIdAnnotation = field.getAnnotation(InternalId.class);

        if (valueAnnotation != null || polymorphicIdAnnotation != null || injectedNameAnnotation != null
                || internalIdAnnotation != null) {
            // It is necessary to use class names without loading classes so that we won't
            // accidentally get NoClassDefFoundError
            ClassName confValueClass = ClassName.get("org.apache.ignite.configuration", "ConfigurationValue");

            TypeName genericType = baseType;

            if (genericType.isPrimitive()) {
                genericType = genericType.box();
            }

            interfaceGetMethodType = ParameterizedTypeName.get(confValueClass, genericType);
        }

        return interfaceGetMethodType;
    }

    /**
     * Create VIEW and CHANGE classes and methods.
     *
     * @param fields Collection of configuration fields.
     * @param schemaClassName Class name of schema.
     * @param configurationInterfaceBuilder Configuration interface builder.
     * @param extendBaseSchema {@code true} if extending base schema interfaces.
     * @param realSchemaClass Class descriptor.
     * @param isPolymorphicConfig Is a polymorphic configuration.
     * @param isPolymorphicInstanceConfig Is an instance of polymorphic configuration.
     */
    private void createPojoBindings(
            Collection<VariableElement> fields,
            ClassName schemaClassName,
            TypeSpec.Builder configurationInterfaceBuilder,
            boolean extendBaseSchema,
            TypeElement realSchemaClass,
            boolean isPolymorphicConfig,
            boolean isPolymorphicInstanceConfig
    ) {
        ClassName viewClsName = getViewName(schemaClassName);
        ClassName changeClsName = getChangeName(schemaClassName);

        TypeName configInterfaceType;
        @Nullable TypeName viewBaseSchemaInterfaceType;
        @Nullable TypeName changeBaseSchemaInterfaceType;

        TypeElement superClass = superClass(realSchemaClass);

        boolean isSuperClassAbstractConfiguration = !isClass(superClass.asType(), Object.class)
                && superClass.getAnnotation(AbstractConfiguration.class) != null;

        if (extendBaseSchema || isSuperClassAbstractConfiguration) {
            ClassName superClassSchemaClassName = ClassName.get(superClass);

            viewBaseSchemaInterfaceType = getViewName(superClassSchemaClassName);
            changeBaseSchemaInterfaceType = getChangeName(superClassSchemaClassName);

            if (isSuperClassAbstractConfiguration) {
                // Example: ExtendedTableConfig extends TableConfig<ExtendedTableView, ExtendedTableChange>
                configInterfaceType = ParameterizedTypeName.get(
                        getConfigurationInterfaceName(superClassSchemaClassName),
                        viewClsName,
                        changeClsName
                );
            } else {
                // Example: ExtendedTableConfig extends TableConfig
                configInterfaceType = getConfigurationInterfaceName(superClassSchemaClassName);
            }
        } else {
            ClassName confTreeInterface = ClassName.get("org.apache.ignite.configuration", "ConfigurationTree");

            if (realSchemaClass.getAnnotation(AbstractConfiguration.class) != null) {
                // Example: TableConfig<VIEWT extends TableView, CHANGET extends TableChange> extends ConfigurationTree<VIEWT, CHANGET>
                configurationInterfaceBuilder.addTypeVariables(List.of(
                        TypeVariableName.get("VIEWT", viewClsName),
                        TypeVariableName.get("CHANGET", changeClsName)
                ));

                configInterfaceType = ParameterizedTypeName.get(
                        confTreeInterface,
                        TypeVariableName.get("VIEWT"),
                        TypeVariableName.get("CHANGET")
                );
            } else {
                // Example: TableConfig extends ConfigurationTree<TableView, TableChange>
                configInterfaceType = ParameterizedTypeName.get(confTreeInterface, viewClsName, changeClsName);
            }

            viewBaseSchemaInterfaceType = null;
            changeBaseSchemaInterfaceType = null;
        }

        configurationInterfaceBuilder.addSuperinterface(configInterfaceType);

        // This code will be refactored in the future. Right now I don't want to entangle it with existing code
        // generation. It has only a few considerable problems - hardcode and a lack of proper arrays handling.
        // Clone method should be used to guarantee data integrity.

        TypeSpec.Builder viewClsBuilder = TypeSpec.interfaceBuilder(viewClsName)
                .addModifiers(PUBLIC);

        if (viewBaseSchemaInterfaceType != null) {
            viewClsBuilder.addSuperinterface(viewBaseSchemaInterfaceType);
        }

        TypeSpec.Builder changeClsBuilder = TypeSpec.interfaceBuilder(changeClsName)
                .addSuperinterface(viewClsName)
                .addModifiers(PUBLIC);

        if (changeBaseSchemaInterfaceType != null) {
            changeClsBuilder.addSuperinterface(changeBaseSchemaInterfaceType);
        }

        if (isPolymorphicInstanceConfig) {
            changeClsBuilder.addSuperinterface(POLYMORPHIC_CHANGE_CLASSNAME);
        }

        ClassName consumerClsName = ClassName.get(Consumer.class);

        for (VariableElement field : fields) {
            Value valAnnotation = field.getAnnotation(Value.class);

            String fieldName = field.getSimpleName().toString();
            TypeMirror schemaFieldType = field.asType();
            TypeName schemaFieldTypeName = TypeName.get(schemaFieldType);

            boolean leafField = isValidValueAnnotationFieldType(schemaFieldType)
                    || !((ClassName) schemaFieldTypeName).simpleName().contains(CONFIGURATION_SCHEMA_POSTFIX);

            boolean namedListField = field.getAnnotation(NamedConfigValue.class) != null;

            TypeName viewFieldType =
                    leafField ? schemaFieldTypeName : getViewName((ClassName) schemaFieldTypeName);

            TypeName changeFieldType =
                    leafField ? schemaFieldTypeName : getChangeName((ClassName) schemaFieldTypeName);

            if (namedListField) {
                changeFieldType = ParameterizedTypeName.get(
                        ClassName.get(NamedListChange.class),
                        viewFieldType,
                        changeFieldType
                );

                viewFieldType = ParameterizedTypeName.get(
                        ClassName.get(NamedListView.class),
                        WildcardTypeName.subtypeOf(viewFieldType)
                );
            }

            MethodSpec.Builder getMtdBuilder = MethodSpec.methodBuilder(fieldName)
                    .addModifiers(PUBLIC, ABSTRACT)
                    .returns(viewFieldType);

            viewClsBuilder.addMethod(getMtdBuilder.build());

            // Read only.
            if (field.getAnnotation(PolymorphicId.class) != null || field.getAnnotation(InjectedName.class) != null
                    || field.getAnnotation(InternalId.class) != null) {
                continue;
            }

            String changeMtdName = "change" + capitalize(fieldName);

            MethodSpec.Builder changeMtdBuilder = MethodSpec.methodBuilder(changeMtdName)
                    .addModifiers(PUBLIC, ABSTRACT)
                    .returns(changeClsName);

            if (valAnnotation != null) {
                if (schemaFieldType.getKind() == TypeKind.ARRAY) {
                    changeMtdBuilder.varargs(true);
                }

                changeMtdBuilder.addParameter(changeFieldType, fieldName);
            } else {
                changeMtdBuilder.addParameter(ParameterizedTypeName.get(consumerClsName, changeFieldType), fieldName);
            }

            changeClsBuilder.addMethod(changeMtdBuilder.build());

            // Create "FooChange changeFoo()" method with no parameters, if it's a config value or named list value.
            if (valAnnotation == null) {
                MethodSpec.Builder shortChangeMtdBuilder = MethodSpec.methodBuilder(changeMtdName)
                        .addModifiers(PUBLIC, ABSTRACT)
                        .returns(changeFieldType);

                changeClsBuilder.addMethod(shortChangeMtdBuilder.build());
            }
        }

        if (isPolymorphicConfig) {
            // Parameter type: Class<T>.
            ParameterizedTypeName parameterType = ParameterizedTypeName.get(
                    ClassName.get(Class.class),
                    TypeVariableName.get("T")
            );

            // Variable type, for example: <T extends SimpleChange & PolymorphicChange>.
            TypeVariableName typeVariable = TypeVariableName.get("T", changeClsName, POLYMORPHIC_CHANGE_CLASSNAME);

            // Method like: <T extends SimpleChange & PolymorphicChange> T convert(Class<T> changeClass);
            MethodSpec.Builder convertByChangeClassMtdBuilder = MethodSpec.methodBuilder("convert")
                    .addModifiers(PUBLIC, ABSTRACT)
                    .addTypeVariable(typeVariable)
                    .addParameter(parameterType, "changeClass")
                    .returns(TypeVariableName.get("T"));

            changeClsBuilder.addMethod(convertByChangeClassMtdBuilder.build());

            // Method like: SimpleChange convert(String polymorphicTypeId);
            MethodSpec.Builder convertByStringMtdBuilder = MethodSpec.methodBuilder("convert")
                    .addModifiers(PUBLIC, ABSTRACT)
                    .addParameter(ClassName.get(String.class), "polymorphicTypeId")
                    .returns(changeClsName);

            changeClsBuilder.addMethod(convertByStringMtdBuilder.build());
        }

        TypeSpec viewCls = viewClsBuilder.build();
        TypeSpec changeCls = changeClsBuilder.build();

        buildClass(viewClsName.packageName(), viewCls);
        buildClass(changeClsName.packageName(), changeCls);
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
     * Validate the class.
     *
     * @param clazz Class type.
     * @param fields Class fields.
     * @throws ConfigurationProcessorException If the class validation fails.
     */
    private void validateConfigurationSchemaClass(TypeElement clazz, List<VariableElement> fields) {
        if (!clazz.getSimpleName().toString().endsWith(CONFIGURATION_SCHEMA_POSTFIX)) {
            throw new ConfigurationProcessorException(
                    String.format("%s must end with '%s'", clazz.getQualifiedName(), CONFIGURATION_SCHEMA_POSTFIX));
        }

        if (clazz.getAnnotation(ConfigurationExtension.class) != null) {
            validateExtensionConfiguration(clazz, fields);
        } else if (clazz.getAnnotation(PolymorphicConfig.class) != null) {
            validatePolymorphicConfig(clazz, fields);
        } else if (clazz.getAnnotation(PolymorphicConfigInstance.class) != null) {
            validatePolymorphicConfigInstance(clazz, fields);
        } else if (clazz.getAnnotation(AbstractConfiguration.class) != null) {
            validateAbstractConfiguration(clazz, fields);
        } else if (clazz.getAnnotation(ConfigurationRoot.class) != null) {
            validateConfigurationRoot(clazz, fields);
        } else if (clazz.getAnnotation(Config.class) != null) {
            validateConfig(clazz, fields);
        }

        validateInjectedNameFields(clazz, fields);

        validateNameFields(clazz, fields);
    }

    /**
     * Checks configuration schema with {@link ConfigurationExtension}.
     *
     * @param clazz Type element under validation.
     * @param fields Non-static fields of the class under validation.
     */
    private void validateExtensionConfiguration(TypeElement clazz, List<VariableElement> fields) {
        checkIncompatibleClassAnnotations(
                clazz,
                ConfigurationExtension.class,
                incompatibleSchemaClassAnnotations(ConfigurationExtension.class, ConfigurationRoot.class)
        );

        checkNotContainsPolymorphicIdField(clazz, ConfigurationExtension.class, fields);

        if (clazz.getAnnotation(ConfigurationRoot.class) != null) {
            checkNotExistSuperClass(clazz, ConfigurationExtension.class);
        } else {
            checkExistSuperClass(clazz, ConfigurationExtension.class);

            TypeElement superClazz = superClass(clazz);

            if (superClazz.getAnnotation(ConfigurationExtension.class) != null) {
                throw new ConfigurationProcessorException(String.format(
                        "Superclass must not have %s: %s",
                        simpleName(ConfigurationExtension.class),
                        clazz.getQualifiedName()
                ));
            }

            checkSuperclassContainAnyAnnotation(clazz, superClazz, ConfigurationRoot.class, Config.class);

            checkNoConflictFieldNames(clazz, superClazz, fields, fields(superClazz));
        }
    }

    /**
     * Checks configuration schema with {@link PolymorphicConfig}.
     *
     * @param clazz Type element under validation.
     * @param fields Non-static fields of the class under validation.
     */
    private void validatePolymorphicConfig(TypeElement clazz, List<VariableElement> fields) {
        checkIncompatibleClassAnnotations(
                clazz,
                PolymorphicConfig.class,
                incompatibleSchemaClassAnnotations(PolymorphicConfig.class)
        );

        checkNotExistSuperClass(clazz, PolymorphicConfig.class);

        List<VariableElement> typeIdFields = collectFieldsWithAnnotation(fields, PolymorphicId.class);

        if (typeIdFields.size() != 1 || fields.indexOf(typeIdFields.get(0)) != 0) {
            throw new ConfigurationProcessorException(String.format(
                    "Class with %s must contain one field with %s and it should be the first in the schema: %s",
                    simpleName(PolymorphicConfig.class),
                    simpleName(PolymorphicId.class),
                    clazz.getQualifiedName()
            ));
        }
    }

    /**
     * Checks configuration schema with {@link PolymorphicConfigInstance}.
     *
     * @param clazz Type element under validation.
     * @param fields Non-static fields of the class under validation.
     */
    private void validatePolymorphicConfigInstance(TypeElement clazz, List<VariableElement> fields) {
        checkIncompatibleClassAnnotations(
                clazz,
                PolymorphicConfigInstance.class,
                incompatibleSchemaClassAnnotations(PolymorphicConfigInstance.class)
        );

        checkNotContainsPolymorphicIdField(clazz, PolymorphicConfigInstance.class, fields);

        String id = clazz.getAnnotation(PolymorphicConfigInstance.class).value();

        if (id == null || id.isBlank()) {
            throw new ConfigurationProcessorException(String.format(
                    EMPTY_FIELD_ERROR_FORMAT,
                    simpleName(PolymorphicConfigInstance.class) + ".id()",
                    clazz.getQualifiedName()
            ));
        }

        checkExistSuperClass(clazz, PolymorphicConfigInstance.class);

        TypeElement superClazz = superClass(clazz);

        checkSuperclassContainAnyAnnotation(clazz, superClazz, PolymorphicConfig.class);

        checkNoConflictFieldNames(clazz, superClazz, fields, fields(superClazz));
    }

    /** {@inheritDoc} */
    @Override
    public Set<String> getSupportedAnnotationTypes() {
        return supportedAnnotationTypes().stream().map(Class::getCanonicalName).collect(toSet());
    }

    /** {@inheritDoc} */
    @Override
    public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.latest();
    }

    /**
     * Returns immutable set of annotation types supported by this processor.
     */
    private Set<Class<? extends Annotation>> supportedAnnotationTypes() {
        return Set.of(
                Config.class,
                ConfigurationRoot.class,
                ConfigurationExtension.class,
                PolymorphicConfig.class,
                PolymorphicConfigInstance.class,
                AbstractConfiguration.class
        );
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
     * Search for duplicate class fields by name.
     *
     * @param fields1 First class fields.
     * @param fields2 Second class fields.
     * @return Field names.
     */
    private static Collection<Name> findDuplicates(
            Collection<VariableElement> fields1,
            Collection<VariableElement> fields2
    ) {
        if (fields1.isEmpty() || fields2.isEmpty()) {
            return List.of();
        }

        Set<Name> filedNames1 = fields1.stream()
                .map(VariableElement::getSimpleName)
                .collect(toSet());

        return fields2.stream()
                .map(VariableElement::getSimpleName)
                .filter(filedNames1::contains)
                .collect(toList());
    }

    /**
     * Checking a class field with annotations {@link ConfigValue} or {@link NamedConfigValue}.
     *
     * @param field Class field.
     * @param annotationClass Field annotation: {@link ConfigValue} or {@link NamedConfigValue}.
     * @throws ConfigurationProcessorException If the check is not successful.
     */
    private void checkConfigField(
            VariableElement field,
            Class<? extends Annotation> annotationClass
    ) {
        assert annotationClass == ConfigValue.class || annotationClass == NamedConfigValue.class : annotationClass;
        assert field.getAnnotation(annotationClass) != null : field.getEnclosingElement() + "." + field;

        Element fieldTypeElement = processingEnv.getTypeUtils().asElement(field.asType());

        if (fieldTypeElement.getAnnotation(Config.class) == null
                && fieldTypeElement.getAnnotation(PolymorphicConfig.class) == null) {
            throw new ConfigurationProcessorException(String.format(
                    "Class for %s field must be defined as %s: %s.%s",
                    simpleName(annotationClass),
                    joinSimpleName(" or ", Config.class, PolymorphicConfig.class),
                    field.getEnclosingElement(),
                    field.getSimpleName()
            ));
        }
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

    /**
     * Checks for an incompatible class annotation with {@code clazzAnnotation}.
     *
     * @param clazz Class type.
     * @param clazzAnnotation Class annotation.
     * @param incompatibleAnnotations Incompatible class annotations with {@code clazzAnnotation}.
     * @throws ConfigurationProcessorException If there is an incompatible class annotation with {@code clazzAnnotation}.
     */
    @SafeVarargs
    private void checkIncompatibleClassAnnotations(
            TypeElement clazz,
            Class<? extends Annotation> clazzAnnotation,
            Class<? extends Annotation>... incompatibleAnnotations
    ) {
        assert clazz.getAnnotation(clazzAnnotation) != null : clazz.getQualifiedName();
        assert !nullOrEmpty(incompatibleAnnotations);

        Optional<? extends Annotation> incompatible = findFirstPresentAnnotation(clazz, incompatibleAnnotations);

        if (incompatible.isPresent()) {
            throw new ConfigurationProcessorException(String.format(
                    "Class with %s is not allowed with %s: %s",
                    simpleName(incompatible.get().annotationType()),
                    simpleName(clazzAnnotation),
                    clazz.getQualifiedName()
            ));
        }
    }

    /**
     * Checks that the class has a superclass.
     *
     * @param clazz Class type.
     * @param clazzAnnotation Class annotation.
     * @throws ConfigurationProcessorException If the class doesn't have a superclass.
     */
    private void checkExistSuperClass(TypeElement clazz, Class<? extends Annotation> clazzAnnotation) {
        assert clazz.getAnnotation(clazzAnnotation) != null : clazz.getQualifiedName();

        if (isClass(clazz.getSuperclass(), Object.class)) {
            throw new ConfigurationProcessorException(String.format(
                    "Class with %s should not have a superclass: %s",
                    simpleName(clazzAnnotation),
                    clazz.getQualifiedName()
            ));
        }
    }

    /**
     * Checks that the class should not have a superclass.
     *
     * @param clazz Class type.
     * @param clazzAnnotation Class annotation.
     * @throws ConfigurationProcessorException If the class have a superclass.
     */
    private void checkNotExistSuperClass(TypeElement clazz, Class<? extends Annotation> clazzAnnotation) {
        assert clazz.getAnnotation(clazzAnnotation) != null : clazz.getQualifiedName();

        if (!isClass(clazz.getSuperclass(), Object.class)) {
            throw new ConfigurationProcessorException(String.format(
                    "Class with %s should not have a superclass: %s",
                    simpleName(clazzAnnotation),
                    clazz.getQualifiedName()
            ));
        }
    }

    /**
     * Checks that the class does not have a field with {@link PolymorphicId}.
     *
     * @param clazz Class type.
     * @param clazzAnnotation Class annotation.
     * @param clazzfields Class fields.
     * @throws ConfigurationProcessorException If the class has a field with {@link PolymorphicId}.
     */
    private void checkNotContainsPolymorphicIdField(
            TypeElement clazz,
            Class<? extends Annotation> clazzAnnotation,
            List<VariableElement> clazzfields
    ) {
        assert clazz.getAnnotation(clazzAnnotation) != null : clazz.getQualifiedName();

        if (!collectFieldsWithAnnotation(clazzfields, PolymorphicId.class).isEmpty()) {
            throw new ConfigurationProcessorException(String.format(
                    "Class with %s cannot have a field with %s: %s",
                    simpleName(clazzAnnotation),
                    simpleName(PolymorphicId.class),
                    clazz.getQualifiedName()
            ));
        }
    }

    /**
     * Checks that there is no conflict of field names between classes.
     *
     * @param clazz0 First class type.
     * @param clazz1 Second class type.
     * @param clazzFields0 First class fields.
     * @param clazzFields1 Second class fields.
     * @throws ConfigurationProcessorException If there is a conflict of field names between classes.
     */
    private void checkNoConflictFieldNames(
            TypeElement clazz0,
            TypeElement clazz1,
            List<VariableElement> clazzFields0,
            List<VariableElement> clazzFields1
    ) {
        Collection<Name> duplicateFieldNames = findDuplicates(clazzFields0, clazzFields1);

        if (!duplicateFieldNames.isEmpty()) {
            throw new ConfigurationProcessorException(String.format(
                    "Duplicate field names are not allowed [class=%s, superClass=%s, fields=%s]",
                    clazz0.getQualifiedName(),
                    clazz1.getQualifiedName(),
                    duplicateFieldNames
            ));
        }
    }

    /**
     * Checks if the superclass has at least one annotation from {@code superClazzAnnotations}.
     *
     * @param clazz Class type.
     * @param superClazz Superclass type.
     * @param superClazzAnnotations Superclass annotations.
     * @throws ConfigurationProcessorException If the superclass has none of the annotations from {@code superClazzAnnotations}.
     */
    @SafeVarargs
    private void checkSuperclassContainAnyAnnotation(
            TypeElement clazz,
            TypeElement superClazz,
            Class<? extends Annotation>... superClazzAnnotations
    ) {
        if (Stream.of(superClazzAnnotations).allMatch(a -> superClazz.getAnnotation(a) == null)) {
            throw new ConfigurationProcessorException(String.format(
                    SUPERCLASS_MISSING_ANNOTATION_ERROR_FORMAT,
                    joinSimpleName(" or ", superClazzAnnotations),
                    clazz.getQualifiedName()
            ));
        }
    }

    /**
     * Validation of class fields with {@link InjectedName} if present.
     *
     * @param clazz Class type.
     * @param fields Class fields.
     * @throws ConfigurationProcessorException If the class validation fails.
     */
    private void validateInjectedNameFields(TypeElement clazz, List<VariableElement> fields) {
        List<VariableElement> injectedNameFields = collectFieldsWithAnnotation(fields, InjectedName.class);

        if (injectedNameFields.isEmpty()) {
            return;
        }

        if (injectedNameFields.size() > 1) {
            throw new ConfigurationProcessorException(String.format(
                    "%s contains more than one field with %s",
                    clazz.getQualifiedName(),
                    simpleName(InjectedName.class)
            ));
        }

        VariableElement injectedNameField = injectedNameFields.get(0);

        if (!isClass(injectedNameField.asType(), String.class)) {
            throw new ConfigurationProcessorException(String.format(
                    FIELD_MUST_BE_SPECIFIC_CLASS_ERROR_FORMAT,
                    simpleName(InjectedName.class),
                    clazz.getQualifiedName(),
                    injectedNameField.getSimpleName(),
                    String.class.getSimpleName()
            ));
        }

        // TODO: IGNITE-17166 Must not contain @Value, @ConfigValue etc
        if (injectedNameField.getAnnotationMirrors().size() > 1) {
            throw new ConfigurationProcessorException(String.format(
                    "%s.%s must contain only one %s",
                    clazz.getQualifiedName(),
                    injectedNameField.getSimpleName(),
                    simpleName(InjectedName.class)
            ));
        }

        findFirstPresentAnnotation(clazz, Config.class, PolymorphicConfig.class, AbstractConfiguration.class)
                .orElseThrow(() -> new ConfigurationProcessorException(String.format(
                        "%s %s.%s can only be present in a class annotated with %s",
                        simpleName(InjectedName.class),
                        clazz.getQualifiedName(),
                        injectedNameField.getSimpleName(),
                        joinSimpleName(" or ", Config.class, PolymorphicConfig.class, AbstractConfiguration.class)
                )));
    }

    /**
     * Validation of class fields with {@link Name} if present.
     *
     * @param clazz Class type.
     * @param fields Class fields.
     * @throws ConfigurationProcessorException If the class validation fails.
     */
    private void validateNameFields(TypeElement clazz, List<VariableElement> fields) {
        List<VariableElement> nameFields = collectFieldsWithAnnotation(fields, org.apache.ignite.configuration.annotation.Name.class);

        if (nameFields.isEmpty()) {
            return;
        }

        for (VariableElement nameField : nameFields) {
            if (nameField.getAnnotation(ConfigValue.class) == null) {
                throw new ConfigurationProcessorException(String.format(
                        "%s annotation can only be used with %s: %s.%s",
                        simpleName(org.apache.ignite.configuration.annotation.Name.class),
                        simpleName(ConfigValue.class),
                        clazz.getQualifiedName(),
                        nameField.getSimpleName()
                ));
            }
        }
    }

    /**
     * Checks for missing {@link org.apache.ignite.configuration.annotation.Name} for nested schema with {@link InjectedName}.
     *
     * @param field Class field.
     * @throws ConfigurationProcessorException If there is no {@link org.apache.ignite.configuration.annotation.Name} for the nested schema
     *      with {@link InjectedName}.
     */
    private void checkMissingNameForInjectedName(VariableElement field) {
        TypeElement fieldType = (TypeElement) processingEnv.getTypeUtils().asElement(field.asType());

        TypeElement superClassFieldType = superClass(fieldType);

        List<VariableElement> fields;

        if (!isClass(superClassFieldType.asType(), Object.class)
                && findFirstPresentAnnotation(superClassFieldType, AbstractConfiguration.class).isPresent()) {
            fields = concat(
                    collectFieldsWithAnnotation(fields(fieldType), InjectedName.class),
                    collectFieldsWithAnnotation(fields(superClassFieldType), InjectedName.class)
            );
        } else {
            fields = collectFieldsWithAnnotation(fields(fieldType), InjectedName.class);
        }

        if (!fields.isEmpty() && field.getAnnotation(org.apache.ignite.configuration.annotation.Name.class) == null) {
            throw new ConfigurationProcessorException(String.format(
                    "Missing %s for field: %s.%s",
                    simpleName(org.apache.ignite.configuration.annotation.Name.class),
                    field.getEnclosingElement(),
                    field.getSimpleName()
            ));
        }
    }

    /**
     * Checks configuration schema with {@link AbstractConfiguration}.
     *
     * @param clazz Type element under validation.
     * @param fields Non-static fields of the class under validation.
     * @throws ConfigurationProcessorException If validation fails.
     */
    private void validateAbstractConfiguration(TypeElement clazz, List<VariableElement> fields) throws ConfigurationProcessorException {
        checkIncompatibleClassAnnotations(
                clazz,
                AbstractConfiguration.class,
                incompatibleSchemaClassAnnotations(AbstractConfiguration.class)
        );

        checkNotExistSuperClass(clazz, AbstractConfiguration.class);

        checkNotContainsPolymorphicIdField(clazz, AbstractConfiguration.class, fields);
    }

    /**
     * Checks configuration schema with {@link ConfigurationRoot}.
     *
     * @param clazz Type element under validation.
     * @param fields Non-static fields of the class under validation.
     * @throws ConfigurationProcessorException If validation fails.
     */
    private void validateConfigurationRoot(TypeElement clazz, List<VariableElement> fields) throws ConfigurationProcessorException {
        checkIncompatibleClassAnnotations(
                clazz,
                ConfigurationRoot.class,
                incompatibleSchemaClassAnnotations(ConfigurationRoot.class)
        );

        checkNotContainsPolymorphicIdField(clazz, ConfigurationRoot.class, fields);

        TypeElement superClazz = superClass(clazz);

        if (!isClass(superClazz.asType(), Object.class)) {
            checkSuperclassContainAnyAnnotation(clazz, superClazz, AbstractConfiguration.class);

            List<VariableElement> superClazzFields = fields(superClazz);

            checkNoConflictFieldNames(clazz, superClazz, fields, superClazzFields);

            String invalidFieldInSuperClassFormat = "Field with %s in superclass are not allowed [class=%s, superClass=%s]";

            if (!collectFieldsWithAnnotation(superClazzFields, InjectedName.class).isEmpty()) {
                throw new ConfigurationProcessorException(String.format(
                        invalidFieldInSuperClassFormat,
                        simpleName(InjectedName.class),
                        clazz.getQualifiedName(),
                        superClazz.getQualifiedName()
                ));
            }

            if (!collectFieldsWithAnnotation(superClazzFields, InternalId.class).isEmpty()) {
                throw new ConfigurationProcessorException(String.format(
                        invalidFieldInSuperClassFormat,
                        simpleName(InternalId.class),
                        clazz.getQualifiedName(),
                        superClazz.getQualifiedName()
                ));
            }
        }
    }

    /**
     * Checks configuration schema with {@link Config}.
     *
     * @param clazz Type element under validation.
     * @param fields Non-static fields of the class under validation.
     * @throws ConfigurationProcessorException If validation fails.
     */
    private void validateConfig(TypeElement clazz, List<VariableElement> fields) throws ConfigurationProcessorException {
        checkIncompatibleClassAnnotations(
                clazz,
                Config.class,
                incompatibleSchemaClassAnnotations(Config.class)
        );

        checkNotContainsPolymorphicIdField(clazz, Config.class, fields);

        TypeElement superClazz = superClass(clazz);

        if (!isClass(superClazz.asType(), Object.class)) {
            checkSuperclassContainAnyAnnotation(clazz, superClazz, AbstractConfiguration.class);

            List<VariableElement> superClazzFields = fields(superClazz);

            checkNoConflictFieldNames(clazz, superClazz, fields, superClazzFields);

            String fieldAlreadyPresentInSuperClassFormat = "Field with %s is already present in the superclass [class=%s, superClass=%s]";

            if (!collectFieldsWithAnnotation(superClazzFields, InjectedName.class).isEmpty()
                    && !collectFieldsWithAnnotation(fields, InjectedName.class).isEmpty()) {
                throw new ConfigurationProcessorException(String.format(
                        fieldAlreadyPresentInSuperClassFormat,
                        simpleName(InjectedName.class),
                        clazz.getQualifiedName(),
                        superClazz.getQualifiedName()
                ));
            }

            if (!collectFieldsWithAnnotation(superClazzFields, InternalId.class).isEmpty()
                    && !collectFieldsWithAnnotation(fields, InternalId.class).isEmpty()) {
                throw new ConfigurationProcessorException(String.format(
                        fieldAlreadyPresentInSuperClassFormat,
                        simpleName(InternalId.class),
                        clazz.getQualifiedName(),
                        superClazz.getQualifiedName()
                ));
            }
        }
    }

    @SafeVarargs
    private Class<? extends Annotation>[] incompatibleSchemaClassAnnotations(Class<? extends Annotation>... compatibleAnnotations) {
        return difference(supportedAnnotationTypes(), Set.of(compatibleAnnotations)).toArray(Class[]::new);
    }
}
