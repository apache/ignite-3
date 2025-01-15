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
import static org.apache.ignite.internal.configuration.processor.ConfigurationProcessorUtils.containsAnyAnnotation;
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
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import com.squareup.javapoet.TypeSpec.Builder;
import com.squareup.javapoet.TypeVariableName;
import com.squareup.javapoet.WildcardTypeName;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.annotation.Annotation;
import java.util.Arrays;
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
import org.apache.ignite.configuration.annotation.InjectedValue;
import org.apache.ignite.configuration.annotation.InternalId;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.PolymorphicConfig;
import org.apache.ignite.configuration.annotation.PolymorphicConfigInstance;
import org.apache.ignite.configuration.annotation.PolymorphicId;
import org.apache.ignite.configuration.annotation.Secret;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.internal.configuration.processor.validation.InjectedValueValidator;
import org.jetbrains.annotations.Nullable;

/**
 * Annotation processor that produces configuration classes.
 */
// TODO: IGNITE-17166 Split into classes/methods for regular/internal/polymorphic/abstract configuration
@AutoService(Processor.class)
public class ConfigurationProcessor extends AbstractProcessor {
    public static final Set<Class<? extends Annotation>> TOP_LEVEL_ANNOTATIONS = Set.of(
            Config.class,
            ConfigurationRoot.class,
            ConfigurationExtension.class,
            PolymorphicConfig.class,
            PolymorphicConfigInstance.class,
            AbstractConfiguration.class
    );

    /** Java file padding. */
    private static final String INDENT = "    ";

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

    /** Postfix with which any configuration schema class name must end. */
    private static final String CONFIGURATION_SCHEMA_POSTFIX = "ConfigurationSchema";

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
                .getElementsAnnotatedWithAny(TOP_LEVEL_ANNOTATIONS)
                .stream()
                .filter(element -> element.getKind() == ElementKind.CLASS)
                .map(TypeElement.class::cast)
                .collect(toList());

        if (annotatedConfigs.isEmpty()) {
            return false;
        }

        var injectedValueValidator = new InjectedValueValidator(processingEnv);

        for (TypeElement clazz : annotatedConfigs) {
            var classWrapper = new ClassWrapper(processingEnv, clazz);

            validateConfigurationSchemaClass(classWrapper);

            injectedValueValidator.validate(classWrapper);

            // Get package name of the schema class
            String packageName = elementUtils.getPackageOf(clazz).getQualifiedName().toString();

            ClassName schemaClassName = ClassName.get(packageName, clazz.getSimpleName().toString());

            // Get name for generated configuration interface.
            ClassName configInterface = getConfigurationInterfaceName(schemaClassName);

            TypeSpec.Builder configurationInterfaceBuilder = TypeSpec.interfaceBuilder(configInterface)
                    .addModifiers(PUBLIC);

            for (VariableElement field : classWrapper.fields()) {
                if (!field.getModifiers().contains(PUBLIC)) {
                    throw new ConfigurationProcessorException("Field " + clazz.getQualifiedName() + "." + field + " must be public");
                }

                String fieldName = field.getSimpleName().toString();

                if (field.getAnnotation(ConfigValue.class) != null) {
                    checkConfigField(field, ConfigValue.class);

                    checkMissingNameForInjectedName(field);
                }

                if (field.getAnnotation(NamedConfigValue.class) != null) {
                    checkConfigField(field, NamedConfigValue.class);
                }

                if (field.getAnnotation(Value.class) != null) {
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

                if (field.getAnnotation(PolymorphicId.class) != null) {
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

                // Get configuration types (VIEW, CHANGE and so on)
                TypeName interfaceGetMethodType = getInterfaceGetMethodType(field);

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
                    classWrapper,
                    schemaClassName,
                    configurationInterfaceBuilder,
                    (isExtendingConfig && !isRootConfig) || isPolymorphicInstance,
                    isPolymorphicConfig,
                    isPolymorphicInstance
            );

            if (isRootConfig) {
                createRootKeyField(configInterface, configurationInterfaceBuilder, schemaClassName, clazz);
            } else if (isExtendingConfig) {
                ClassWrapper superClass = classWrapper.superClass();

                boolean isSuperClassRootConfig = superClass != null && superClass.getAnnotation(ConfigurationRoot.class) != null;
                if (isSuperClassRootConfig) {
                    createExtensionKeyField(configInterface, configurationInterfaceBuilder, ClassName.get(superClass.clazz()));
                }
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

    private static void createExtensionKeyField(
            ClassName configInterface,
            Builder configurationClassBuilder,
            ClassName superClassSchemaClassName
    ) {
        ClassName viewClassName = getViewName(superClassSchemaClassName);

        ClassName superConfigInterface = getConfigurationInterfaceName(superClassSchemaClassName);

        ParameterizedTypeName fieldTypeName = ParameterizedTypeName.get(ROOT_KEY_CLASSNAME, configInterface, viewClassName);

        FieldSpec keyField = FieldSpec.builder(fieldTypeName, "KEY", PUBLIC, STATIC, FINAL)
                .initializer(
                        "($T) $T.KEY",
                        ROOT_KEY_CLASSNAME,
                        superConfigInterface
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
        TypeName baseType = TypeName.get(field.asType());

        ConfigValue confAnnotation = field.getAnnotation(ConfigValue.class);
        if (confAnnotation != null) {
            return getConfigurationInterfaceName((ClassName) baseType);
        }

        NamedConfigValue namedConfigAnnotation = field.getAnnotation(NamedConfigValue.class);
        if (namedConfigAnnotation != null) {
            ClassName interfaceGetType = getConfigurationInterfaceName((ClassName) baseType);

            TypeName viewClassType = getViewName((ClassName) baseType);
            TypeName changeClassType = getChangeName((ClassName) baseType);

            return ParameterizedTypeName.get(
                    ClassName.get(NamedConfigurationTree.class),
                    interfaceGetType,
                    viewClassType,
                    changeClassType
            );
        }

        boolean containsAnnotation = containsAnyAnnotation(
                field,
                Value.class,
                PolymorphicId.class,
                InjectedName.class,
                InternalId.class,
                InjectedValue.class
        );

        if (containsAnnotation) {
            // It is necessary to use class names without loading classes so that we won't
            // accidentally get NoClassDefFoundError
            ClassName confValueClass = ClassName.get("org.apache.ignite.configuration", "ConfigurationValue");

            TypeName genericType = baseType;

            if (genericType.isPrimitive()) {
                genericType = genericType.box();
            }

            return ParameterizedTypeName.get(confValueClass, genericType);
        }

        throw new IllegalArgumentException(String.format("Field \"%s\" does not contain any supported annotations", field));
    }

    /**
     * Create VIEW and CHANGE classes and methods.
     *
     * @param schemaClassName Class name of schema.
     * @param configurationInterfaceBuilder Configuration interface builder.
     * @param extendBaseSchema {@code true} if extending base schema interfaces.
     * @param isPolymorphicConfig Is a polymorphic configuration.
     * @param isPolymorphicInstanceConfig Is an instance of polymorphic configuration.
     */
    private void createPojoBindings(
            ClassWrapper classWrapper,
            ClassName schemaClassName,
            TypeSpec.Builder configurationInterfaceBuilder,
            boolean extendBaseSchema,
            boolean isPolymorphicConfig,
            boolean isPolymorphicInstanceConfig
    ) {
        ClassName viewClsName = getViewName(schemaClassName);
        ClassName changeClsName = getChangeName(schemaClassName);

        TypeName configInterfaceType;
        @Nullable TypeName viewBaseSchemaInterfaceType;
        @Nullable TypeName changeBaseSchemaInterfaceType;

        ClassWrapper superClass = classWrapper.superClass();

        boolean isSuperClassAbstractConfiguration = superClass != null
                && superClass.getAnnotation(AbstractConfiguration.class) != null;

        if (superClass != null && (extendBaseSchema || isSuperClassAbstractConfiguration)) {
            ClassName superClassSchemaClassName = ClassName.get(superClass.clazz());

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

            if (classWrapper.getAnnotation(AbstractConfiguration.class) != null) {
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

        for (VariableElement field : classWrapper.fields()) {
            String fieldName = field.getSimpleName().toString();
            TypeMirror schemaFieldType = field.asType();
            TypeName schemaFieldTypeName = TypeName.get(schemaFieldType);

            boolean leafField = isValidValueAnnotationFieldType(schemaFieldType)
                    || !((ClassName) schemaFieldTypeName).simpleName().contains(CONFIGURATION_SCHEMA_POSTFIX);

            TypeName viewFieldType =
                    leafField ? schemaFieldTypeName : getViewName((ClassName) schemaFieldTypeName);

            TypeName changeFieldType =
                    leafField ? schemaFieldTypeName : getChangeName((ClassName) schemaFieldTypeName);

            if (field.getAnnotation(NamedConfigValue.class) != null) {
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
            if (containsAnyAnnotation(field, PolymorphicId.class, InjectedName.class, InternalId.class)) {
                continue;
            }

            String changeMtdName = "change" + capitalize(fieldName);

            MethodSpec.Builder changeMtdBuilder = MethodSpec.methodBuilder(changeMtdName)
                    .addModifiers(PUBLIC, ABSTRACT)
                    .returns(changeClsName);

            if (containsAnyAnnotation(field, Value.class, InjectedValue.class)) {
                if (schemaFieldType.getKind() == TypeKind.ARRAY) {
                    changeMtdBuilder.varargs(true);
                }

                changeMtdBuilder.addParameter(changeFieldType, fieldName);
            } else {
                changeMtdBuilder.addParameter(ParameterizedTypeName.get(consumerClsName, changeFieldType), fieldName);

                // Create "FooChange changeFoo()" method with no parameters, if it's a config value or named list value.
                MethodSpec.Builder shortChangeMtdBuilder = MethodSpec.methodBuilder(changeMtdName)
                        .addModifiers(PUBLIC, ABSTRACT)
                        .returns(changeFieldType);

                changeClsBuilder.addMethod(shortChangeMtdBuilder.build());
            }

            changeClsBuilder.addMethod(changeMtdBuilder.build());
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
        try {
            JavaFile.builder(packageName, cls)
                    .indent(INDENT)
                    .build()
                    .writeTo(processingEnv.getFiler());
        } catch (Throwable throwable) {
            throw new ConfigurationProcessorException("Failed to generate class " + packageName + "." + cls.name, throwable);
        }
    }

    private static String capitalize(String name) {
        return name.substring(0, 1).toUpperCase() + name.substring(1);
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
     * @throws ConfigurationProcessorException If the class validation fails.
     */
    private void validateConfigurationSchemaClass(ClassWrapper classWrapper) {
        if (!classWrapper.clazz().getSimpleName().toString().endsWith(CONFIGURATION_SCHEMA_POSTFIX)) {
            throw new ConfigurationProcessorException(
                    String.format("%s must end with '%s'", classWrapper.clazz().getQualifiedName(), CONFIGURATION_SCHEMA_POSTFIX));
        }

        if (classWrapper.getAnnotation(ConfigurationExtension.class) != null) {
            validateExtensionConfiguration(classWrapper);
        } else if (classWrapper.getAnnotation(PolymorphicConfig.class) != null) {
            validatePolymorphicConfig(classWrapper);
        } else if (classWrapper.getAnnotation(PolymorphicConfigInstance.class) != null) {
            validatePolymorphicConfigInstance(classWrapper);
        } else if (classWrapper.getAnnotation(AbstractConfiguration.class) != null) {
            validateAbstractConfiguration(classWrapper);
        } else if (classWrapper.getAnnotation(ConfigurationRoot.class) != null) {
            validateConfigurationRoot(classWrapper);
        } else if (classWrapper.getAnnotation(Config.class) != null) {
            validateConfig(classWrapper);
        }

        validateInjectedNameFields(classWrapper);

        validateNameFields(classWrapper);
    }

    /**
     * Checks configuration schema with {@link ConfigurationExtension}.
     */
    private void validateExtensionConfiguration(ClassWrapper classWrapper) {
        checkIncompatibleClassAnnotations(
                classWrapper.clazz(),
                ConfigurationExtension.class,
                incompatibleSchemaClassAnnotations(ConfigurationExtension.class, ConfigurationRoot.class)
        );

        checkNotContainsPolymorphicIdField(classWrapper, ConfigurationExtension.class);

        if (classWrapper.getAnnotation(ConfigurationRoot.class) != null) {
            checkNotExistSuperClass(classWrapper.clazz(), ConfigurationExtension.class);
        } else {
            checkExistSuperClass(classWrapper.clazz(), ConfigurationExtension.class);

            ClassWrapper superClassWrapper = classWrapper.requiredSuperClass();

            if (superClassWrapper.getAnnotation(ConfigurationExtension.class) != null) {
                throw new ConfigurationProcessorException(String.format(
                        "Superclass must not have %s: %s",
                        simpleName(ConfigurationExtension.class),
                        classWrapper.clazz().getQualifiedName()
                ));
            }

            checkSuperclassContainAnyAnnotation(classWrapper.clazz(), superClassWrapper.clazz(), ConfigurationRoot.class, Config.class);

            checkNoConflictFieldNames(classWrapper, superClassWrapper);
        }
    }

    /**
     * Checks configuration schema with {@link PolymorphicConfig}.
     */
    private void validatePolymorphicConfig(ClassWrapper classWrapper) {
        checkIncompatibleClassAnnotations(
                classWrapper.clazz(),
                PolymorphicConfig.class,
                incompatibleSchemaClassAnnotations(PolymorphicConfig.class)
        );

        checkNotExistSuperClass(classWrapper.clazz(), PolymorphicConfig.class);

        List<VariableElement> typeIdFields = classWrapper.fieldsAnnotatedWith(PolymorphicId.class);

        if (typeIdFields.size() != 1 || classWrapper.fields().indexOf(typeIdFields.get(0)) != 0) {
            throw new ConfigurationProcessorException(String.format(
                    "Class with %s must contain one field with %s and it should be the first in the schema: %s",
                    simpleName(PolymorphicConfig.class),
                    simpleName(PolymorphicId.class),
                    classWrapper.clazz().getQualifiedName()
            ));
        }
    }

    /**
     * Checks configuration schema with {@link PolymorphicConfigInstance}.
     */
    private void validatePolymorphicConfigInstance(ClassWrapper classWrapper) {
        checkIncompatibleClassAnnotations(
                classWrapper.clazz(),
                PolymorphicConfigInstance.class,
                incompatibleSchemaClassAnnotations(PolymorphicConfigInstance.class)
        );

        checkNotContainsPolymorphicIdField(classWrapper, PolymorphicConfigInstance.class);

        String id = classWrapper.getAnnotation(PolymorphicConfigInstance.class).value();

        if (id == null || id.isBlank()) {
            throw new ConfigurationProcessorException(String.format(
                    EMPTY_FIELD_ERROR_FORMAT,
                    simpleName(PolymorphicConfigInstance.class) + ".id()",
                    classWrapper.clazz().getQualifiedName()
            ));
        }

        checkExistSuperClass(classWrapper.clazz(), PolymorphicConfigInstance.class);

        ClassWrapper superClassWrapper = classWrapper.requiredSuperClass();

        checkSuperclassContainAnyAnnotation(classWrapper.clazz(), superClassWrapper.clazz(), PolymorphicConfig.class);

        checkNoConflictFieldNames(classWrapper, superClassWrapper);
    }

    @Override
    public Set<String> getSupportedAnnotationTypes() {
        return TOP_LEVEL_ANNOTATIONS.stream().map(Class::getCanonicalName).collect(toSet());
    }

    @Override
    public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.latest();
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

        Optional<Class<? extends Annotation>> incompatible = Arrays.stream(incompatibleAnnotations)
                .filter(a -> clazz.getAnnotation(a) != null)
                .findAny();

        if (incompatible.isPresent()) {
            throw new ConfigurationProcessorException(String.format(
                    "Class with %s is not allowed with %s: %s",
                    simpleName(incompatible.get()),
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
     * @throws ConfigurationProcessorException If the class has a field with {@link PolymorphicId}.
     */
    private void checkNotContainsPolymorphicIdField(
            ClassWrapper classWrapper,
            Class<? extends Annotation> clazzAnnotation
    ) {
        assert classWrapper.getAnnotation(clazzAnnotation) != null : classWrapper.clazz().getQualifiedName();

        if (!classWrapper.fieldsAnnotatedWith(PolymorphicId.class).isEmpty()) {
            throw new ConfigurationProcessorException(String.format(
                    "Class with %s cannot have a field with %s: %s",
                    simpleName(clazzAnnotation),
                    simpleName(PolymorphicId.class),
                    classWrapper.clazz().getQualifiedName()
            ));
        }
    }

    /**
     * Checks that there is no conflict of field names between classes.
     *
     * @throws ConfigurationProcessorException If there is a conflict of field names between classes.
     */
    private void checkNoConflictFieldNames(
            ClassWrapper classWrapper0,
            ClassWrapper classWrapper1
    ) {
        Collection<Name> duplicateFieldNames = findDuplicates(classWrapper0.fields(), classWrapper1.fields());

        if (!duplicateFieldNames.isEmpty()) {
            throw new ConfigurationProcessorException(String.format(
                    "Duplicate field names are not allowed [class=%s, superClass=%s, fields=%s]",
                    classWrapper0.clazz().getQualifiedName(),
                    classWrapper1.clazz().getQualifiedName(),
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
     * @throws ConfigurationProcessorException If the class validation fails.
     */
    private void validateInjectedNameFields(ClassWrapper classWrapper) {
        List<VariableElement> injectedNameFields = classWrapper.fieldsAnnotatedWith(InjectedName.class);

        if (injectedNameFields.isEmpty()) {
            return;
        }

        if (injectedNameFields.size() > 1) {
            throw new ConfigurationProcessorException(String.format(
                    "%s contains more than one field with %s",
                    classWrapper.clazz().getQualifiedName(),
                    simpleName(InjectedName.class)
            ));
        }

        VariableElement injectedNameField = injectedNameFields.get(0);

        if (!isClass(injectedNameField.asType(), String.class)) {
            throw new ConfigurationProcessorException(String.format(
                    FIELD_MUST_BE_SPECIFIC_CLASS_ERROR_FORMAT,
                    simpleName(InjectedName.class),
                    classWrapper.clazz().getQualifiedName(),
                    injectedNameField.getSimpleName(),
                    String.class.getSimpleName()
            ));
        }

        // TODO: IGNITE-17166 Must not contain @Value, @ConfigValue etc
        if (injectedNameField.getAnnotationMirrors().size() > 1) {
            throw new ConfigurationProcessorException(String.format(
                    "%s.%s must contain only one %s",
                    classWrapper.clazz().getQualifiedName(),
                    injectedNameField.getSimpleName(),
                    simpleName(InjectedName.class)
            ));
        }

        if (!containsAnyAnnotation(classWrapper.clazz(), Config.class, PolymorphicConfig.class, AbstractConfiguration.class)) {
            throw new ConfigurationProcessorException(String.format(
                    "%s %s.%s can only be present in a class annotated with %s",
                    simpleName(InjectedName.class),
                    classWrapper.clazz().getQualifiedName(),
                    injectedNameField.getSimpleName(),
                    joinSimpleName(" or ", Config.class, PolymorphicConfig.class, AbstractConfiguration.class)
            ));
        }
    }

    /**
     * Validation of class fields with {@link Name} if present.
     *
     * @throws ConfigurationProcessorException If the class validation fails.
     */
    private void validateNameFields(ClassWrapper classWrapper) {
        List<VariableElement> nameFields = classWrapper.fieldsAnnotatedWith(org.apache.ignite.configuration.annotation.Name.class);

        if (nameFields.isEmpty()) {
            return;
        }

        for (VariableElement nameField : nameFields) {
            if (nameField.getAnnotation(ConfigValue.class) == null) {
                throw new ConfigurationProcessorException(String.format(
                        "%s annotation can only be used with %s: %s.%s",
                        simpleName(org.apache.ignite.configuration.annotation.Name.class),
                        simpleName(ConfigValue.class),
                        classWrapper.clazz().getQualifiedName(),
                        nameField.getSimpleName()
                ));
            }
        }
    }

    /**
     * Checks for missing {@link org.apache.ignite.configuration.annotation.Name} for nested schema with {@link InjectedName}.
     *
     * @param field Class field.
     * @throws ConfigurationProcessorException If there is no {@link org.apache.ignite.configuration.annotation.Name} for the nested
     *         schema with {@link InjectedName}.
     */
    private void checkMissingNameForInjectedName(VariableElement field) {
        TypeElement fieldType = (TypeElement) processingEnv.getTypeUtils().asElement(field.asType());

        var fieldClassWrapper = new ClassWrapper(processingEnv, fieldType);

        ClassWrapper fieldSuperClassWrapper = fieldClassWrapper.superClass();

        List<VariableElement> fields;

        if (fieldSuperClassWrapper != null
                && fieldSuperClassWrapper.getAnnotation(AbstractConfiguration.class) != null) {
            fields = concat(
                    fieldClassWrapper.fieldsAnnotatedWith(InjectedName.class),
                    fieldSuperClassWrapper.fieldsAnnotatedWith(InjectedName.class)
            );
        } else {
            fields = fieldClassWrapper.fieldsAnnotatedWith(InjectedName.class);
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
     * @throws ConfigurationProcessorException If validation fails.
     */
    private void validateAbstractConfiguration(ClassWrapper classWrapper) throws ConfigurationProcessorException {
        checkIncompatibleClassAnnotations(
                classWrapper.clazz(),
                AbstractConfiguration.class,
                incompatibleSchemaClassAnnotations(AbstractConfiguration.class)
        );

        checkNotExistSuperClass(classWrapper.clazz(), AbstractConfiguration.class);

        checkNotContainsPolymorphicIdField(classWrapper, AbstractConfiguration.class);
    }

    /**
     * Checks configuration schema with {@link ConfigurationRoot}.
     *
     * @throws ConfigurationProcessorException If validation fails.
     */
    private void validateConfigurationRoot(ClassWrapper classWrapper) throws ConfigurationProcessorException {
        checkIncompatibleClassAnnotations(
                classWrapper.clazz(),
                ConfigurationRoot.class,
                incompatibleSchemaClassAnnotations(ConfigurationRoot.class)
        );

        checkNotContainsPolymorphicIdField(classWrapper, ConfigurationRoot.class);

        ClassWrapper superClassWrapper = classWrapper.superClass();

        if (superClassWrapper != null) {
            checkSuperclassContainAnyAnnotation(classWrapper.clazz(), superClassWrapper.clazz(), AbstractConfiguration.class);

            checkNoConflictFieldNames(classWrapper, superClassWrapper);

            String invalidFieldInSuperClassFormat = "Field with %s in superclass are not allowed [class=%s, superClass=%s]";

            if (!superClassWrapper.fieldsAnnotatedWith(InjectedName.class).isEmpty()) {
                throw new ConfigurationProcessorException(String.format(
                        invalidFieldInSuperClassFormat,
                        simpleName(InjectedName.class),
                        classWrapper.clazz().getQualifiedName(),
                        superClassWrapper.clazz().getQualifiedName()
                ));
            }

            if (!superClassWrapper.fieldsAnnotatedWith(InternalId.class).isEmpty()) {
                throw new ConfigurationProcessorException(String.format(
                        invalidFieldInSuperClassFormat,
                        simpleName(InternalId.class),
                        classWrapper.clazz().getQualifiedName(),
                        superClassWrapper.clazz().getQualifiedName()
                ));
            }
        }
    }

    /**
     * Checks configuration schema with {@link Config}.
     *
     * @throws ConfigurationProcessorException If validation fails.
     */
    private void validateConfig(ClassWrapper classWrapper) throws ConfigurationProcessorException {
        checkIncompatibleClassAnnotations(
                classWrapper.clazz(),
                Config.class,
                incompatibleSchemaClassAnnotations(Config.class)
        );

        checkNotContainsPolymorphicIdField(classWrapper, Config.class);

        ClassWrapper superClassWrapper = classWrapper.superClass();

        if (superClassWrapper != null) {
            checkSuperclassContainAnyAnnotation(classWrapper.clazz(), superClassWrapper.clazz(), AbstractConfiguration.class);

            checkNoConflictFieldNames(classWrapper, superClassWrapper);

            String fieldAlreadyPresentInSuperClassFormat = "Field with %s is already present in the superclass [class=%s, superClass=%s]";

            if (!superClassWrapper.fieldsAnnotatedWith(InjectedName.class).isEmpty()
                    && !classWrapper.fieldsAnnotatedWith(InjectedName.class).isEmpty()) {
                throw new ConfigurationProcessorException(String.format(
                        fieldAlreadyPresentInSuperClassFormat,
                        simpleName(InjectedName.class),
                        classWrapper.clazz().getQualifiedName(),
                        superClassWrapper.clazz().getQualifiedName()
                ));
            }

            if (!superClassWrapper.fieldsAnnotatedWith(InternalId.class).isEmpty()
                    && !classWrapper.fieldsAnnotatedWith(InternalId.class).isEmpty()) {
                throw new ConfigurationProcessorException(String.format(
                        fieldAlreadyPresentInSuperClassFormat,
                        simpleName(InternalId.class),
                        classWrapper.clazz().getQualifiedName(),
                        superClassWrapper.clazz().getQualifiedName()
                ));
            }
        }
    }

    @SafeVarargs
    private Class<? extends Annotation>[] incompatibleSchemaClassAnnotations(Class<? extends Annotation>... compatibleAnnotations) {
        return difference(TOP_LEVEL_ANNOTATIONS, Set.of(compatibleAnnotations)).toArray(Class[]::new);
    }
}
