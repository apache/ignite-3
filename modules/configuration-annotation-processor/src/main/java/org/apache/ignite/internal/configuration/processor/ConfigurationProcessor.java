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
import static org.apache.ignite.internal.configuration.processor.ConfigurationProcessorUtils.isValidValueAnnotationFieldType;

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
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
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
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.configuration.annotation.InjectedName;
import org.apache.ignite.configuration.annotation.InjectedValue;
import org.apache.ignite.configuration.annotation.InternalId;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.PolymorphicConfig;
import org.apache.ignite.configuration.annotation.PolymorphicConfigInstance;
import org.apache.ignite.configuration.annotation.PolymorphicId;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.internal.configuration.processor.validation.AbstractConfigurationValidator;
import org.apache.ignite.internal.configuration.processor.validation.ConfigValidator;
import org.apache.ignite.internal.configuration.processor.validation.ConfigValueValidator;
import org.apache.ignite.internal.configuration.processor.validation.ConfigurationExtentionValidator;
import org.apache.ignite.internal.configuration.processor.validation.ConfigurationRootValidator;
import org.apache.ignite.internal.configuration.processor.validation.InjectedNameValidator;
import org.apache.ignite.internal.configuration.processor.validation.InjectedValueValidator;
import org.apache.ignite.internal.configuration.processor.validation.MiscellaneousIssuesValidator;
import org.apache.ignite.internal.configuration.processor.validation.NamedConfigValueValidator;
import org.apache.ignite.internal.configuration.processor.validation.PolymorphicConfigInstanceValidator;
import org.apache.ignite.internal.configuration.processor.validation.PolymorphicConfigValidator;
import org.apache.ignite.internal.configuration.processor.validation.Validator;
import org.jetbrains.annotations.Nullable;

/**
 * Annotation processor that produces configuration classes.
 */
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

    /** Postfix with which any configuration schema class name must end. */
    public static final String CONFIGURATION_SCHEMA_POSTFIX = "ConfigurationSchema";

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

    private List<Validator> validators() {
        return List.of(
                new MiscellaneousIssuesValidator(processingEnv),
                new ConfigurationExtentionValidator(processingEnv),
                new PolymorphicConfigValidator(processingEnv),
                new PolymorphicConfigInstanceValidator(processingEnv),
                new AbstractConfigurationValidator(processingEnv),
                new ConfigurationRootValidator(processingEnv),
                new ConfigValidator(processingEnv),
                new InjectedNameValidator(processingEnv),
                new InjectedValueValidator(processingEnv),
                new ConfigValueValidator(processingEnv),
                new NamedConfigValueValidator(processingEnv)
        );
    }

    /**
     * Processes a set of annotation types on type elements.
     *
     * @param roundEnvironment Processing environment.
     * @return Whether the set of annotation types are claimed by this processor.
     */
    private boolean process0(RoundEnvironment roundEnvironment) {
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

        List<Validator> validators = validators();

        for (TypeElement clazz : annotatedConfigs) {
            var classWrapper = new ClassWrapper(processingEnv, clazz);

            validators.forEach(validator -> validator.validate(classWrapper));

            Elements elementUtils = processingEnv.getElementUtils();

            // Get package name of the schema class
            String packageName = elementUtils.getPackageOf(clazz).getQualifiedName().toString();

            ClassName schemaClassName = ClassName.get(packageName, clazz.getSimpleName().toString());

            // Get name for generated configuration interface.
            ClassName configInterface = getConfigurationInterfaceName(schemaClassName);

            TypeSpec.Builder configurationInterfaceBuilder = TypeSpec.interfaceBuilder(configInterface)
                    .addModifiers(PUBLIC)
                    .addJavadoc("@see $T", schemaClassName);

            for (VariableElement field : classWrapper.fields()) {
                createGetters(configurationInterfaceBuilder, field);
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
                ClassWrapper superClass = classWrapper.requiredSuperClass();

                if (superClass.getAnnotation(ConfigurationRoot.class) != null) {
                    ClassName superClassSchemaClassName = ClassName.get(superClass.clazz());

                    createExtensionKeyField(configInterface, configurationInterfaceBuilder, schemaClassName, superClassSchemaClassName);
                }
            }

            // Generates "public FooConfiguration directProxy();" in the configuration interface.
            configurationInterfaceBuilder.addMethod(MethodSpec.methodBuilder("directProxy")
                    .addModifiers(PUBLIC, ABSTRACT)
                    .addAnnotation(Override.class)
                    .returns(configInterface).build()
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
        ClassName changeClassName = getChangeName(schemaClassName);

        ConfigurationRoot rootAnnotation = realSchemaClass.getAnnotation(ConfigurationRoot.class);
        ConfigurationExtension extensionAnnotation = realSchemaClass.getAnnotation(ConfigurationExtension.class);

        ParameterizedTypeName fieldTypeName
                = ParameterizedTypeName.get(ROOT_KEY_CLASSNAME, configInterface, viewClassName, changeClassName);

        FieldSpec keyField = FieldSpec.builder(fieldTypeName, "KEY", PUBLIC, STATIC, FINAL)
                .initializer(
                        "new $T($S, $T.$L, $T.class, $L)",
                        ROOT_KEY_CLASSNAME,
                        rootAnnotation.rootName(),
                        ConfigurationType.class,
                        rootAnnotation.type(),
                        realSchemaClass,
                        extensionAnnotation != null && extensionAnnotation.internal()
                )
                .build();

        configurationClassBuilder.addField(keyField);
    }

    private static void createExtensionKeyField(
            ClassName configInterface,
            Builder configurationClassBuilder,
            ClassName schemaClassName,
            ClassName superClassSchemaClassName
    ) {
        ClassName viewClassName = getViewName(schemaClassName);
        ClassName changeClassName = getChangeName(schemaClassName);

        ClassName superConfigInterface = getConfigurationInterfaceName(superClassSchemaClassName);

        ParameterizedTypeName fieldTypeName
                = ParameterizedTypeName.get(ROOT_KEY_CLASSNAME, configInterface, viewClassName, changeClassName);

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
     * @param field Original schema field.
     */
    private static void createGetters(
            Builder configurationInterfaceBuilder,
            VariableElement field
    ) {
        String fieldName = field.getSimpleName().toString();

        // Get configuration types (VIEW, CHANGE and so on)
        TypeName interfaceGetMethodType = getInterfaceGetMethodType(field);

        MethodSpec.Builder builder = MethodSpec.methodBuilder(fieldName)
                .addModifiers(PUBLIC, ABSTRACT)
                .returns(interfaceGetMethodType)
                .addJavadoc("@see $T#" + field, field.getEnclosingElement());

        if (field.getAnnotation(Deprecated.class) != null) {
            builder.addAnnotation(Deprecated.class);
        }

        MethodSpec interfaceGetMethod = builder.build();

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

        // It is necessary to use class names without loading classes so that we won't
        // accidentally get NoClassDefFoundError
        ClassName confValueClass = ClassName.get("org.apache.ignite.configuration", "ConfigurationValue");

        TypeName genericType = baseType;

        if (genericType.isPrimitive()) {
            genericType = genericType.box();
        }

        return ParameterizedTypeName.get(confValueClass, genericType);
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
                .addJavadoc("@see $T", schemaClassName)
                .addModifiers(PUBLIC);

        if (viewBaseSchemaInterfaceType != null) {
            viewClsBuilder.addSuperinterface(viewBaseSchemaInterfaceType);
        }

        TypeSpec.Builder changeClsBuilder = TypeSpec.interfaceBuilder(changeClsName)
                .addSuperinterface(viewClsName)
                .addJavadoc("@see $T", schemaClassName)
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

            boolean leafField = isValidValueAnnotationFieldType(processingEnv, schemaFieldType)
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
                    .addJavadoc("@see $T#" + field, field.getEnclosingElement())
                    .returns(viewFieldType);

            boolean isDeprecated = field.getAnnotation(Deprecated.class) != null;
            if (isDeprecated) {
                getMtdBuilder.addAnnotation(Deprecated.class);
            }

            viewClsBuilder.addMethod(getMtdBuilder.build());

            // Read only.
            if (containsAnyAnnotation(field, PolymorphicId.class, InjectedName.class, InternalId.class)) {
                continue;
            }

            String changeMtdName = "change" + capitalize(fieldName);

            MethodSpec.Builder changeMtdBuilder = MethodSpec.methodBuilder(changeMtdName)
                    .addModifiers(PUBLIC, ABSTRACT)
                    .addJavadoc("@see $T#" + field, field.getEnclosingElement())
                    .returns(changeClsName);

            if (isDeprecated) {
                changeMtdBuilder.addAnnotation(Deprecated.class);
            }

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
                        .addJavadoc("@see $T#" + field, field.getEnclosingElement())
                        .returns(changeFieldType);

                if (isDeprecated) {
                    shortChangeMtdBuilder.addAnnotation(Deprecated.class);
                }

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

    @Override
    public Set<String> getSupportedAnnotationTypes() {
        return TOP_LEVEL_ANNOTATIONS.stream().map(Class::getCanonicalName).collect(toSet());
    }

    @Override
    public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.latest();
    }
}
