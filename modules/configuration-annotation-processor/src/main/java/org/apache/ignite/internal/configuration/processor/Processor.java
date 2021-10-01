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

package org.apache.ignite.internal.configuration.processor;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.Name;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.ArrayType;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.tools.Diagnostic;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import com.squareup.javapoet.TypeVariableName;
import com.squareup.javapoet.WildcardTypeName;
import org.apache.ignite.configuration.NamedConfigurationTree;
import org.apache.ignite.configuration.NamedListChange;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.DirectAccess;
import org.apache.ignite.configuration.annotation.InternalConfiguration;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.PolymorphicConfig;
import org.apache.ignite.configuration.annotation.PolymorphicConfigInstance;
import org.apache.ignite.configuration.annotation.Value;
import org.jetbrains.annotations.Nullable;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static javax.lang.model.element.Modifier.ABSTRACT;
import static javax.lang.model.element.Modifier.FINAL;
import static javax.lang.model.element.Modifier.PUBLIC;
import static javax.lang.model.element.Modifier.STATIC;
import static org.apache.ignite.internal.configuration.processor.Utils.joinSimpleName;
import static org.apache.ignite.internal.configuration.processor.Utils.simpleName;
import static org.apache.ignite.internal.util.CollectionUtils.viewReadOnly;

/**
 * Annotation processor that produces configuration classes.
 */
public class Processor extends AbstractProcessor {
    /** Java file padding. */
    private static final String INDENT = "    ";

    /** {@link RootKey} class name. */
    private static final ClassName ROOT_KEY_CLASSNAME = ClassName.get("org.apache.ignite.configuration", "RootKey");

    /** Error format for the superclass missing annotation. */
    private static final String SUPERCLASS_MISSING_ANNOTATION_ERROR_FORMAT = "Superclass must have %s: %s";

    /** Superclass existence error format. */
    private static final String EXIST_SUPERCLASS_ERROR_FORMAT = "Class with %s should not have a superclass: %s";

    /** Superclass missing error format. */
    private static final String MISSING_SUPERCLASS_ERROR_FORMAT = "Class with %s should not have a superclass: %s";

    /** Format of class annotation incompatibility error. */
    private static final String INCOMPATIBLE_CLASS_ANNOTATION_ERROR_FORMAT = "Class with %s is not allowed with %s: %s";

    /** Error format for the presence of duplicate class fields (by names). */
    private static final String DUPLICATE_FIELD_NAMES_ERROR_FORMAT =
        "Duplicate field names are not allowed [class=%s, superClass=%s, fields=%s]";

    /** {@inheritDoc} */
    @Override public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnvironment) {
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

        if (annotatedConfigs.isEmpty())
            return false;

        for (TypeElement clazz : annotatedConfigs) {
            // Find all the fields of the schema.
            Collection<VariableElement> fields = fields(clazz);

            validate(clazz, fields);

            // Get package name of the schema class
            String packageName = elementUtils.getPackageOf(clazz).getQualifiedName().toString();

            ClassName schemaClassName = ClassName.get(packageName, clazz.getSimpleName().toString());

            // Get name for generated configuration interface.
            ClassName configInterface = Utils.getConfigurationInterfaceName(schemaClassName);

            TypeSpec.Builder configurationInterfaceBuilder = TypeSpec.interfaceBuilder(configInterface)
                .addModifiers(PUBLIC);

            for (VariableElement field : fields) {
                if (field.getModifiers().contains(STATIC))
                    continue;

                if (!field.getModifiers().contains(PUBLIC))
                    throw new ProcessorException("Field " + clazz.getQualifiedName() + "." + field + " must be public");

                Element fieldTypeElement = processingEnv.getTypeUtils().asElement(field.asType());

                String fieldName = field.getSimpleName().toString();

                // Get configuration types (VIEW, CHANGE and so on)
                TypeName interfaceGetMethodType = getInterfaceGetMethodType(field);

                ConfigValue confAnnotation = field.getAnnotation(ConfigValue.class);
                if (confAnnotation != null) {
                    if (fieldTypeElement.getAnnotation(Config.class) == null) {
                        throw new ProcessorException(
                            "Class for @ConfigValue field must be defined as @Config: " +
                                clazz.getQualifiedName() + "." + field.getSimpleName()
                        );
                    }

                    if (field.getAnnotation(DirectAccess.class) != null) {
                        throw new ProcessorException(
                            "@DirectAccess annotation must not be present on nested configuration fields"
                        );
                    }
                }

                NamedConfigValue namedConfigAnnotation = field.getAnnotation(NamedConfigValue.class);
                if (namedConfigAnnotation != null) {
                    if (fieldTypeElement.getAnnotation(Config.class) == null) {
                        throw new ProcessorException(
                            "Class for @NamedConfigValue field must be defined as @Config: " +
                                clazz.getQualifiedName() + "." + field.getSimpleName()
                        );
                    }

                    if (field.getAnnotation(DirectAccess.class) != null) {
                        throw new ProcessorException(
                            "@DirectAccess annotation must not be present on nested configuration fields"
                        );
                    }
                }

                Value valueAnnotation = field.getAnnotation(Value.class);
                if (valueAnnotation != null) {
                    // Must be a primitive or an array of the primitives (including java.lang.String)
                    if (!isPrimitiveOrArray(field.asType())) {
                        throw new ProcessorException(
                            "@Value " + clazz.getQualifiedName() + "." + field.getSimpleName() + " field must" +
                                " have one of the following types: boolean, int, long, double, String or an array of " +
                                "aforementioned type."
                        );
                    }
                }

                createGetters(configurationInterfaceBuilder, fieldName, interfaceGetMethodType);
            }

            // Is root of the configuration.
            boolean isRootConfig = clazz.getAnnotation(ConfigurationRoot.class) != null;

            // Is the internal configuration.
            boolean isInternalConfig = clazz.getAnnotation(InternalConfiguration.class) != null;

            // Is a polymorphic configuration.
            boolean isPolymorphicConfig = clazz.getAnnotation(PolymorphicConfig.class) != null;

            // Is an instance of a polymorphic configuration.
            boolean isPolymorphicInstance = clazz.getAnnotation(PolymorphicConfigInstance.class) != null;

            // Create VIEW and CHANGE classes.
            createPojoBindings(
                fields,
                schemaClassName,
                configurationInterfaceBuilder,
                (isInternalConfig && !isRootConfig) || isPolymorphicInstance,
                clazz,
                isPolymorphicConfig
            );

            if (isRootConfig)
                createRootKeyField(configInterface, configurationInterfaceBuilder, schemaClassName, clazz);

            // Write configuration interface
            buildClass(packageName, configurationInterfaceBuilder.build());
        }

        return true;
    }

    /** */
    private static void createRootKeyField(
        ClassName configInterface,
        TypeSpec.Builder configurationClassBuilder,
        ClassName schemaClassName,
        TypeElement realSchemaClass
    ) {
        ClassName viewClassName = Utils.getViewName(schemaClassName);

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
     * @param configurationInterfaceBuilder
     * @param fieldName
     * @param interfaceGetMethodType
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
     * @param field
     * @return Bundle with all types for configuration
     */
    private static TypeName getInterfaceGetMethodType(VariableElement field) {
        TypeName interfaceGetMethodType = null;

        TypeName baseType = TypeName.get(field.asType());

        ConfigValue confAnnotation = field.getAnnotation(ConfigValue.class);
        if (confAnnotation != null)
            interfaceGetMethodType = Utils.getConfigurationInterfaceName((ClassName) baseType);

        NamedConfigValue namedConfigAnnotation = field.getAnnotation(NamedConfigValue.class);
        if (namedConfigAnnotation != null) {
            ClassName interfaceGetType = Utils.getConfigurationInterfaceName((ClassName) baseType);

            TypeName viewClassType = Utils.getViewName((ClassName) baseType);
            TypeName changeClassType = Utils.getChangeName((ClassName) baseType);

            interfaceGetMethodType = ParameterizedTypeName.get(ClassName.get(NamedConfigurationTree.class), interfaceGetType, viewClassType, changeClassType);
        }

        Value valueAnnotation = field.getAnnotation(Value.class);
        if (valueAnnotation != null) {
            // It is necessary to use class names without loading classes so that we won't
            // accidentally get NoClassDefFoundError
            ClassName confValueClass = ClassName.get("org.apache.ignite.configuration", "ConfigurationValue");

            TypeName genericType = baseType;

            if (genericType.isPrimitive())
                genericType = genericType.box();

            interfaceGetMethodType = ParameterizedTypeName.get(confValueClass, genericType);
        }

        return interfaceGetMethodType;
    }

    /**
     * Create VIEW and CHANGE classes and methods.
     *
     * @param fields List of configuration fields.
     * @param schemaClassName Class name of schema.
     * @param configurationInterfaceBuilder Configuration interface builder.
     * @param extendBaseSchema {@code true} if extending base schema interfaces.
     * @param realSchemaClass Class descriptor.
     * @param isPolymorphicConfig Is a polymorphic configuration.
     */
    private void createPojoBindings(
        Collection<VariableElement> fields,
        ClassName schemaClassName,
        TypeSpec.Builder configurationInterfaceBuilder,
        boolean extendBaseSchema,
        TypeElement realSchemaClass,
        boolean isPolymorphicConfig
    ) {
        ClassName viewClsName = Utils.getViewName(schemaClassName);
        ClassName changeClsName = Utils.getChangeName(schemaClassName);

        TypeName configInterfaceType;
        @Nullable TypeName viewBaseSchemaInterfaceType;
        @Nullable TypeName changeBaseSchemaInterfaceType;

        if (extendBaseSchema) {
            DeclaredType superClassType = (DeclaredType)realSchemaClass.getSuperclass();
            ClassName superClassSchemaClassName = ClassName.get((TypeElement)superClassType.asElement());

            configInterfaceType = Utils.getConfigurationInterfaceName(superClassSchemaClassName);
            viewBaseSchemaInterfaceType = Utils.getViewName(superClassSchemaClassName);
            changeBaseSchemaInterfaceType = Utils.getChangeName(superClassSchemaClassName);
        }
        else {
            ClassName confTreeInterface = ClassName.get("org.apache.ignite.configuration", "ConfigurationTree");
            configInterfaceType = ParameterizedTypeName.get(confTreeInterface, viewClsName, changeClsName);

            viewBaseSchemaInterfaceType = null;
            changeBaseSchemaInterfaceType = null;
        }

        configurationInterfaceBuilder.addSuperinterface(configInterfaceType);

        // This code will be refactored in the future. Right now I don't want to entangle it with existing code
        // generation. It has only a few considerable problems - hardcode and a lack of proper arrays handling.
        // Clone method should be used to guarantee data integrity.

        TypeSpec.Builder viewClsBuilder = TypeSpec.interfaceBuilder(viewClsName)
            .addModifiers(PUBLIC);

        if (viewBaseSchemaInterfaceType != null)
            viewClsBuilder.addSuperinterface(viewBaseSchemaInterfaceType);

        TypeSpec.Builder changeClsBuilder = TypeSpec.interfaceBuilder(changeClsName)
            .addSuperinterface(viewClsName)
            .addModifiers(PUBLIC);

        if (changeBaseSchemaInterfaceType != null)
            changeClsBuilder.addSuperinterface(changeBaseSchemaInterfaceType);

        ClassName consumerClsName = ClassName.get(Consumer.class);

        for (VariableElement field : fields) {
            Value valAnnotation = field.getAnnotation(Value.class);

            String fieldName = field.getSimpleName().toString();
            TypeMirror schemaFieldType = field.asType();
            TypeName schemaFieldTypeName = TypeName.get(schemaFieldType);

            boolean leafField = isPrimitiveOrArray(schemaFieldType)
                || !((ClassName)schemaFieldTypeName).simpleName().contains("ConfigurationSchema");

            boolean namedListField = field.getAnnotation(NamedConfigValue.class) != null;

            TypeName viewFieldType =
                leafField ? schemaFieldTypeName : Utils.getViewName((ClassName)schemaFieldTypeName);

            TypeName changeFieldType =
                leafField ? schemaFieldTypeName : Utils.getChangeName((ClassName)schemaFieldTypeName);

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

            {
                MethodSpec.Builder getMtdBuilder = MethodSpec.methodBuilder(fieldName)
                    .addModifiers(PUBLIC, ABSTRACT)
                    .returns(viewFieldType);

                viewClsBuilder.addMethod(getMtdBuilder.build());
            }

            {
                String changeMtdName = "change" + capitalize(fieldName);

                {
                    MethodSpec.Builder changeMtdBuilder = MethodSpec.methodBuilder(changeMtdName)
                        .addModifiers(PUBLIC, ABSTRACT)
                        .returns(changeClsName);

                    if (valAnnotation != null) {
                        if (schemaFieldType.getKind() == TypeKind.ARRAY)
                            changeMtdBuilder.varargs(true);

                        changeMtdBuilder.addParameter(changeFieldType, fieldName);
                    }
                    else
                        changeMtdBuilder.addParameter(ParameterizedTypeName.get(consumerClsName, changeFieldType), fieldName);

                    changeClsBuilder.addMethod(changeMtdBuilder.build());
                }
            }
        }

        if (isPolymorphicConfig) {
            // Parameter type: Class<T>.
            ParameterizedTypeName parameterType = ParameterizedTypeName.get(
                ClassName.get(Class.class),
                TypeVariableName.get("T")
            );

            // Variable type, for example: <T extends SimpleChange>.
            TypeVariableName typeVariable = TypeVariableName.get("T", changeClsName);

            // Method like: <T extends SimpleChange> T convert(Class<T> changeClass);
            MethodSpec.Builder convertMtdBuilder = MethodSpec.methodBuilder("convert")
                .addModifiers(PUBLIC, ABSTRACT)
                .addTypeVariable(typeVariable)
                .addParameter(parameterType, "changeClass")
                .returns(TypeVariableName.get("T"));

            changeClsBuilder.addMethod(convertMtdBuilder.build());
        }
        
        TypeSpec viewCls = viewClsBuilder.build();
        TypeSpec changeCls = changeClsBuilder.build();

        buildClass(viewClsName.packageName(), viewCls);
        buildClass(changeClsName.packageName(), changeCls);
    }

    /** */
    private void buildClass(String packageName, TypeSpec cls) {
        try {
            JavaFile.builder(packageName, cls)
                .indent(INDENT)
                .build()
                .writeTo(processingEnv.getFiler());
        }
        catch (IOException e) {
            throw new ProcessorException("Failed to generate class " + packageName + "." + cls.name, e);
        }
    }

    /** */
    private static String capitalize(String name) {
        return name.substring(0, 1).toUpperCase() + name.substring(1);
    }

    /**
     * Checks whether the given type is a primitive (or String) or an array of primitives (or Strings).
     *
     * @param type type
     * @return {@code true} if type is a primitive or a String or an array of primitives or Strings
     */
    private boolean isPrimitiveOrArray(TypeMirror type) {
        if (type.getKind() == TypeKind.ARRAY)
            type = ((ArrayType) type).getComponentType();

        if (type.getKind().isPrimitive())
            return true;

        TypeMirror stringType = processingEnv
            .getElementUtils()
            .getTypeElement(String.class.getCanonicalName())
            .asType();

        return processingEnv.getTypeUtils().isSameType(type, stringType);
    }

    /**
     * Check if a class type is {@link Object}.
     *
     * @param type Class type.
     * @return {@code true} if class type is {@link Object}.
     */
    private boolean isObjectClass(TypeMirror type) {
        TypeMirror objectType = processingEnv
            .getElementUtils()
            .getTypeElement(Object.class.getCanonicalName())
            .asType();

        return objectType.equals(type);
    }

    /**
     * Get class fields.
     *
     * @param type Class type.
     * @return Class fields.
     */
    private static Collection<VariableElement> fields(TypeElement type) {
        return type.getEnclosedElements().stream()
            .filter(el -> el.getKind() == ElementKind.FIELD)
            .map(VariableElement.class::cast)
            .collect(toList());
    }

    /**
     * Validate the class.
     *
     * @param clazz Class type.
     * @param fields Class fields.
     * @throws ProcessorException If the class validation fails.
     */
    private void validate(TypeElement clazz, Collection<VariableElement> fields) {
        if (clazz.getAnnotation(InternalConfiguration.class) != null) {
            Annotation incompatible =
                findFirst(clazz, Config.class, PolymorphicConfig.class, PolymorphicConfigInstance.class);

            if (incompatible != null) {
                throw new ProcessorException(String.format(
                    INCOMPATIBLE_CLASS_ANNOTATION_ERROR_FORMAT,
                    simpleName(incompatible.getClass()),
                    simpleName(InternalConfiguration.class),
                    clazz.getQualifiedName()
                ));
            }
            else if (clazz.getAnnotation(ConfigurationRoot.class) != null) {
                if (!isObjectClass(clazz.getSuperclass())) {
                    throw new ProcessorException(String.format(
                        EXIST_SUPERCLASS_ERROR_FORMAT,
                        joinSimpleName(" and ", ConfigurationRoot.class, InternalConfiguration.class),
                        clazz.getQualifiedName()
                    ));
                }
            }
            else if (isObjectClass(clazz.getSuperclass())) {
                throw new ProcessorException(String.format(
                    MISSING_SUPERCLASS_ERROR_FORMAT,
                    simpleName(InternalConfiguration.class),
                    clazz.getQualifiedName()
                ));
            }
            else {
                TypeElement superClazz = superClass(clazz);

                if (superClazz.getAnnotation(InternalConfiguration.class) != null) {
                    throw new ProcessorException(String.format(
                        "Superclass must not have %s: %s",
                        simpleName(InternalConfiguration.class),
                        clazz.getQualifiedName()
                    ));
                }
                else if (superClazz.getAnnotation(ConfigurationRoot.class) == null &&
                    superClazz.getAnnotation(Config.class) == null) {
                    throw new ProcessorException(String.format(
                        SUPERCLASS_MISSING_ANNOTATION_ERROR_FORMAT,
                        joinSimpleName(" or ", ConfigurationRoot.class, Config.class),
                        clazz.getQualifiedName()
                    ));
                }
                else {
                    Collection<Name> duplicateFieldNames = duplicates(fields(superClazz), fields);

                    if (!duplicateFieldNames.isEmpty()) {
                        throw new ProcessorException(String.format(
                            DUPLICATE_FIELD_NAMES_ERROR_FORMAT,
                            clazz.getQualifiedName(),
                            superClazz.getQualifiedName(),
                            duplicateFieldNames
                        ));
                    }
                }
            }
        }
        else if (clazz.getAnnotation(PolymorphicConfig.class) != null) {
            Annotation incompatible =
                findFirst(clazz, ConfigurationRoot.class, Config.class, PolymorphicConfigInstance.class);

            if (incompatible != null) {
                throw new ProcessorException(String.format(
                    INCOMPATIBLE_CLASS_ANNOTATION_ERROR_FORMAT,
                    simpleName(incompatible.getClass()),
                    simpleName(PolymorphicConfig.class),
                    clazz.getQualifiedName()
                ));
            }
            else if (!isObjectClass(clazz.getSuperclass())) {
                throw new ProcessorException(String.format(
                    EXIST_SUPERCLASS_ERROR_FORMAT,
                    simpleName(PolymorphicConfig.class),
                    clazz.getQualifiedName()
                ));
            }
            else {
                String fieldName = clazz.getAnnotation(PolymorphicConfig.class).fieldName();

                if (fields.stream().anyMatch(f -> f.getSimpleName().contentEquals(fieldName))) {
                    throw new ProcessorException(String.format(
                        "Field name '%s' is reserved for %s: %s",
                        fieldName,
                        simpleName(PolymorphicConfig.class),
                        clazz.getQualifiedName()
                    ));
                }
            }
        }
        else if (clazz.getAnnotation(PolymorphicConfigInstance.class) != null) {
            Annotation incompatible = findFirst(clazz, ConfigurationRoot.class, Config.class);

            if (incompatible != null) {
                throw new ProcessorException(String.format(
                    INCOMPATIBLE_CLASS_ANNOTATION_ERROR_FORMAT,
                    simpleName(incompatible.getClass()),
                    simpleName(PolymorphicConfigInstance.class),
                    clazz.getQualifiedName()
                ));
            }
            else if (isObjectClass(clazz.getSuperclass())) {
                throw new ProcessorException(String.format(
                    MISSING_SUPERCLASS_ERROR_FORMAT,
                    simpleName(PolymorphicConfigInstance.class),
                    clazz.getQualifiedName()
                ));
            }
            else if (superClass(clazz).getAnnotation(PolymorphicConfig.class) == null) {
                throw new ProcessorException(String.format(
                    SUPERCLASS_MISSING_ANNOTATION_ERROR_FORMAT,
                    simpleName(PolymorphicConfig.class),
                    clazz.getQualifiedName())
                );
            }
            else {
                TypeElement superClazz = superClass(clazz);

                Collection<Name> duplicateFieldNames = duplicates(fields(superClazz), fields);

                if (!duplicateFieldNames.isEmpty()) {
                    throw new ProcessorException(String.format(
                        DUPLICATE_FIELD_NAMES_ERROR_FORMAT,
                        clazz.getQualifiedName(),
                        superClazz.getQualifiedName(),
                        duplicateFieldNames
                    ));
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public Set<String> getSupportedAnnotationTypes() {
        return Set.copyOf(viewReadOnly(supportedAnnotationTypes(), Class::getCanonicalName));
    }

    /** {@inheritDoc} */
    @Override public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.latest();
    }

    /**
     * @return Annotation types supported by this processor.
     */
    private Set<Class<? extends Annotation>> supportedAnnotationTypes() {
        return Set.of(
            Config.class,
            ConfigurationRoot.class,
            InternalConfiguration.class,
            PolymorphicConfig.class,
            PolymorphicConfigInstance.class
        );
    }

    /**
     * Getting a superclass.
     *
     * @param clazz Class type.
     * @return Superclass type.
     */
    private TypeElement superClass(TypeElement clazz) {
        return processingEnv.getElementUtils().getTypeElement(clazz.getSuperclass().toString());
    }

    /**
     * Returns the first annotation found for the class.
     *
     * @param clazz Class type.
     * @param annotationClasses Annotation classes that will be searched for the class.
     * @return First annotation found.
     */
    @SafeVarargs
    @Nullable public static Annotation findFirst(
        TypeElement clazz,
        Class<? extends Annotation>... annotationClasses
    ) {
        return Stream.of(annotationClasses).map(clazz::getAnnotation).filter(Objects::nonNull).findAny().orElse(null);
    }

    /**
     * Search for duplicate class fields by name.
     *
     * @param fields1 First class fields.
     * @param fields2 Second class fields.
     * @return Field names.
     */
    public static Collection<Name> duplicates(
        Collection<VariableElement> fields1,
        Collection<VariableElement> fields2
    ) {
        if (fields1.isEmpty() || fields2.isEmpty())
            return List.of();

        Set<Name> filedNames1 = fields1.stream()
            .map(VariableElement::getSimpleName)
            .collect(toSet());

        return fields2.stream()
            .map(VariableElement::getSimpleName)
            .filter(filedNames1::contains)
            .collect(toList());
    }
}
