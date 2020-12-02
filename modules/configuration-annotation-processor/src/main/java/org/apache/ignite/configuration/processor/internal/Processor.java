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

package org.apache.ignite.configuration.processor.internal;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Filer;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.util.Elements;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import com.squareup.javapoet.WildcardTypeName;
import org.apache.ignite.configuration.internal.Configurator;
import org.apache.ignite.configuration.internal.DynamicConfiguration;
import org.apache.ignite.configuration.internal.NamedListConfiguration;
import org.apache.ignite.configuration.internal.annotation.Config;
import org.apache.ignite.configuration.internal.annotation.ConfigValue;
import org.apache.ignite.configuration.internal.annotation.NamedConfigValue;
import org.apache.ignite.configuration.internal.annotation.Value;
import org.apache.ignite.configuration.internal.property.DynamicProperty;
import org.apache.ignite.configuration.internal.selector.BaseSelectors;
import org.apache.ignite.configuration.internal.selector.Selector;
import org.apache.ignite.configuration.internal.validation.MemberKey;
import org.apache.ignite.configuration.processor.internal.pojo.ChangeClassGenerator;
import org.apache.ignite.configuration.processor.internal.pojo.InitClassGenerator;
import org.apache.ignite.configuration.processor.internal.pojo.ViewClassGenerator;
import org.apache.ignite.configuration.processor.internal.validation.ValidationGenerator;

import static javax.lang.model.element.Modifier.FINAL;
import static javax.lang.model.element.Modifier.PRIVATE;
import static javax.lang.model.element.Modifier.PUBLIC;
import static javax.lang.model.element.Modifier.STATIC;

/**
 * Annotation processor that produces configuration classes.
 */
public class Processor extends AbstractProcessor {
    /** Generator of VIEW classes. */
    private ViewClassGenerator viewClassGenerator;

    /** Generator of INIT classes. */
    private InitClassGenerator initClassGenerator;

    /** Generator of CHANGE classes. */
    private ChangeClassGenerator changeClassGenerator;

    /** Class file writer. */
    private Filer filer;

    /** {@inheritDoc} */
    @Override public synchronized void init(ProcessingEnvironment processingEnv) {
        super.init(processingEnv);
        filer = processingEnv.getFiler();
        viewClassGenerator = new ViewClassGenerator(processingEnv);
        initClassGenerator = new InitClassGenerator(processingEnv);
        changeClassGenerator = new ChangeClassGenerator(processingEnv);
    }

    /** {@inheritDoc} */
    @Override public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnvironment) {
        final Elements elementUtils = processingEnv.getElementUtils();

        Map<TypeName, ConfigurationDescription> props = new HashMap<>();

        List<ConfigurationDescription> roots = new ArrayList<>();

        String packageForUtil = "";

        final Set<? extends Element> annotatedConfigs = roundEnvironment.getElementsAnnotatedWith(Config.class);

        if (annotatedConfigs.isEmpty())
            return false;

        for (Element element : annotatedConfigs) {
            if (element.getKind() != ElementKind.CLASS) {
                continue;
            }
            TypeElement clazz = (TypeElement) element;

            final PackageElement elementPackage = elementUtils.getPackageOf(clazz);
            final String packageName = elementPackage.getQualifiedName().toString();

            final List<VariableElement> fields
                = clazz.getEnclosedElements().stream()
                .filter(el -> el.getKind() == ElementKind.FIELD)
                .map(el -> (VariableElement) el)
                .collect(Collectors.toList());

            final Config clazzConfigAnnotation = clazz.getAnnotation(Config.class);

            final String configName = clazzConfigAnnotation.value();
            final boolean isRoot = clazzConfigAnnotation.root();
            final ClassName schemaClassName = ClassName.get(packageName, clazz.getSimpleName().toString());
            final ClassName configClass = Utils.getConfigurationName(schemaClassName);

            ConfigurationDescription configDesc = new ConfigurationDescription(configClass, configName, Utils.getViewName(schemaClassName), Utils.getInitName(schemaClassName), Utils.getChangeName(schemaClassName));

            if (isRoot) {
                roots.add(configDesc);
                packageForUtil = packageName;
            }

            TypeSpec.Builder configurationClassBuilder = TypeSpec
                .classBuilder(configClass)
                .addModifiers(PUBLIC, FINAL);
            TypeName wildcard = WildcardTypeName.subtypeOf(Object.class);

            final ParameterizedTypeName configuratorClassName = ParameterizedTypeName.get(
                ClassName.get(Configurator.class),
                WildcardTypeName.subtypeOf(ParameterizedTypeName.get(ClassName.get(DynamicConfiguration.class), wildcard, wildcard, wildcard))
            );

            CodeBlock.Builder constructorBodyBuilder = CodeBlock.builder();
            CodeBlock.Builder copyConstructorBodyBuilder = CodeBlock.builder();

            for (VariableElement field : fields) {
                TypeName getMethodType = null;

                final TypeName baseType = TypeName.get(field.asType());
                final String fieldName = field.getSimpleName().toString();

                TypeName unwrappedType = baseType;
                TypeName viewClassType = baseType;
                TypeName initClassType = baseType;
                TypeName changeClassType = baseType;

                final ConfigValue confAnnotation = field.getAnnotation(ConfigValue.class);
                if (confAnnotation != null) {
                    getMethodType = Utils.getConfigurationName((ClassName) baseType);

                    final FieldSpec nestedConfigField =
                        FieldSpec
                            .builder(getMethodType, fieldName, Modifier.PRIVATE, FINAL)
                            .build();

                    configurationClassBuilder.addField(nestedConfigField);

                    constructorBodyBuilder.addStatement("add($L = new $T(qualifiedName, $S, false, configurator, this.root))", fieldName, getMethodType, fieldName);
                    copyConstructorBodyBuilder.addStatement("add($L = base.$L.copy(this.root))", fieldName, fieldName);

                    unwrappedType = getMethodType;
                    viewClassType = Utils.getViewName((ClassName) baseType);
                    initClassType = Utils.getInitName((ClassName) baseType);
                    changeClassType = Utils.getChangeName((ClassName) baseType);
                }

                final NamedConfigValue namedConfigAnnotation = field.getAnnotation(NamedConfigValue.class);
                if (namedConfigAnnotation != null) {
                    ClassName fieldType = Utils.getConfigurationName((ClassName) baseType);

                    viewClassType = Utils.getViewName((ClassName) baseType);
                    initClassType = Utils.getInitName((ClassName) baseType);
                    changeClassType = Utils.getChangeName((ClassName) baseType);

                    getMethodType = ParameterizedTypeName.get(ClassName.get(NamedListConfiguration.class), viewClassType, fieldType, initClassType, changeClassType);

                    final FieldSpec nestedConfigField =
                        FieldSpec
                            .builder(getMethodType, fieldName, Modifier.PRIVATE, FINAL)
                            .build();

                    configurationClassBuilder.addField(nestedConfigField);

                    constructorBodyBuilder.addStatement(
                        "add($L = new $T(qualifiedName, $S, configurator, this.root, (p, k) -> new $T(p, k, true, configurator, this.root)))",
                        fieldName,
                        getMethodType,
                        fieldName,
                        fieldType
                    );

                    copyConstructorBodyBuilder.addStatement("add($L = base.$L.copy(this.root))", fieldName, fieldName);
                }

                final Value valueAnnotation = field.getAnnotation(Value.class);
                if (valueAnnotation != null) {
                    ClassName dynPropClass = ClassName.get(DynamicProperty.class);

                    TypeName genericType = baseType;

                    if (genericType.isPrimitive()) {
                        genericType = genericType.box();
                    }

                    getMethodType = ParameterizedTypeName.get(dynPropClass, genericType);

                    final FieldSpec generatedField = FieldSpec.builder(getMethodType, fieldName, Modifier.PRIVATE, FINAL).build();

                    configurationClassBuilder.addField(generatedField);

                    final CodeBlock validatorsBlock = ValidationGenerator.generateValidators(field);

                    constructorBodyBuilder.addStatement(
                        "add($L = new $T(qualifiedName, $S, new $T($T.class, $S), this.configurator, this.root), $L)",
                        fieldName, getMethodType, fieldName, MemberKey.class, configClass, fieldName, validatorsBlock
                    );

                    copyConstructorBodyBuilder.addStatement("add($L = base.$L.copy(this.root))", fieldName, fieldName);
                }

                configDesc.getFields().add(new ConfigurationElement(getMethodType, fieldName, viewClassType, initClassType, changeClassType));

                MethodSpec getMethod = MethodSpec
                    .methodBuilder(fieldName)
                    .addModifiers(PUBLIC, FINAL)
                    .returns(getMethodType)
                    .addStatement("return $L", fieldName)
                    .build();
                configurationClassBuilder.addMethod(getMethod);

                if (valueAnnotation != null) {
                    MethodSpec setMethod = MethodSpec
                        .methodBuilder(fieldName)
                        .addModifiers(PUBLIC, FINAL)
                        .addParameter(unwrappedType, fieldName)
                        .addStatement("this.$L.change($L)", fieldName, fieldName)
                        .build();
                    configurationClassBuilder.addMethod(setMethod);
                }
            }

            props.put(configClass, configDesc);

            createPojoBindings(packageName, fields, schemaClassName, configurationClassBuilder);

            createConstructors(configClass, configName, configurationClassBuilder, configuratorClassName, constructorBodyBuilder, copyConstructorBodyBuilder);

            createCopyMethod(configClass, configurationClassBuilder);

            JavaFile classFile = JavaFile.builder(packageName, configurationClassBuilder.build()).build();
            try {
                classFile.writeTo(filer);
            } catch (IOException e) {
                throw new ProcessorException("Failed to create configuration class " + configClass.toString(), e);
            }
        }
        final List<ConfigurationNode> flattenConfig = roots.stream().map((ConfigurationDescription cfg) -> buildConfigForest(cfg, props)).flatMap(Set::stream).collect(Collectors.toList());

        createKeysClass(packageForUtil, flattenConfig);

        createSelectorsClass(packageForUtil, flattenConfig);

        return true;
    }

    /**
     * Create copy-method for configuration class.
     *
     * @param configClass Configuration class name.
     * @param configurationClassBuilder Configuration class builder.
     */
    private void createCopyMethod(ClassName configClass, TypeSpec.Builder configurationClassBuilder) {
        MethodSpec copyMethod = MethodSpec.methodBuilder("copy")
            .addAnnotation(Override.class)
            .addModifiers(PUBLIC)
            .addParameter(DynamicConfiguration.class, "root")
            .returns(configClass)
            .addStatement("return new $T(this, root)", configClass)
            .build();

        configurationClassBuilder.addMethod(copyMethod);
    }

    /**
     * Create configuration class constructors.
     *
     * @param configClass Configuration class name.
     * @param configName Configuration name.
     * @param configurationClassBuilder Configuration class builder.
     * @param configuratorClassName Configurator (configuration wrapper) class name.
     * @param constructorBodyBuilder Constructor body.
     * @param copyConstructorBodyBuilder Copy constructor body.
     */
    private void createConstructors(
        ClassName configClass,
        String configName,
        TypeSpec.Builder configurationClassBuilder,
        ParameterizedTypeName configuratorClassName,
        CodeBlock.Builder constructorBodyBuilder,
        CodeBlock.Builder copyConstructorBodyBuilder
    ) {
        final MethodSpec constructorWithName = MethodSpec.constructorBuilder()
            .addModifiers(PUBLIC)
            .addParameter(String.class, "prefix")
            .addParameter(String.class, "key")
            .addParameter(boolean.class, "isNamed")
            .addParameter(configuratorClassName, "configurator")
            .addParameter(DynamicConfiguration.class, "root")
            .addStatement("super(prefix, key, isNamed, configurator, root)")
            .addCode(constructorBodyBuilder.build())
            .build();
        configurationClassBuilder.addMethod(constructorWithName);

        final MethodSpec copyConstructor = MethodSpec.constructorBuilder()
            .addModifiers(PRIVATE)
            .addParameter(configClass, "base")
            .addParameter(DynamicConfiguration.class, "root")
            .addStatement("super(base.prefix, base.key, base.isNamed, base.configurator, root)")
            .addCode(copyConstructorBodyBuilder.build())
            .build();
        configurationClassBuilder.addMethod(copyConstructor);

        final MethodSpec emptyConstructor = MethodSpec.constructorBuilder()
                .addModifiers(PUBLIC)
                .addParameter(configuratorClassName, "configurator")
                .addStatement("this($S, $S, false, configurator, null)", "", configName)
                .build();

        configurationClassBuilder.addMethod(emptyConstructor);
    }

    /**
     * Create selectors.
     *
     * @param packageForUtil Package to place selectors class to.
     * @param flattenConfig List of configuration nodes.
     */
    private void createSelectorsClass(String packageForUtil, List<ConfigurationNode> flattenConfig) {
        ClassName selectorsClassName = ClassName.get(packageForUtil, "Selectors");

        final TypeSpec.Builder selectorsClass = TypeSpec.classBuilder(selectorsClassName).superclass(BaseSelectors.class).addModifiers(PUBLIC, FINAL);

        final CodeBlock.Builder selectorsStaticBlockBuilder = CodeBlock.builder();
        selectorsStaticBlockBuilder.addStatement("$T publicLookup = $T.publicLookup()", MethodHandles.Lookup.class, MethodHandles.class);

        selectorsStaticBlockBuilder
            .beginControlFlow("try");

        flattenConfig.forEach(configNode -> {
            String regex = "([a-z])([A-Z]+)";
            String replacement = "$1_$2";

            final String varName = configNode.name
                .replaceAll(regex, replacement)
                .toUpperCase()
                .replace(".", "_");

            TypeName t = configNode.type;

            if (Utils.isNamedConfiguration(t))
                t = Utils.unwrapNamedListConfigurationClass(t);

            StringBuilder methodCall = new StringBuilder();

            ConfigurationNode current = configNode;
            ConfigurationNode root = null;
            int namedCount = 0;

            while (current != null) {
                boolean isNamed = false;
                if (Utils.isNamedConfiguration(current.type)) {
                    namedCount++;
                    isNamed = true;
                }

                if (current.getParent() != null) {
                    String newMethodCall = "." + current.getOriginalName() + "()";

                    if (isNamed)
                        newMethodCall += ".get(name" + (namedCount - 1) + ")";

                    methodCall.insert(0, newMethodCall);
                }
                else
                    root = current;

                current = current.getParent();
            }

            TypeName selectorRec = Utils.getParameterized(ClassName.get(Selector.class), root.type, t, configNode.view, configNode.init, configNode.change);

            if (namedCount > 0) {
                final MethodSpec.Builder builder = MethodSpec.methodBuilder(varName);

                for (int i = 0; i < namedCount; i++) {
                    builder.addParameter(String.class, "name" + i);
                }

                selectorsClass.addMethod(
                    builder
                        .returns(selectorRec)
                        .addModifiers(PUBLIC, STATIC, FINAL)
                        .addStatement("return (root) -> root$L", methodCall.toString())
                        .build()
                );

                final String collect = IntStream.range(0, namedCount).mapToObj(i -> "$T.class").collect(Collectors.joining(","));
                List<Object> params = new ArrayList<>();
                params.add(MethodHandle.class);
                params.add(varName);
                params.add(selectorsClassName);
                params.add(varName);
                params.add(MethodType.class);
                params.add(Selector.class);
                for (int i = 0; i < namedCount; i++) {
                    params.add(String.class);
                }

                selectorsStaticBlockBuilder.addStatement("$T $L = publicLookup.findStatic($T.class, $S, $T.methodType($T.class, " + collect + "))", params.toArray());

                selectorsStaticBlockBuilder.addStatement("put($S, $L)", configNode.name, varName);
            } else {
                selectorsClass.addField(
                    FieldSpec.builder(selectorRec, varName)
                        .addModifiers(PUBLIC, STATIC, FINAL)
                        .initializer("(root) -> root$L", methodCall.toString())
                        .build()
                );
                selectorsStaticBlockBuilder.addStatement("put($S, $L)", configNode.name, varName);
            }
        });

        selectorsStaticBlockBuilder
            .nextControlFlow("catch ($T e)", Exception.class)
            .endControlFlow();

        selectorsClass.addStaticBlock(selectorsStaticBlockBuilder.build());

        JavaFile selectorsClassFile = JavaFile.builder(selectorsClassName.packageName(), selectorsClass.build()).build();
        try {
            selectorsClassFile.writeTo(filer);
        }
        catch (IOException e) {
            throw new ProcessorException("Failed to write class: " + e.getMessage(), e);
        }
    }

    /**
     * Create keys class.
     *
     * @param packageForUtil Package to place keys class to.
     * @param flattenConfig List of configuration nodes.
     */
    private void createKeysClass(String packageForUtil, List<ConfigurationNode> flattenConfig) {
        final TypeSpec.Builder keysClass = TypeSpec.classBuilder("Keys").addModifiers(PUBLIC, FINAL);

        flattenConfig.forEach(s -> {
            final String varName = s.name.toUpperCase().replace(".", "_");
            keysClass.addField(
                FieldSpec.builder(String.class, varName)
                    .addModifiers(PUBLIC, STATIC, FINAL)
                    .initializer("$S", s.name)
                    .build()
            );
        });

        JavaFile keysClassFile = JavaFile.builder(packageForUtil, keysClass.build()).build();
        try {
            keysClassFile.writeTo(filer);
        } catch (IOException e) {
            throw new ProcessorException("Failed to write class: " + e.getMessage(), e);
        }
    }

    /**
     * Create VIEW, INIT and CHANGE classes and methods.
     *
     * @param packageName Configuration package name.
     * @param fields List of configuration fields.
     * @param schemaClassName Class name of schema.
     * @param configurationClassBuilder Configuration class builder.
     */
    private void createPojoBindings(String packageName, List<VariableElement> fields, ClassName schemaClassName, TypeSpec.Builder configurationClassBuilder) {
        final ClassName viewClassTypeName = Utils.getViewName(schemaClassName);
        final ClassName initClassName = Utils.getInitName(schemaClassName);
        final ClassName changeClassName = Utils.getChangeName(schemaClassName);

        try {
            viewClassGenerator.create(packageName, viewClassTypeName, fields);
            ClassName dynConfClass = ClassName.get(DynamicConfiguration.class);
            TypeName dynConfViewClassType = ParameterizedTypeName.get(dynConfClass, viewClassTypeName, initClassName, changeClassName);
            configurationClassBuilder.superclass(dynConfViewClassType);
            final MethodSpec toViewMethod = createToViewMethod(viewClassTypeName, fields);
            configurationClassBuilder.addMethod(toViewMethod);
        }
        catch (IOException e) {
            throw new ProcessorException("Failed to write class " + viewClassTypeName.toString(), e);
        }

        try {
            changeClassGenerator.create(packageName, changeClassName, fields);
            final MethodSpec changeMethod = createChangeMethod(changeClassName, fields);
            configurationClassBuilder.addMethod(changeMethod);
        }
        catch (IOException e) {
            throw new ProcessorException("Failed to write class " + changeClassName.toString(), e);
        }

        try {
            initClassGenerator.create(packageName, initClassName, fields);
            final MethodSpec initMethod = createInitMethod(initClassName, fields);
            configurationClassBuilder.addMethod(initMethod);
        }
        catch (IOException e) {
            throw new ProcessorException("Failed to write class " + initClassName.toString(), e);
        }
    }

    /**
     * Build configuration forest base on root configuration description and all processed configurations.
     *
     * @param root Root configuration description.
     * @param props All configurations.
     * @return All possible config trees.
     */
    private Set<ConfigurationNode> buildConfigForest(ConfigurationDescription root, Map<TypeName, ConfigurationDescription> props) {
        Set<ConfigurationNode> res = new HashSet<>();
        Deque<ConfigurationNode> propsStack = new LinkedList<>();

        ConfigurationNode rootNode = new ConfigurationNode(root.type, root.name, root.name, root.view, root.init, root.change, null);

        propsStack.addFirst(rootNode);

        while (!propsStack.isEmpty()) {
            final ConfigurationNode current = propsStack.pollFirst();

            TypeName type = current.type;

            if (Utils.isNamedConfiguration(type))
                type = Utils.unwrapNamedListConfigurationClass(current.type);

            final ConfigurationDescription configDesc = props.get(type);
            final List<ConfigurationElement> propertiesList = configDesc.getFields();

            if (current.name != null && !current.name.isEmpty())
                res.add(current);

            for (ConfigurationElement property : propertiesList) {
                String qualifiedName = property.name;

                if (current.name != null && !current.name.isEmpty())
                    qualifiedName = current.name + "." + qualifiedName;

                final ConfigurationNode newChainElement = new ConfigurationNode(
                    property.type,
                    qualifiedName,
                    property.name,
                    property.view,
                    property.init,
                    property.change,
                    current
                );

                boolean isNamedConfig = false;
                if (property.type instanceof ParameterizedTypeName) {
                    final ParameterizedTypeName parameterized = (ParameterizedTypeName) property.type;

                    if (parameterized.rawType.equals(ClassName.get(NamedListConfiguration.class)))
                        isNamedConfig = true;
                }

                if (props.containsKey(property.type) || isNamedConfig)
                    propsStack.add(newChainElement);
                else
                    res.add(newChainElement);

            }
        }
        return res;
    }

    /**
     * Create {@link org.apache.ignite.configuration.internal.property.Modifier#toView()} method for configuration class.
     *
     * @param type VIEW method type.
     * @param variables List of VIEW object's fields.
     * @return toView() method.
     */
    public MethodSpec createToViewMethod(TypeName type, List<VariableElement> variables) {
        String args = variables.stream()
            .map(v -> v.getSimpleName().toString() + ".toView()")
            .collect(Collectors.joining(", "));

        final CodeBlock returnBlock = CodeBlock.builder()
            .add("return new $T($L)", type, args)
            .build();

        return MethodSpec.methodBuilder("toView")
            .addModifiers(PUBLIC)
            .addAnnotation(Override.class)
            .returns(type)
            .addStatement(returnBlock)
            .build();
    }

    /**
     * Create {@link org.apache.ignite.configuration.internal.property.Modifier#init(Object)} method (accepts INIT object) for configuration class.
     *
     * @param type INIT method type.
     * @param variables List of INIT object's fields.
     * @return Init method.
     */
    public MethodSpec createInitMethod(TypeName type, List<VariableElement> variables) {
        final CodeBlock.Builder builder = CodeBlock.builder();

        variables.forEach(variable -> {
            final String name = variable.getSimpleName().toString();
            builder.beginControlFlow("if (initial.$L() != null)", name);
            builder.addStatement("$L.initWithoutValidation(initial.$L())", name, name);
            builder.endControlFlow();
        });

        return MethodSpec.methodBuilder("initWithoutValidation")
            .addModifiers(PUBLIC)
            .addAnnotation(Override.class)
            .addParameter(type, "initial")
            .addCode(builder.build())
            .build();
    }

    /**
     * Create {@link org.apache.ignite.configuration.internal.property.Modifier#change(Object)} method (accepts CHANGE object) for configuration class.
     *
     * @param type CHANGE method type.
     * @param variables List of CHANGE object's fields.
     * @return Change method.
     */
    public MethodSpec createChangeMethod(TypeName type, List<VariableElement> variables) {
        final CodeBlock.Builder builder = CodeBlock.builder();

        variables.forEach(variable -> {
            final Value valueAnnotation = variable.getAnnotation(Value.class);
            if (valueAnnotation != null && valueAnnotation.immutable())
                return;

            final String name = variable.getSimpleName().toString();
            builder.beginControlFlow("if (changes.$L() != null)", name);
            builder.addStatement("$L.changeWithoutValidation(changes.$L())", name, name);
            builder.endControlFlow();
        });

        return MethodSpec.methodBuilder("changeWithoutValidation")
            .addModifiers(PUBLIC)
            .addAnnotation(Override.class)
            .addParameter(type, "changes")
            .addCode(builder.build())
            .build();
    }

    /** {@inheritDoc} */
    @Override public Set<String> getSupportedAnnotationTypes() {
        return Collections.singleton(Config.class.getCanonicalName());
    }

    /** {@inheritDoc} */
    @Override public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.RELEASE_8;
    }
}
