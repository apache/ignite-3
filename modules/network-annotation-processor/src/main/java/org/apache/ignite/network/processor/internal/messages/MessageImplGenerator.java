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

package org.apache.ignite.network.processor.internal.messages;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.tools.Diagnostic;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.processor.internal.MessageClass;
import org.apache.ignite.network.processor.internal.MessageTypes;

/**
 * Class for generating implementations of the {@link NetworkMessage} interfaces and their builders, generated by a
 * {@link MessageBuilderGenerator}.
 */
public class MessageImplGenerator {
    /** */
    private final ProcessingEnvironment processingEnv;

    /** Network message interface. */
    private final MessageClass message;

    /** Name of the builder interface, created by {@link MessageBuilderGenerator}. */
    private final ClassName builderInterfaceName;

    /** Module message types. */
    private final MessageTypes messageTypes;

    /** */
    public MessageImplGenerator(
        ProcessingEnvironment processingEnv,
        MessageClass message,
        TypeSpec builderInterface,
        MessageTypes messageTypes
    ) {
        this.processingEnv = processingEnv;
        this.message = message;
        this.builderInterfaceName = ClassName.get(message.packageName(), builderInterface.name);
        this.messageTypes = messageTypes;
    }

    /**
     * Generates the implementation of a given Network Message interface and its Builder (as a nested class).
     */
    public TypeSpec generateMessageImpl() {
        ClassName messageImplClassName = message.implClassName();

        processingEnv.getMessager()
            .printMessage(Diagnostic.Kind.NOTE, "Generating " + messageImplClassName, message.element());

        List<ExecutableElement> getters = message.getters();

        var fields = new ArrayList<FieldSpec>(getters.size());
        var getterImpls = new ArrayList<MethodSpec>(getters.size());

        // create a field, a getter implementation and a constructor parameter for every getter in the message interface
        for (ExecutableElement getter : getters) {
            var getterReturnType = TypeName.get(getter.getReturnType());

            String getterName = getter.getSimpleName().toString();

            FieldSpec field = FieldSpec.builder(getterReturnType, getterName)
                .addModifiers(Modifier.PRIVATE, Modifier.FINAL)
                .build();

            fields.add(field);

            MethodSpec getterImpl = MethodSpec.overriding(getter)
                .addStatement("return $N", field)
                .build();

            getterImpls.add(getterImpl);
        }

        TypeSpec.Builder messageImpl = TypeSpec.classBuilder(messageImplClassName)
            .addModifiers(Modifier.PUBLIC)
            .addSuperinterface(message.className())
            .addFields(fields)
            .addMethods(getterImpls)
            .addMethod(constructor(fields));

        // module type constant and getter
        FieldSpec moduleTypeField = FieldSpec.builder(short.class, "MODULE_TYPE")
            .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
            .initializer("$L", messageTypes.moduleType())
            .build();

        messageImpl.addField(moduleTypeField);

        MethodSpec moduleTypeMethod = MethodSpec.methodBuilder("moduleType")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(short.class)
            .addStatement("return $N", moduleTypeField)
            .build();

        messageImpl.addMethod(moduleTypeMethod);

        // message type constant and getter
        FieldSpec messageTypeField = FieldSpec.builder(short.class, "TYPE")
            .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
            .initializer("$L", message.messageType())
            .build();

        messageImpl.addField(messageTypeField);

        MethodSpec messageTypeMethod = MethodSpec.methodBuilder("messageType")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(short.class)
            .addStatement("return $N", messageTypeField)
            .build();

        messageImpl.addMethod(messageTypeMethod);

        // nested builder interface and static factory method
        TypeSpec builder = generateBuilderImpl(messageImplClassName);

        messageImpl.addType(builder);

        MethodSpec builderMethod = MethodSpec.methodBuilder("builder")
            .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
            .returns(builderInterfaceName)
            .addStatement("return new $N()", builder)
            .build();

        messageImpl.addMethod(builderMethod);

        return messageImpl.build();
    }

    /**
     * Creates a constructor for the current Network Message implementation.
     */
    private static MethodSpec constructor(List<FieldSpec> fields) {
        MethodSpec.Builder constructor = MethodSpec.constructorBuilder()
            .addModifiers(Modifier.PRIVATE);

        fields.forEach(field ->
            constructor
                .addParameter(field.type, field.name)
                .addStatement("this.$N = $N", field, field)
        );

        return constructor.build();
    }

    /**
     * Generates a nested static class that implements the Builder interface, generated during previous steps.
     */
    private TypeSpec generateBuilderImpl(ClassName messageImplClass) {
        List<ExecutableElement> getters = message.getters();

        var fields = new ArrayList<FieldSpec>(getters.size());
        var setters = new ArrayList<MethodSpec>(getters.size());

        for (ExecutableElement getter : getters) {
            var getterReturnType = TypeName.get(getter.getReturnType());

            String getterName = getter.getSimpleName().toString();

            FieldSpec field = FieldSpec.builder(getterReturnType, getterName)
                .addModifiers(Modifier.PRIVATE)
                .build();

            fields.add(field);

            MethodSpec setter = MethodSpec.methodBuilder(getterName)
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(builderInterfaceName)
                .addParameter(getterReturnType, getterName)
                .addStatement("this.$N = $L", field, getterName)
                .addStatement("return this")
                .build();

            setters.add(setter);
        }

        return TypeSpec.classBuilder("Builder")
            .addModifiers(Modifier.PRIVATE, Modifier.STATIC)
            .addSuperinterface(builderInterfaceName)
            .addFields(fields)
            .addMethods(setters)
            .addMethod(buildMethod(messageImplClass, fields))
            .build();
    }

    /**
     * Generates the {@code build()} method for the Builder interface implementation.
     */
    private MethodSpec buildMethod(ClassName messageImplClass, List<FieldSpec> fields) {
        String constructorParameters = fields.stream()
            .map(field -> field.name)
            .collect(Collectors.joining(", ", "(", ")"));

        return MethodSpec.methodBuilder("build")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(message.className())
            .addStatement("return new $T$L", messageImplClass, constructorParameters)
            .build();
    }
}
