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

package org.apache.ignite.internal.network.processor.serialization;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.network.processor.MessageGeneratorUtils.addByteArrayPostfix;

import com.squareup.javapoet.ArrayTypeName;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import java.util.List;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.tools.Diagnostic;
import org.apache.ignite.internal.network.annotations.Marshallable;
import org.apache.ignite.internal.network.annotations.Transient;
import org.apache.ignite.internal.network.processor.MessageClass;
import org.apache.ignite.internal.network.processor.MessageGroupWrapper;
import org.apache.ignite.internal.network.serialization.MessageDeserializer;
import org.apache.ignite.internal.network.serialization.MessageMappingException;
import org.apache.ignite.internal.network.serialization.MessageReader;

/**
 * Class for generating {@link MessageDeserializer} classes.
 */
public class MessageDeserializerGenerator {
    /** Processing environment. */
    private final ProcessingEnvironment processingEnv;

    /**
     * Message Types declarations for the current module.
     */
    private final MessageGroupWrapper messageGroup;

    /**
     * Constructor.
     *
     * @param processingEnv Processing environment.
     * @param messageGroup  Message group.
     */
    public MessageDeserializerGenerator(ProcessingEnvironment processingEnv, MessageGroupWrapper messageGroup) {
        this.processingEnv = processingEnv;
        this.messageGroup = messageGroup;
    }

    /**
     * Generates a {@link MessageDeserializer} class for the given network message type.
     *
     * @param message network message
     * @return {@code TypeSpec} of the generated deserializer
     */
    public TypeSpec generateDeserializer(MessageClass message) {
        processingEnv.getMessager()
                .printMessage(Diagnostic.Kind.NOTE, "Generating a MessageDeserializer for " + message.className());

        FieldSpec msgField = FieldSpec.builder(message.builderClassName(), "msg")
                .addModifiers(Modifier.PRIVATE, Modifier.FINAL)
                .build();

        return TypeSpec.classBuilder(message.simpleName() + "Deserializer")
                .addSuperinterface(ParameterizedTypeName.get(ClassName.get(MessageDeserializer.class), message.className()))
                .addField(msgField)
                .addMethod(
                        MethodSpec.constructorBuilder()
                                .addParameter(messageGroup.messageFactoryClassName(), "messageFactory")
                                .addStatement("this.$N = messageFactory.$L()", msgField, message.asMethodName())
                                .build()
                )
                .addMethod(
                        MethodSpec.methodBuilder("klass")
                                .addAnnotation(Override.class)
                                .addModifiers(Modifier.PUBLIC)
                                .returns(ParameterizedTypeName.get(ClassName.get(Class.class), message.className()))
                                .addStatement("return $T.class", message.className())
                                .build()
                )
                .addMethod(
                        MethodSpec.methodBuilder("getMessage")
                                .addAnnotation(Override.class)
                                .addModifiers(Modifier.PUBLIC)
                                .returns(message.className())
                                .addStatement("return $N.build()", msgField)
                                .build()
                )
                .addMethod(readMessageMethod(message, msgField))
                .addOriginatingElement(message.element())
                .addOriginatingElement(messageGroup.element())
                .build();
    }

    /**
     * Generates the {@link MessageDeserializer#readMessage(MessageReader)} implementation.
     */
    private MethodSpec readMessageMethod(MessageClass message, FieldSpec msgField) {
        MethodSpec.Builder method = MethodSpec.methodBuilder("readMessage")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(boolean.class)
                .addParameter(MessageReader.class, "reader")
                .addException(MessageMappingException.class);

        List<ExecutableElement> getters = message.getters().stream()
                .filter(e -> e.getAnnotation(Transient.class) == null)
                .collect(toList());

        method
                .beginControlFlow("if (!reader.beforeMessageRead())")
                .addStatement("return false")
                .endControlFlow()
                .addCode("\n");

        if (!getters.isEmpty()) {
            method.beginControlFlow("switch (reader.state())");

            for (int i = 0; i < getters.size(); ++i) {
                ExecutableElement getter = getters.get(i);

                String name = getter.getSimpleName().toString();

                if (getter.getAnnotation(Marshallable.class) != null) {
                    name = addByteArrayPostfix(name);
                }

                method
                        .beginControlFlow("case $L:", i)
                        .addStatement(readMessageCodeBlock(getter))
                        .addCode("\n")
                        .addCode(CodeBlock.builder()
                                .beginControlFlow("if (!reader.isLastRead())")
                                .addStatement("return false")
                                .endControlFlow()
                                .build()
                        )
                        .addCode("\n")
                        .addStatement("$N.$N(tmp)", msgField, name)
                        .addStatement("reader.incrementState()")
                        .endControlFlow()
                        .addComment("Falls through");
            }

            method.endControlFlow().addCode("\n");
        }

        method.addStatement("return reader.afterMessageRead($T.class)", message.className());

        return method.build();
    }

    /**
     * Helper method for resolving a {@link MessageReader} "read*" call based on the message field type.
     */
    private CodeBlock readMessageCodeBlock(ExecutableElement getter) {
        var methodResolver = new MessageReaderMethodResolver(processingEnv);

        TypeName varType;

        if (getter.getAnnotation(Marshallable.class) != null) {
            varType = ArrayTypeName.of(TypeName.BYTE);
        } else {
            varType = TypeName.get(getter.getReturnType());
        }

        return CodeBlock.builder()
                .add("$T tmp = reader.", varType)
                .add(methodResolver.resolveReadMethod(getter))
                .build();
    }
}
