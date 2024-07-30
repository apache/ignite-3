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
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.type.TypeMirror;
import javax.tools.Diagnostic;
import org.apache.ignite.internal.network.annotations.Marshallable;
import org.apache.ignite.internal.network.annotations.Transient;
import org.apache.ignite.internal.network.processor.MessageClass;
import org.apache.ignite.internal.network.processor.MessageGroupWrapper;
import org.apache.ignite.internal.network.processor.ProcessingException;
import org.apache.ignite.internal.network.processor.TypeUtils;
import org.apache.ignite.internal.network.serialization.MessageDeserializer;
import org.apache.ignite.internal.network.serialization.MessageMappingException;
import org.apache.ignite.internal.network.serialization.MessageReader;

/**
 * Class for generating {@link MessageDeserializer} classes.
 */
public class MessageDeserializerGenerator {
    private static final String FROM_ORDINAL_METHOD_NAME = "fromOrdinal";

    /** Processing environment. */
    private final ProcessingEnvironment processingEnv;

    /** Message group. */
    private final MessageGroupWrapper messageGroup;

    private final TypeUtils typeUtils;

    private final MessageReaderMethodResolver methodResolver;

    /** Constructor. */
    public MessageDeserializerGenerator(ProcessingEnvironment processingEnv, MessageGroupWrapper messageGroup) {
        this.processingEnv = processingEnv;
        this.messageGroup = messageGroup;

        typeUtils = new TypeUtils(processingEnv);
        methodResolver = new MessageReaderMethodResolver(processingEnv);
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

                String getterName = getter.getSimpleName().toString();

                if (getter.getAnnotation(Marshallable.class) != null) {
                    getterName = addByteArrayPostfix(getterName);
                }

                method.beginControlFlow("case $L:", i);

                if (typeUtils.isEnum(getter.getReturnType())) {
                    checkFromOrdinalMethodExists(getter.getReturnType());

                    // At the beginning we read the shifted ordinal, shifted by +1 to efficiently transfer null (since we use "var int").
                    // If we read garbage then we should not convert to an enumeration, the check below does this.
                    method.addStatement("int ordinalShifted = reader.readInt($S)", getterName);
                } else {
                    method.addStatement(readMessageCodeBlock(getter));
                }

                method
                        .addCode("\n")
                        .addCode(CodeBlock.builder()
                                .beginControlFlow("if (!reader.isLastRead())")
                                .addStatement("return false")
                                .endControlFlow()
                                .build()
                        )
                        .addCode("\n");

                if (typeUtils.isEnum(getter.getReturnType())) {
                    TypeName varType = TypeName.get(getter.getReturnType());

                    method
                            .addStatement(
                                    "$T tmp = ordinalShifted == 0 ? null : $T.$L(ordinalShifted - 1)",
                                    varType, varType, FROM_ORDINAL_METHOD_NAME
                            )
                            .addCode("\n");
                }

                method
                        .addStatement("$N.$N(tmp)", msgField, getterName)
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

    private void checkFromOrdinalMethodExists(TypeMirror enumType) {
        assert typeUtils.isEnum(enumType) : enumType;

        typeUtils.types().asElement(enumType).getEnclosedElements().stream()
                .filter(element -> element.getKind() == ElementKind.METHOD)
                .filter(element -> element.getSimpleName().toString().equals(FROM_ORDINAL_METHOD_NAME))
                .filter(element -> element.getModifiers().contains(Modifier.PUBLIC))
                .filter(element -> element.getModifiers().contains(Modifier.STATIC))
                .findAny()
                .orElseThrow(() -> new ProcessingException(
                        String.format("Missing public static method \"%s\" for enum %s", FROM_ORDINAL_METHOD_NAME, enumType)
                ));
    }
}
