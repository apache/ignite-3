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

import static org.apache.ignite.internal.network.processor.MessageGeneratorUtils.addByteArrayPostfix;
import static org.apache.ignite.internal.network.processor.MessageGeneratorUtils.propertyName;

import com.squareup.javapoet.CodeBlock;
import java.util.List;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.type.ArrayType;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeMirror;
import org.apache.ignite.internal.network.annotations.Marshallable;
import org.apache.ignite.internal.network.serialization.MessageCollectionItemType;
import org.apache.ignite.internal.network.serialization.MessageReader;

/**
 * Class for resolving {@link MessageReader} "read*" methods for the corresponding message field type.
 */
class MessageReaderMethodResolver {
    /** Method name resolver. */
    private final BaseMethodNameResolver methodNameResolver;

    /** Type converter. */
    private final MessageCollectionItemTypeConverter typeConverter;

    /**
     * Constructor.
     *
     * @param processingEnvironment Processing environment.
     */
    MessageReaderMethodResolver(ProcessingEnvironment processingEnvironment) {
        methodNameResolver = new BaseMethodNameResolver(processingEnvironment);
        typeConverter = new MessageCollectionItemTypeConverter(processingEnvironment);
    }

    /**
     * Resolves the "read" method by the type of the given message's builder method.
     *
     * @param getter getter method
     * @return code for the method for reading the corresponding type of the getter
     */
    CodeBlock resolveReadMethod(ExecutableElement getter) {
        TypeMirror parameterType = getter.getReturnType();

        String propertyName = propertyName(getter);

        if (getter.getAnnotation(Marshallable.class) != null) {
            propertyName = addByteArrayPostfix(propertyName);
            return CodeBlock.builder()
                    .add("readByteArray($S)", propertyName)
                    .build();
        }

        String methodName = methodNameResolver.resolveBaseMethodName(parameterType);

        switch (methodName) {
            case "ObjectArray":
                return resolveReadObjectArray((ArrayType) parameterType, propertyName);
            case "Collection":
                return resolveReadCollection((DeclaredType) parameterType, propertyName);
            case "List":
                return resolveReadList((DeclaredType) parameterType, propertyName);
            case "Set":
                return resolveReadSet((DeclaredType) parameterType, propertyName);
            case "Map":
                return resolveReadMap((DeclaredType) parameterType, propertyName);
            default:
                return CodeBlock.builder().add("read$L($S)", methodName, propertyName).build();
        }
    }

    /**
     * Creates a {@link MessageReader#readObjectArray(String, MessageCollectionItemType, Class)} method call.
     */
    private CodeBlock resolveReadObjectArray(ArrayType parameterType, String parameterName) {
        TypeMirror componentType = parameterType.getComponentType();

        return CodeBlock.builder()
                .add(
                        "readObjectArray($S, $T.$L, $T.class)",
                        parameterName,
                        MessageCollectionItemType.class,
                        typeConverter.fromTypeMirror(componentType),
                        componentType
                )
                .build();
    }

    /**
     * Creates a {@link MessageReader#readCollection(String, MessageCollectionItemType)} method call.
     */
    private CodeBlock resolveReadCollection(DeclaredType parameterType, String parameterName) {
        TypeMirror collectionGenericType = parameterType.getTypeArguments().get(0);

        return CodeBlock.builder()
                .add(
                        "readCollection($S, $T.$L)",
                        parameterName,
                        MessageCollectionItemType.class,
                        typeConverter.fromTypeMirror(collectionGenericType)
                )
                .build();
    }

    /**
     * Creates a {@link MessageReader#readList(String, MessageCollectionItemType)} method call.
     */
    private CodeBlock resolveReadList(DeclaredType parameterType, String parameterName) {
        TypeMirror listGenericType = parameterType.getTypeArguments().get(0);

        return CodeBlock.builder()
                .add(
                        "readList($S, $T.$L)",
                        parameterName,
                        MessageCollectionItemType.class,
                        typeConverter.fromTypeMirror(listGenericType)
                )
                .build();
    }

    /**
     * Creates a {@link MessageReader#readSet(String, MessageCollectionItemType)} method call.
     */
    private CodeBlock resolveReadSet(DeclaredType parameterType, String parameterName) {
        TypeMirror setGenericType = parameterType.getTypeArguments().get(0);

        return CodeBlock.builder()
                .add(
                        "readSet($S, $T.$L)",
                        parameterName,
                        MessageCollectionItemType.class,
                        typeConverter.fromTypeMirror(setGenericType)
                )
                .build();
    }

    /**
     * Creates a {@link MessageReader#readMap(String, MessageCollectionItemType, MessageCollectionItemType, boolean)} method call.
     */
    private CodeBlock resolveReadMap(DeclaredType parameterType, String parameterName) {
        List<? extends TypeMirror> typeArguments = parameterType.getTypeArguments();

        MessageCollectionItemType mapKeyType = typeConverter.fromTypeMirror(typeArguments.get(0));
        MessageCollectionItemType mapValueType = typeConverter.fromTypeMirror(typeArguments.get(1));

        return CodeBlock.builder()
                .add(
                        "readMap($S, $T.$L, $T.$L, false)",
                        parameterName,
                        MessageCollectionItemType.class,
                        mapKeyType,
                        MessageCollectionItemType.class,
                        mapValueType
                )
                .build();
    }
}
