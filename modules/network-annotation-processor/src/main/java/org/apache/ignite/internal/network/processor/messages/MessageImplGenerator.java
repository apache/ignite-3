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

package org.apache.ignite.internal.network.processor.messages;

import com.squareup.javapoet.ArrayTypeName;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.MethodSpec.Builder;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.type.ArrayType;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.tools.Diagnostic;
import org.apache.ignite.internal.network.processor.MessageClass;
import org.apache.ignite.internal.network.processor.MessageGroupWrapper;
import org.apache.ignite.internal.network.processor.TypeUtils;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.annotations.Marshallable;

/**
 * Class for generating implementations of the {@link NetworkMessage} interfaces and their builders, generated by a {@link
 * MessageBuilderGenerator}.
 */
public class MessageImplGenerator {
    /** Type name of the {@code byte[]}. */
    static final ArrayTypeName BYTE_ARRAY_TYPE = ArrayTypeName.of(TypeName.BYTE);

    /** Processing environment. */
    private final ProcessingEnvironment processingEnv;

    /** Message group. */
    private final MessageGroupWrapper messageGroup;

    private final TypeUtils typeUtils;

    /**
     * Constructor.
     *
     * @param processingEnv Processing environment.
     * @param messageGroup  Message group.
     */
    public MessageImplGenerator(ProcessingEnvironment processingEnv, MessageGroupWrapper messageGroup) {
        this.processingEnv = processingEnv;
        this.messageGroup = messageGroup;
        this.typeUtils = new TypeUtils(processingEnv);
    }

    /**
     * Generates the implementation of a given Network Message interface and its Builder (as a nested class).
     *
     * @param message          network message
     * @param builderInterface generated builder interface
     * @return {@code TypeSpec} of the generated message implementation
     */
    public TypeSpec generateMessageImpl(MessageClass message, TypeSpec builderInterface) {
        ClassName messageImplClassName = message.implClassName();

        processingEnv.getMessager()
                .printMessage(Diagnostic.Kind.NOTE, "Generating " + messageImplClassName, message.element());

        List<ExecutableElement> getters = message.getters();

        var fields = new ArrayList<FieldSpec>(getters.size());
        var getterImpls = new ArrayList<MethodSpec>(getters.size());

        // create a field and a getter implementation for every getter in the message interface
        for (ExecutableElement getter : getters) {
            var getterReturnType = TypeName.get(getter.getReturnType());

            String getterName = getter.getSimpleName().toString();

            FieldSpec.Builder fieldBuilder = FieldSpec.builder(getterReturnType, getterName)
                    .addAnnotation(IgniteToStringInclude.class)
                    .addModifiers(Modifier.PRIVATE);

            boolean isMarshallable = getter.getAnnotation(Marshallable.class) != null;

            if (!isMarshallable) {
                fieldBuilder.addModifiers(Modifier.FINAL);
            }

            FieldSpec field = fieldBuilder.build();
            fields.add(field);

            if (isMarshallable) {
                String name = getByteArrayFieldName(getterName);
                FieldSpec marshallableFieldArray = FieldSpec.builder(BYTE_ARRAY_TYPE, name)
                        .addModifiers(Modifier.PRIVATE)
                        .build();

                fields.add(marshallableFieldArray);

                MethodSpec baGetterImpl = MethodSpec.methodBuilder(name)
                        .returns(BYTE_ARRAY_TYPE)
                        .addStatement("return $N", marshallableFieldArray)
                        .build();

                getterImpls.add(baGetterImpl);
            }

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

        // group type constant and getter
        FieldSpec groupTypeField = FieldSpec.builder(short.class, "GROUP_TYPE")
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
                .initializer("$L", messageGroup.groupType())
                .build();

        messageImpl.addField(groupTypeField);

        MethodSpec groupTypeMethod = MethodSpec.methodBuilder("groupType")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(short.class)
                .addStatement("return $N", groupTypeField)
                .build();

        messageImpl.addMethod(groupTypeMethod);

        // TODO: https://issues.apache.org/jira/browse/IGNITE-17591
        MethodSpec toStringMethod = MethodSpec.methodBuilder("toString")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(String.class)
                .addStatement("return $T.toString($T.class, this)", S.class, messageImplClassName)
                .build();

        messageImpl.addMethod(toStringMethod);

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

        // equals and hashCode
        generateEqualsAndHashCode(messageImpl, message);

        var builderName = ClassName.get(message.packageName(), builderInterface.name);

        // nested builder interface and static factory method
        TypeSpec builder = generateBuilderImpl(message, messageImplClassName, builderName);

        messageImpl.addType(builder);

        MethodSpec builderMethod = MethodSpec.methodBuilder("builder")
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                .returns(builderName)
                .addStatement("return new $N()", builder)
                .build();

        messageImpl.addMethod(builderMethod);

        generatePrepareMarshal(messageImpl, message);
        generateUnmarshalMethod(messageImpl, message);

        messageImpl
                .addOriginatingElement(message.element())
                .addOriginatingElement(messageGroup.element());

        return messageImpl.build();
    }

    /**
     * Resolves type of an object to a type that may hold a message. Returns {@code null} if the type
     * can't hold a message.
     *
     * @param parameterType Type.
     * @return {@link MaybeMessageType} or {@code null} if the type can't hold a message.
     */
    private Optional<MaybeMessageType> resolveType(TypeMirror parameterType) {
        if (parameterType.getKind() == TypeKind.ARRAY) {
            if (!((ArrayType) parameterType).getComponentType().getKind().isPrimitive()) {
                return Optional.of(MaybeMessageType.OBJECT_ARRAY);
            }
        } else if (parameterType.getKind() == TypeKind.DECLARED) {
            if (typeUtils.isSubType(parameterType, Collection.class)) {
                return Optional.of(MaybeMessageType.COLLECTION);
            } else if (typeUtils.isSameType(parameterType, Map.class)) {
                return Optional.of(MaybeMessageType.MAP);
            } else if (typeUtils.isSubType(parameterType, NetworkMessage.class)) {
                return Optional.of(MaybeMessageType.MESSAGE);
            }
        }
        return Optional.empty();
    }

    private void generatePrepareMarshal(TypeSpec.Builder messageImplBuild, MessageClass message) {
        boolean isNeeded = false;

        ClassName setType = ClassName.get(IntSet.class);

        Builder prepareMarshal = MethodSpec.methodBuilder("prepareMarshal")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .addException(Exception.class)
                .addParameter(setType, "usedDescriptors")
                .addParameter(Object.class, "marshallerObj");

        String marshallerPackage = "org.apache.ignite.internal.network.serialization.marshal";
        ClassName marshallerClass = ClassName.get(marshallerPackage, "UserObjectMarshaller");
        ClassName marshalledObjectClass = ClassName.get(marshallerPackage, "MarshalledObject");

        prepareMarshal.addStatement("$T marshaller = ($T) marshallerObj", marshallerClass, marshallerClass);

        for (ExecutableElement executableElement : message.getters()) {
            TypeMirror type = executableElement.getReturnType();
            String objectName = executableElement.getSimpleName().toString();

            if (executableElement.getAnnotation(Marshallable.class) != null) {
                isNeeded = true;

                String baName = getByteArrayFieldName(objectName);
                String moName = baName + "mo";
                prepareMarshal.addStatement("$T $N = marshaller.marshal($N)", marshalledObjectClass, moName, objectName);
                prepareMarshal.addStatement("usedDescriptors.addAll($N.usedDescriptorIds())", moName);
                prepareMarshal.addStatement("$N = $N.bytes()", baName, moName).addCode("\n");
            } else {
                Optional<MaybeMessageType> objectType = resolveType(type);

                if (objectType.isEmpty()) {
                    continue;
                }

                switch (objectType.get()) {
                    case OBJECT_ARRAY:
                        isNeeded = generateObjectArrayHandler((ArrayType) type, prepareMarshal, objectName,
                                "prepareMarshal(usedDescriptors, marshaller)") || isNeeded;
                        break;
                    case COLLECTION:
                        isNeeded = generateCollectionHandler((DeclaredType) type, prepareMarshal, objectName,
                                "prepareMarshal(usedDescriptors, marshaller)") || isNeeded;
                        break;
                    case MESSAGE:
                        isNeeded = generateMessageHandler(prepareMarshal, objectName,
                            "prepareMarshal(usedDescriptors, marshaller)") || isNeeded;
                        break;
                    case MAP:
                        isNeeded = generateMapHandler(prepareMarshal, (DeclaredType) type, objectName,
                            "prepareMarshal(usedDescriptors, marshaller)") || isNeeded;
                        break;
                    default:
                        break;
                }
            }
        }

        if (isNeeded) {
            messageImplBuild.addMethod(prepareMarshal.build());
        }
    }

    private void generateUnmarshalMethod(TypeSpec.Builder messageImplBuild, MessageClass message) {
        boolean isNeeded = false;

        Builder unmarshal = MethodSpec.methodBuilder("unmarshal")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .addException(Exception.class)
                .addParameter(Object.class, "marshallerObj")
                .addParameter(Object.class, "descriptorsObj");

        String uosPackage = "org.apache.ignite.internal.network.serialization.marshal";
        ClassName marshallerClass = ClassName.get(uosPackage, "UserObjectMarshaller");

        unmarshal.addStatement("$T marshaller = ($T) marshallerObj", marshallerClass, marshallerClass);

        for (ExecutableElement executableElement : message.getters()) {
            TypeMirror type = executableElement.getReturnType();
            String objectName = executableElement.getSimpleName().toString();

            if (executableElement.getAnnotation(Marshallable.class) != null) {
                isNeeded = true;

                String baName = getByteArrayFieldName(objectName);
                unmarshal.addStatement("$N = marshaller.unmarshal($N, descriptorsObj)", objectName, baName);
                unmarshal.addStatement("$N = null", baName);
            } else {
                Optional<MaybeMessageType> objectType = resolveType(type);

                if (objectType.isEmpty()) {
                    continue;
                }

                switch (objectType.get()) {
                    case OBJECT_ARRAY:
                        isNeeded = generateObjectArrayHandler((ArrayType) type, unmarshal, objectName,
                                "unmarshal(marshaller, descriptorsObj)") || isNeeded;
                        break;
                    case COLLECTION:
                        isNeeded = generateCollectionHandler((DeclaredType) type, unmarshal, objectName,
                                "unmarshal(marshaller, descriptorsObj)") || isNeeded;
                        break;
                    case MESSAGE:
                        isNeeded = generateMessageHandler(unmarshal, objectName,
                                "unmarshal(marshaller, descriptorsObj)") || isNeeded;
                        break;
                    case MAP:
                        isNeeded = generateMapHandler(unmarshal, (DeclaredType) type, objectName,
                            "unmarshal(marshaller, descriptorsObj)") || isNeeded;
                        break;
                    default:
                        break;
                }
            }
        }

        if (isNeeded) {
            messageImplBuild.addMethod(unmarshal.build());
        }
    }

    private boolean generateObjectArrayHandler(ArrayType type, Builder methodBuilder, String objectName, String code) {
        TypeMirror componentType = type.getComponentType();
        if (typeUtils.isSubType(componentType, NetworkMessage.class)) {
            methodBuilder.beginControlFlow("if ($N != null)", objectName);
            methodBuilder.beginControlFlow("for ($T obj : $N)", componentType, objectName);
            methodBuilder.addStatement("if (obj != null) obj." + code);
            methodBuilder.endControlFlow();
            methodBuilder.endControlFlow().addCode("\n");
            return true;
        }
        return false;
    }

    private boolean generateCollectionHandler(DeclaredType type, Builder methodBuilder, String objectName, String code) {
        TypeMirror elementType = type.getTypeArguments().get(0);
        if (typeUtils.isSubType(elementType, NetworkMessage.class)) {
            methodBuilder.beginControlFlow("if ($N != null)", objectName);
            methodBuilder.beginControlFlow("for ($T obj : $N)", elementType, objectName);
            methodBuilder.addStatement("if (obj != null) obj." + code, objectName);
            methodBuilder.endControlFlow();
            methodBuilder.endControlFlow().addCode("\n");
            return true;
        }
        return false;
    }

    private boolean generateMessageHandler(Builder methodBuilder, String objectName, String code) {
        methodBuilder.addStatement("if ($N != null) $N." + code, objectName, objectName);
        return true;
    }

    private boolean generateMapHandler(Builder methodBuilder, DeclaredType type, String objectName, String code) {
        TypeMirror keyType = type.getTypeArguments().get(0);
        boolean keyIsMessage = typeUtils.isSubType(keyType, NetworkMessage.class);
        TypeMirror valueType = type.getTypeArguments().get(1);
        boolean valueIsMessage = typeUtils.isSubType(valueType, NetworkMessage.class);

        if (keyIsMessage || valueIsMessage) {
            ParameterizedTypeName entryType = ParameterizedTypeName.get(
                    ClassName.get(Entry.class),
                    TypeName.get(keyType),
                    TypeName.get(valueType)
            );
            ParameterizedTypeName entrySetType = ParameterizedTypeName.get(ClassName.get(Set.class), entryType);
            String entrySetName = objectName + "EntrySet";

            methodBuilder.beginControlFlow("if ($N != null)", objectName);
            methodBuilder.addStatement("$T $N = $N.entrySet()", entrySetType, entrySetName, objectName);
            methodBuilder.beginControlFlow("for ($T entry : $N)", entryType, entrySetName);
            methodBuilder.addStatement("$T key = entry.getKey()", keyType);
            methodBuilder.addStatement("$T value = entry.getValue()", valueType);
            if (keyIsMessage) {
                methodBuilder.addStatement("if (key != null) key." + code);
            }
            if (valueIsMessage) {
                methodBuilder.addStatement("if (value != null) value." + code);
            }
            methodBuilder.endControlFlow();
            methodBuilder.endControlFlow().addCode("\n");
            return true;
        }
        return false;
    }

    /**
     * Generates implementations of {@link #hashCode} and {@link #equals} for the provided {@code message} and adds them to the provided
     * builder.
     */
    private static void generateEqualsAndHashCode(TypeSpec.Builder messageImplBuilder, MessageClass message) {
        MethodSpec.Builder equals = MethodSpec.methodBuilder("equals")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(boolean.class)
                .addParameter(Object.class, "other")
                .addCode(CodeBlock.builder()
                        .beginControlFlow("if (this == other)")
                        .addStatement("return true")
                        .endControlFlow()
                        .build())
                .addCode(CodeBlock.builder()
                        .beginControlFlow("if (other == null || getClass() != other.getClass())")
                        .addStatement("return false")
                        .endControlFlow()
                        .build());

        MethodSpec.Builder hashCode = MethodSpec.methodBuilder("hashCode")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(int.class);

        if (message.getters().isEmpty()) {
            equals.addStatement("return true");

            hashCode.addStatement("return $T.class.hashCode()", message.implClassName());

            messageImplBuilder
                    .addMethod(equals.build())
                    .addMethod(hashCode.build());

            return;
        }

        var arrays = new ArrayList<ExecutableElement>();
        var primitives = new ArrayList<ExecutableElement>();
        var others = new ArrayList<ExecutableElement>();

        for (ExecutableElement element : message.getters()) {
            TypeKind typeKind = element.getReturnType().getKind();

            if (typeKind.isPrimitive()) {
                primitives.add(element);
            } else if (typeKind == TypeKind.ARRAY) {
                arrays.add(element);
            } else {
                others.add(element);
            }
        }

        CodeBlock.Builder comparisonStatement = CodeBlock.builder().add("return ");

        boolean first = true;

        // objects are compared using "Objects.equals"
        for (ExecutableElement other : others) {
            if (first) {
                first = false;
            } else {
                comparisonStatement.add(" && ");
            }

            String fieldName = other.getSimpleName().toString();

            comparisonStatement.add("$T.equals(this.$L, otherMessage.$L)", Objects.class, fieldName, fieldName);
        }

        // arrays are compared using "Arrays.equals"
        for (ExecutableElement array : arrays) {
            if (first) {
                first = false;
            } else {
                comparisonStatement.add(" && ");
            }

            String fieldName = array.getSimpleName().toString();

            comparisonStatement.add("$T.equals(this.$L, otherMessage.$L)", Arrays.class, fieldName, fieldName);
        }

        // primitives are compared using "==", except for floating point values (because of NaNs and stuff)
        for (ExecutableElement primitive : primitives) {
            if (first) {
                first = false;
            } else {
                comparisonStatement.add(" && ");
            }

            String fieldName = primitive.getSimpleName().toString();

            switch (primitive.getReturnType().getKind()) {
                case FLOAT:
                    comparisonStatement.add("$T.compare(this.$L, otherMessage.$L) == 0", Float.class, fieldName, fieldName);
                    break;
                case DOUBLE:
                    comparisonStatement.add("$T.compare(this.$L, otherMessage.$L) == 0", Double.class, fieldName, fieldName);
                    break;
                default:
                    comparisonStatement.add("this.$L == otherMessage.$L", fieldName, fieldName);
                    break;
            }
        }

        equals
                .addStatement("var otherMessage = ($T)other", message.implClassName())
                .addStatement(comparisonStatement.build());

        hashCode
                .addStatement("int result = 0");

        // primitives can be boxed and used in "Objects.hash"
        String objectHashCode = Stream.concat(primitives.stream(), others.stream())
                .map(element -> "this." + element.getSimpleName())
                .collect(Collectors.joining(", ", "result = $T.hash(", ")"));

        if (!objectHashCode.isEmpty()) {
            hashCode.addStatement(objectHashCode, Objects.class);
        }

        for (ExecutableElement array : arrays) {
            hashCode.addStatement("result = 31 * result + $T.hashCode(this.$L)", Arrays.class, array.getSimpleName());
        }

        hashCode.addStatement("return result");

        messageImplBuilder
                .addMethod(equals.build())
                .addMethod(hashCode.build());
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
    private static TypeSpec generateBuilderImpl(
            MessageClass message, ClassName messageImplClass, ClassName builderName
    ) {
        List<ExecutableElement> messageGetters = message.getters();

        var fields = new ArrayList<FieldSpec>(messageGetters.size());
        var setters = new ArrayList<MethodSpec>(messageGetters.size());
        var getters = new ArrayList<MethodSpec>(messageGetters.size());

        for (ExecutableElement messageGetter : messageGetters) {
            var getterReturnType = TypeName.get(messageGetter.getReturnType());

            String getterName = messageGetter.getSimpleName().toString();

            FieldSpec field = FieldSpec.builder(getterReturnType, getterName)
                    .addModifiers(Modifier.PRIVATE)
                    .build();

            fields.add(field);

            MethodSpec setter = MethodSpec.methodBuilder(getterName)
                    .addAnnotation(Override.class)
                    .addModifiers(Modifier.PUBLIC)
                    .returns(builderName)
                    .addParameter(getterReturnType, getterName)
                    .addStatement("this.$N = $L", field, getterName)
                    .addStatement("return this")
                    .build();

            setters.add(setter);

            MethodSpec getter = MethodSpec.methodBuilder(getterName)
                    .addAnnotation(Override.class)
                    .addModifiers(Modifier.PUBLIC)
                    .returns(getterReturnType)
                    .addStatement("return $N", field)
                    .build();

            getters.add(getter);

            if (messageGetter.getAnnotation(Marshallable.class) != null) {
                String name = getByteArrayFieldName(getterName);
                FieldSpec baField = FieldSpec.builder(BYTE_ARRAY_TYPE, name)
                        .addModifiers(Modifier.PRIVATE)
                        .build();

                fields.add(baField);

                MethodSpec baSetter = MethodSpec.methodBuilder(name)
                        .addAnnotation(Override.class)
                        .addModifiers(Modifier.PUBLIC)
                        .returns(builderName)
                        .addParameter(BYTE_ARRAY_TYPE, name)
                        .addStatement("this.$N = $L", baField, name)
                        .addStatement("return this")
                        .build();

                setters.add(baSetter);

                MethodSpec baGetter = MethodSpec.methodBuilder(name)
                        .addAnnotation(Override.class)
                        .addModifiers(Modifier.PUBLIC)
                        .returns(BYTE_ARRAY_TYPE)
                        .addStatement("return $N", baField)
                        .build();

                getters.add(baGetter);
            }
        }

        return TypeSpec.classBuilder("Builder")
                .addModifiers(Modifier.PRIVATE, Modifier.STATIC)
                .addSuperinterface(builderName)
                .addFields(fields)
                .addMethods(setters)
                .addMethods(getters)
                .addMethod(buildMethod(message, messageImplClass, fields))
                .build();
    }

    /**
     * Generates the {@code build()} method for the Builder interface implementation.
     */
    private static MethodSpec buildMethod(MessageClass message, ClassName messageImplClass, List<FieldSpec> fields) {
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

    public static String getByteArrayFieldName(String objectName) {
        return objectName + "ByteArray";
    }

    /** Types that may hold network message. */
    private enum MaybeMessageType {
        OBJECT_ARRAY,
        COLLECTION,
        MESSAGE,
        MAP;
    }
}
