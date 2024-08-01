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

package org.apache.ignite.internal.network.processor;

import static java.util.stream.Collectors.toSet;
import static javax.lang.model.element.Modifier.FINAL;
import static javax.lang.model.element.Modifier.PRIVATE;
import static javax.lang.model.element.Modifier.PUBLIC;
import static javax.lang.model.element.Modifier.STATIC;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.type.TypeMirror;
import org.apache.ignite.internal.network.annotations.Transferable;
import org.apache.ignite.internal.network.annotations.TransferableEnum;
import org.apache.ignite.internal.network.annotations.Transient;

/**
 * Generates classes for {@link Enum} that are present in network messages (with {@link Transferable}) with utility methods.
 *
 * <p>{@link Enum} are expected to implement {@link TransferableEnum}.</p>
 *
 * <p>Name of the generated class will be the class name + {@code TransferableUtils}, for example for the enum {@code SomeEnum} the class
 * name will be {@code SomeEnumTransferableUtils}.</p>
 *
 * <p>Generated methods:</p>
 * <ul>
 *     <li>{@code public static SomeEnum formTransferableId(int transferableId) throws IllegalInitArgumentException} - which will return
 *     the enumeration constant by its {@link TransferableEnum#transferableId} and throw {@link IllegalArgumentException} if it does not
 *     find it.</li>
 * </ul>
 */
public class EnumMethodsGenerator {
    private static final String VALUE_BY_TRANSFERABLE_ID_FIELD_NAME = "VALUE_BY_TRANSFERABLE_ID";

    private static final String TRANSFERABLE_ID_FIELD_NAME = "transferableId";

    /** Name of static method to get {@link Enum} by {@link TransferableEnum#transferableId}. */
    public static final String FORM_TRANSFERABLE_ID_METHOD_NAME = "formTransferableId";

    /** Postfix the name of the utility class for the {@link Enum} with {@link TransferableEnum}. */
    public static final String TRANSFERABLE_UTILS_CLASS_POSTFIX = "TransferableUtils";

    private final TypeUtils typeUtils;

    /** Constructor. */
    EnumMethodsGenerator(ProcessingEnvironment processingEnv) {
        typeUtils = new TypeUtils(processingEnv);
    }

    /**
     * Collects all {@link Enum}s that are present in network messages without the {@link Transient}.
     *
     * @param messageClasses Network message wrappers.
     */
    Set<TypeMirror> collectEnums(Collection<MessageClass> messageClasses) {
        return messageClasses.stream()
                .flatMap(messageClass -> messageClass.getters().stream())
                .filter(element -> element.getAnnotation(Transient.class) == null)
                .map(ExecutableElement::getReturnType)
                .filter(this::isEnum)
                .peek(this::checkEnumImplementTransferableEnum)
                .collect(toSet());
    }

    /**
     * Generates a utility class for the {@link Enum} with {@link TransferableEnum}.
     *
     * @param enumType {@link Enum} type mirror.
     * @return Generated class like {@code SomeEnumTransferableUtils}.
     */
    TypeSpec generateEnumTransferableUtils(TypeMirror enumType) {
        assert isEnum(enumType) && isTransferableEnum(enumType) : enumType;

        TypeSpec.Builder typeSpecBuilder = TypeSpec.classBuilder(simpleEnumName(enumType) + "TransferableUtils")
                .addModifiers(PUBLIC);

        addFormTransferableIdMethod(enumType, typeSpecBuilder);

        return typeSpecBuilder.build();
    }

    private static void addFormTransferableIdMethod(TypeMirror enumType, TypeSpec.Builder enumTypeSpecBuilder) {
        ParameterizedTypeName staticFieldType = ParameterizedTypeName.get(
                ClassName.get(Map.class),
                TypeName.get(Integer.class),
                TypeName.get(enumType)
        );

        FieldSpec staticFieldSpec = FieldSpec.builder(staticFieldType, VALUE_BY_TRANSFERABLE_ID_FIELD_NAME)
                .addModifiers(PRIVATE, STATIC, FINAL)
                .build();

        CodeBlock staticBlock = CodeBlock.builder()
                .beginControlFlow("try")
                .addStatement("$T[] values = $T.values()", enumType, enumType)
                .addStatement("var tmp = new $T<$T, $T>()", HashMap.class, Integer.class, enumType)
                .add("\n")
                .beginControlFlow("for ($T value : values)", enumType)
                .addStatement("int $L = value.transferableId()", TRANSFERABLE_ID_FIELD_NAME)
                .addStatement("assert $L >= 0 : \"Must not be negative: \" + value", TRANSFERABLE_ID_FIELD_NAME)
                .add("\n")
                .addStatement("$T prev = tmp.put($L, value)", enumType, TRANSFERABLE_ID_FIELD_NAME)
                .addStatement("assert prev == null : \"Must be unique: \" + value")
                .endControlFlow()
                .add("\n")
                .addStatement("$L = Map.copyOf(tmp)", VALUE_BY_TRANSFERABLE_ID_FIELD_NAME)
                .nextControlFlow("catch($T t)", Throwable.class)
                .addStatement("throw new $T(t)", ExceptionInInitializerError.class)
                .endControlFlow()
                .build();

        MethodSpec staticMethodSpec = MethodSpec.methodBuilder(FORM_TRANSFERABLE_ID_METHOD_NAME)
                .addModifiers(PUBLIC, STATIC)
                .returns(TypeName.get(enumType))
                .addParameter(int.class, TRANSFERABLE_ID_FIELD_NAME)
                .addException(IllegalArgumentException.class)
                .addCode(CodeBlock.builder()
                        .addStatement("$T value = $L.get($L)", enumType, VALUE_BY_TRANSFERABLE_ID_FIELD_NAME, TRANSFERABLE_ID_FIELD_NAME)
                        .add("\n")
                        .beginControlFlow("if (value != null)")
                        .addStatement("return value")
                        .endControlFlow()
                        .add("\n")
                        .addStatement(
                                "throw new $T(\"No enum constant from $L: \" + $L)",
                                IllegalArgumentException.class, TRANSFERABLE_ID_FIELD_NAME, TRANSFERABLE_ID_FIELD_NAME
                        )
                        .build()
                )
                .build();

        enumTypeSpecBuilder
                .addField(staticFieldSpec)
                .addStaticBlock(staticBlock)
                .addMethod(staticMethodSpec);
    }

    private boolean isEnum(TypeMirror typeMirror) {
        return typeUtils.isEnum(typeMirror);
    }

    private boolean isTransferableEnum(TypeMirror enumType) {
        return typeUtils.isSubType(enumType, TransferableEnum.class);
    }

    private void checkEnumImplementTransferableEnum(TypeMirror enumType) {
        if (!isTransferableEnum(enumType)) {
            throw new ProcessingException(String.format("Enum must implement %s: %s", TransferableEnum.class, enumType));
        }
    }

    private String simpleEnumName(TypeMirror enumType) {
        return typeUtils.types().asElement(enumType).getSimpleName().toString();
    }
}
