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

package org.apache.ignite.internal.schema.marshaller.codegen;

import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ArrayTypeName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.TimeUnit;
import javax.annotation.processing.Generated;
import javax.lang.model.element.Modifier;
import jdk.jfr.Experimental;
import org.apache.ignite.internal.schema.Columns;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.Tuple;
import org.apache.ignite.internal.schema.TupleAssembler;
import org.apache.ignite.internal.schema.marshaller.AbstractSerializer;
import org.apache.ignite.internal.schema.marshaller.BinaryMode;
import org.apache.ignite.internal.schema.marshaller.CompilerUtils;
import org.apache.ignite.internal.schema.marshaller.MarshallerUtil;
import org.apache.ignite.internal.schema.marshaller.SerializationException;
import org.apache.ignite.internal.schema.marshaller.Serializer;
import org.apache.ignite.internal.schema.marshaller.SerializerFactory;
import org.apache.ignite.internal.util.ObjectFactory;
import org.jetbrains.annotations.Nullable;

/**
 * {@link Serializer} code generator.
 */
@Experimental
public class SerializerGenerator implements SerializerFactory {
    /** Serializer package name. */
    public static final String SERIALIZER_PACKAGE_NAME = "org.apache.ignite.internal.schema.marshaller";

    /** Serializer package name prefix. */
    public static final String SERIALIZER_CLASS_NAME_PREFIX = "SerializerForSchema_";

    /** {@inheritDoc} */
    @Override public Serializer create(
        SchemaDescriptor schema,
        Class<?> keyClass,
        Class<?> valClass
    ) {
        final boolean isDebugEnabled = false; // log.isDebugEnabled();
        final String className = SERIALIZER_CLASS_NAME_PREFIX + schema.version();

        try {
            // Generate Serializer code.
            long generated = System.nanoTime();
            JavaFile javaFile = generateSerializerClassCode(className, schema, keyClass, valClass);
            generated = System.nanoTime() - generated;

            if (isDebugEnabled)
                System.out.println(javaFile.toString());

            // Compile.
            long compilation = System.nanoTime();
            ClassLoader loader = CompilerUtils.compileCode(javaFile);
            compilation = System.nanoTime() - compilation;

            //TODO: pass code to logger on trace level.
            if (isDebugEnabled)
                System.out.println("Serializer created: generated=" + TimeUnit.NANOSECONDS.toMicros(generated) + "us" +
                    ", compiled=" + TimeUnit.NANOSECONDS.toMicros(compilation) + "us." /*+ javaFile.toString()*/);

            // Instantiate serializer.
            return (Serializer)loader.loadClass(javaFile.packageName + '.' + className)
                .getDeclaredConstructor(SchemaDescriptor.class)
                .newInstance(schema);

        }
        catch (InstantiationException | ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new IllegalStateException("Failed to create serializer for key-value pair: schemaVer=" + schema.version() +
                ", keyClass=" + keyClass.getSimpleName() + ", valueClass=" + valClass.getSimpleName(), e);
        }
    }

    /**
     * Generates serializer code.
     *
     * @param className Serializer class name.
     * @param schema Schema descriptor.
     * @param keyClass Key class.
     * @param valClass Value class.
     * @return Generated java file representation.
     */
    private JavaFile generateSerializerClassCode(String className, SchemaDescriptor schema, Class<?> keyClass,
        Class<?> valClass) {
        try {
            // Build code generators.
            final MarshallerCodeGenerator keyMarsh = createObjectMarshaller(keyClass, "keyFactory", schema.keyColumns(), 0);
            final MarshallerCodeGenerator valMarsh = createObjectMarshaller(valClass, "valFactory", schema.valueColumns(), schema.keyColumns().length());

            final TypeSpec.Builder classBuilder = TypeSpec.classBuilder(className)
                .superclass(AbstractSerializer.class)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addAnnotation(AnnotationSpec.builder(Generated.class).addMember("value", "$S", getClass().getName()).build());

            initStaticFields(keyMarsh, valMarsh, keyClass, valClass, classBuilder);

            classBuilder
                .addField(ParameterizedTypeName.get(ObjectFactory.class), "keyFactory", Modifier.PRIVATE, Modifier.FINAL)
                .addField(ParameterizedTypeName.get(ObjectFactory.class), "valFactory", Modifier.PRIVATE, Modifier.FINAL)
                .addMethod(
                    // Constructor.
                    MethodSpec.constructorBuilder()
                        .addModifiers(Modifier.PUBLIC)
                        .addParameter(SchemaDescriptor.class, "schema")
                        .addStatement("super(schema)")
                        .addStatement("this.keyFactory = $T.factoryForClass(KEY_CLASS)", MarshallerUtil.class)
                        .addStatement("this.valFactory = $T.factoryForClass(VALUE_CLASS)", MarshallerUtil.class)
                        .build()
                )
                .addMethod(generateTupleAsseblerFactoryMethod(schema, keyMarsh, valMarsh))
                .addMethod(generateSerializeMethod(keyMarsh, valMarsh))
                .addMethod(generateDeserializeKeyMethod(keyMarsh))
                .addMethod(generateDeserializeValueMethod(valMarsh));

            return JavaFile
                .builder(SERIALIZER_PACKAGE_NAME, classBuilder.build())
                .addStaticImport(MethodHandles.class, "Lookup")
                .skipJavaLangImports(true)
                .indent("    ")
                .build();
        }
        catch (Exception ex) {
            throw new IllegalStateException(ex);
        }
    }

    /**
     * @param keyMarsh Key marshaller code generator.
     * @param valMarsh Value marshaller code generator.
     * @param classBuilder Serializer class builder.
     */
    private void initStaticFields(
        MarshallerCodeGenerator keyMarsh,
        MarshallerCodeGenerator valMarsh,
        Class<?> keyClass,
        Class<?> valueClass,
        TypeSpec.Builder classBuilder
    ) {
        classBuilder.addField(FieldSpec.builder(
            Class.class,
            "KEY_CLASS",
            Modifier.PRIVATE,
            Modifier.FINAL,
            Modifier.STATIC)
            .build());

        classBuilder.addField(FieldSpec.builder(
            Class.class,
            "VALUE_CLASS",
            Modifier.PRIVATE,
            Modifier.FINAL,
            Modifier.STATIC)
            .build());

        final CodeBlock.Builder staticInitBuilder = CodeBlock.builder()
            .addStatement("$T.Lookup lookup", MethodHandles.class)
            .beginControlFlow("try");

        // Avoid direct class name usage in code to avoid potential linkage errors.
        staticInitBuilder.addStatement("KEY_CLASS = $T.forName($S)", Class.class, keyClass.getName());
        staticInitBuilder.addStatement("VALUE_CLASS = $T.forName($S)", Class.class, valueClass.getName());

        if (!keyMarsh.isSimpleType()) {
            staticInitBuilder.addStatement(
                "lookup = $T.privateLookupIn(KEY_CLASS, $T.lookup())",
                MethodHandles.class,
                MethodHandles.class
            );

            keyMarsh.initStaticHandlers(classBuilder, "KEY_CLASS", staticInitBuilder);
        }

        if (!valMarsh.isSimpleType()) {
            staticInitBuilder.addStatement(
                "lookup = $T.privateLookupIn(VALUE_CLASS, $T.lookup())",
                MethodHandles.class,
                MethodHandles.class
            );

            valMarsh.initStaticHandlers(classBuilder, "VALUE_CLASS", staticInitBuilder);
        }

        staticInitBuilder
            .nextControlFlow(
                "catch ($T | $T ex)",
                ReflectiveOperationException.class,
                SecurityException.class
            )
            .addStatement("throw new $T(ex)", IllegalStateException.class)
            .endControlFlow();

        classBuilder.addStaticBlock(staticInitBuilder.build());
    }

    /**
     * @param schema Schema descriptor.
     * @param keyMarsh Key marshaller code generator.
     * @param valMarsh Value marshaller code generator.
     * @return Tuple accembler factory method spec.
     */
    private MethodSpec generateTupleAsseblerFactoryMethod(SchemaDescriptor schema, MarshallerCodeGenerator keyMarsh,
        MarshallerCodeGenerator valMarsh) {
        final MethodSpec.Builder builder = MethodSpec
            .methodBuilder("createAssembler")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PROTECTED, Modifier.FINAL)
            .addParameter(Object.class, "key", Modifier.FINAL)
            .addParameter(Object.class, "val", Modifier.FINAL)
            .returns(TupleAssembler.class)

            .addStatement("int varlenKeyCols = 0; int varlenValueCols = 0")
            .addStatement("int varlenKeyColsSize = 0; int varlenValueColsSize = 0")
            .addStatement("$T keyCols = schema.keyColumns()", Columns.class)
            .addStatement("$T valCols = schema.valueColumns()", Columns.class);

        Columns keyCols = schema.keyColumns();
        if (keyCols.firstVarlengthColumn() >= 0) {
            final CodeBlock.Builder block = CodeBlock.builder().indent()
                .addStatement("$T fVal", Object.class);// Temporary vars.

            for (int i = keyCols.firstVarlengthColumn(); i < keyCols.length(); i++) {
                assert !keyCols.column(i).type().spec().fixedLength();

                block.addStatement("fVal = $L", keyMarsh.getValueCode("key", i).toString());

                block.beginControlFlow("if (fVal != null)")
                    .addStatement("varlenKeyColsSize += $T.getValueSize(fVal, keyCols.column($L).type())", MarshallerUtil.class, i)
                    .addStatement("varlenKeyCols++")
                    .endControlFlow();
            }
            block.unindent();

            builder
                .addCode("{\n")
                .addCode(block.build())
                .addCode("}\n");
        }

        Columns valCols = schema.valueColumns();
        if (valCols.firstVarlengthColumn() >= 0) {
            final CodeBlock.Builder block = CodeBlock.builder().indent()
                .addStatement("$T fVal", Object.class);// Temporary vars.

            for (int i = valCols.firstVarlengthColumn(); i < valCols.length(); i++) {
                assert !valCols.column(i).type().spec().fixedLength();

                block.addStatement("fVal = $L", valMarsh.getValueCode("val", i).toString());

                block.beginControlFlow("if (fVal != null)")
                    .addStatement("varlenValueColsSize += $T.getValueSize(fVal, valCols.column($L).type())", MarshallerUtil.class, i)
                    .addStatement("varlenValueCols++")
                    .endControlFlow();
            }
            block.unindent();

            builder
                .addCode("{\n")
                .addCode(block.build())
                .addCode("}\n");
        }

        builder.addStatement("int size = $T.tupleSize(keyCols, varlenKeyCols, varlenKeyColsSize," +
            "valCols, varlenValueCols, varlenValueColsSize)", TupleAssembler.class);

        builder.addStatement("return new $T(schema, size, varlenKeyCols, varlenValueCols)", TupleAssembler.class);

        return builder.build();
    }

    /**
     * @param valMarsh Value marshaller code generator.
     * @return Deserialize value method spec.
     */
    private MethodSpec generateDeserializeValueMethod(MarshallerCodeGenerator valMarsh) {
        return MethodSpec
            .methodBuilder("deserializeValue0")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PROTECTED, Modifier.FINAL)
            .addParameter(Tuple.class, "tuple", Modifier.FINAL)
            .addException(SerializationException.class)
            .returns(TypeName.OBJECT)

            .beginControlFlow("try")
            .addCode(valMarsh.unmarshallObjectCode("tuple"))
            .nextControlFlow("catch($T th)", Throwable.class)
            .addStatement("throw new $T(th)", SerializationException.class)
            .endControlFlow()
            .build();
    }

    /**
     * @param keyMarsh Key marshaller code generator.
     * @return Deserialize key method spec.
     */
    private MethodSpec generateDeserializeKeyMethod(MarshallerCodeGenerator keyMarsh) {
        return MethodSpec
            .methodBuilder("deserializeKey0")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PROTECTED, Modifier.FINAL)
            .addParameter(Tuple.class, "tuple", Modifier.FINAL)
            .addException(SerializationException.class)
            .returns(TypeName.OBJECT)

            .beginControlFlow("try")
            .addCode(keyMarsh.unmarshallObjectCode("tuple"))
            .nextControlFlow("catch($T th)", Throwable.class)
            .addStatement("throw new $T(th)", SerializationException.class)
            .endControlFlow()
            .build();
    }

    /**
     * @param keyMarsh Key marshaller code generator.
     * @param valMarsh Value marshaller code generator.
     * @return Serialize method spec.
     */
    private MethodSpec generateSerializeMethod(MarshallerCodeGenerator keyMarsh, MarshallerCodeGenerator valMarsh) {
        return MethodSpec.
            methodBuilder("serialize0")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PROTECTED, Modifier.FINAL)
            .addParameter(TupleAssembler.class, "asm", Modifier.FINAL)
            .addParameter(TypeName.OBJECT, "key", Modifier.FINAL)
            .addParameter(TypeName.OBJECT, "val", Modifier.FINAL)
            .addException(SerializationException.class)
            .returns(ArrayTypeName.of(TypeName.BYTE))

            .beginControlFlow("try")
            .addCode(keyMarsh.marshallObjectCode("asm", "key"))
            .addCode(valMarsh.marshallObjectCode("asm", "val"))
            .addStatement("return asm.build()")

            .nextControlFlow("catch($T th)", Throwable.class)
            .addStatement("throw new $T(th)", SerializationException.class)
            .endControlFlow()
            .build();
    }

    /**
     * Creates marshaller code generator for given class.
     *
     * @param tClass Target class.
     * @param factoryRefVar Object factory variable.
     * @param columns Columns that tClass mapped to.
     * @param firstColIdx First column absolute index in schema.
     * @return Marshaller code generator.
     */
    private MarshallerCodeGenerator createObjectMarshaller(
        Class<?> tClass,
        @Nullable String factoryRefVar,
        Columns columns,
        int firstColIdx
    ) {
        BinaryMode mode = MarshallerUtil.mode(tClass);

        if (mode != null) // Simple type.
            return new IdentityObjectMarshallerExprGenerator(TupleColumnAccessCodeGenerator.createAccessor(mode, firstColIdx));
        else
            return new ObjectMarshallerCodeGenerator(tClass, factoryRefVar, columns, firstColIdx);
    }
}
