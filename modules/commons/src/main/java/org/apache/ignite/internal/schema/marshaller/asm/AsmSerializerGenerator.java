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

package org.apache.ignite.internal.schema.marshaller.asm;

import com.facebook.presto.bytecode.Access;
import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.bytecode.ClassGenerator;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.Parameter;
import com.facebook.presto.bytecode.ParameterizedType;
import com.facebook.presto.bytecode.Scope;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.control.IfStatement;
import com.facebook.presto.bytecode.expression.BytecodeExpression;
import com.facebook.presto.bytecode.expression.BytecodeExpressions;
import java.io.StringWriter;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.concurrent.TimeUnit;
import javax.annotation.processing.Generated;
import jdk.jfr.Experimental;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.Columns;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.Tuple;
import org.apache.ignite.internal.schema.TupleAssembler;
import org.apache.ignite.internal.schema.marshaller.AbstractSerializer;
import org.apache.ignite.internal.schema.marshaller.BinaryMode;
import org.apache.ignite.internal.schema.marshaller.MarshallerUtil;
import org.apache.ignite.internal.schema.marshaller.SerializationException;
import org.apache.ignite.internal.schema.marshaller.Serializer;
import org.apache.ignite.internal.schema.marshaller.SerializerFactory;
import org.apache.ignite.internal.util.ObjectFactory;
import org.jetbrains.annotations.NotNull;

/**
 * {@link Serializer} code generator.
 */
@Experimental
public class AsmSerializerGenerator implements SerializerFactory {
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
        final String className = SERIALIZER_CLASS_NAME_PREFIX + schema.version();

        final StringWriter writer = new StringWriter();
        try {
            // Generate Serializer code.
            long generation = System.nanoTime();

            final ClassDefinition classDef = generateSerializerClass(className, schema, keyClass, valClass);

            long compilationTime = System.nanoTime();
            generation = compilationTime - generation;

            final Class<? extends Serializer> aClass = ClassGenerator.classGenerator(getClassLoader())
                .fakeLineNumbers(true)
                .runAsmVerifier(true)
                .dumpRawBytecode(true)
                .dumpClassFilesTo(Paths.get("./target"))
//                .outputTo(writer)
                .defineClass(classDef, Serializer.class);

            compilationTime = System.nanoTime() - compilationTime;

            //TODO: pass code to logger on trace level.
            System.out.println("Serializer created: generated=" + TimeUnit.NANOSECONDS.toMicros(generation) + "us\n" +
                ", compiled=" + TimeUnit.NANOSECONDS.toMicros(compilationTime) + "us\n" +
                writer.toString());

            // Instantiate serializer.
            return aClass.getDeclaredConstructor(SchemaDescriptor.class, Class.class, Class.class)
                .newInstance(schema, keyClass, valClass);

        }
        catch (Exception e) {
            throw new IllegalStateException("Failed to create serializer for key-value pair: schemaVer=" + schema.version() +
                ", keyClass=" + keyClass.getSimpleName() + ", valueClass=" + valClass.getSimpleName() + " code=\n" + writer.toString(), e);
        }
    }

    @NotNull private ClassDefinition generateSerializerClass(String className,
        SchemaDescriptor schema, Class<?> keyClass, Class<?> valClass) throws ReflectiveOperationException {

        final ClassDefinition classDef = new ClassDefinition(
            EnumSet.of(Access.PUBLIC),
            SERIALIZER_PACKAGE_NAME.replace('.', '/') + '/' + className,
            ParameterizedType.type(AbstractSerializer.class)
        );

        classDef.declareAnnotation(Generated.class).setValue("value", getClass().getCanonicalName());

        MarshallerCodeGenerator keyMarsh = createMarshaller(keyClass, schema.keyColumns(), 0);
        MarshallerCodeGenerator valMarsh = createMarshaller(valClass, schema.valueColumns(), schema.keyColumns().length());

        generateStaticHandlers(classDef, keyMarsh, valMarsh);

        generateConstructor(classDef);
        generateAssemblerFactoryMethod(classDef, schema, keyMarsh, valMarsh);

        generateSerializeMethod(classDef, keyMarsh, valMarsh);
        generateDeserializeKeyMethod(classDef, keyMarsh);
        generateDeserializeValueMethod(classDef, valMarsh);

        return classDef;
    }

    private void generateStaticHandlers(
        ClassDefinition classDef,
        MarshallerCodeGenerator keyMarsh,
        MarshallerCodeGenerator valMarsh
    ) {
        keyMarsh.initStaticHandlers(classDef);
        valMarsh.initStaticHandlers(classDef);
    }

    @NotNull
    private static MarshallerCodeGenerator createMarshaller(Class<?> tClass, Columns columns, int firstColumnIdx) {
        final BinaryMode mode = MarshallerUtil.mode(tClass);

        if (mode == null)
            return new ObjectMarshallerCodeGenerator(columns, tClass, firstColumnIdx);
        else
            return new IdentityMarshallerCodeGenerator(TupleColumnAccessCodeGenerator.createAccessor(mode, firstColumnIdx));
    }

    private void generateConstructor(ClassDefinition classDef) {
        final MethodDefinition constrDef = classDef.declareConstructor(
            EnumSet.of(Access.PUBLIC),
            Parameter.arg("schema", SchemaDescriptor.class),
            Parameter.arg("keyClass", Class.class),
            Parameter.arg("valClass", Class.class)
        );

        constrDef.getBody()
            .append(constrDef.getThis())
            .append(constrDef.getScope().getVariable("schema"))
            .invokeConstructor(classDef.getSuperClass(), ParameterizedType.type(SchemaDescriptor.class))
            .ret();
    }

    private void generateAssemblerFactoryMethod(
        ClassDefinition classDef,
        SchemaDescriptor schema,
        MarshallerCodeGenerator keyMarsh,
        MarshallerCodeGenerator valMarsh
    ) throws ReflectiveOperationException {
        final MethodDefinition methodDef = classDef.declareMethod(
            EnumSet.of(Access.PROTECTED),
            "createAssembler",
            ParameterizedType.type(TupleAssembler.class),
            Parameter.arg("key", Object.class),
            Parameter.arg("val", Object.class)
        );
        methodDef.declareAnnotation(Override.class);

        final Scope scope = methodDef.getScope();
        final BytecodeBlock body = methodDef.getBody();

        final Variable varlenKeyCols = scope.declareVariable("varlenKeyCols", body, BytecodeExpressions.defaultValue(int.class));
        final Variable varlenValueCols = scope.declareVariable("varlenValueCols", body, BytecodeExpressions.defaultValue(int.class));
        final Variable varlenKeyColsSize = scope.declareVariable("varlenKeyColsSize", body, BytecodeExpressions.defaultValue(int.class));
        final Variable varlenValueColsSize = scope.declareVariable("varlenValueColsSize", body, BytecodeExpressions.defaultValue(int.class));

        final Variable keyCols = scope.declareVariable(Columns.class, "keyCols");
        final Variable valCols = scope.declareVariable(Columns.class, "valCols");

        body.append(keyCols.set(
            methodDef.getThis().getField("schema", SchemaDescriptor.class)
                .invoke("keyColumns", Columns.class)));
        body.append(valCols.set(
            methodDef.getThis().getField("schema", SchemaDescriptor.class)
                .invoke("valueColumns", Columns.class)));

        Columns columns = schema.keyColumns();
        if (columns.firstVarlengthColumn() >= 0) {
            final Variable tmp = scope.createTempVariable(Object.class);

            for (int i = columns.firstVarlengthColumn(); i < columns.length(); i++) {
                assert !columns.column(i).type().spec().fixedLength();

                body.append(keyMarsh.getValue(classDef.getType(), scope.getVariable("key"), i)).putVariable(tmp);
                body.append(new IfStatement().condition(BytecodeExpressions.isNotNull(tmp)).ifTrue(
                    new BytecodeBlock()
                        .append(varlenKeyCols.increment())
                        .append(BytecodeExpressions.add(
                            varlenKeyColsSize,
                            getColumnValueSize(tmp, keyCols, i))
                        )
                        .putVariable(varlenKeyColsSize))
                );
            }
        }

        columns = schema.valueColumns();
        if (columns.firstVarlengthColumn() >= 0) {
            final Variable tmp = scope.createTempVariable(Object.class);

            for (int i = columns.firstVarlengthColumn(); i < columns.length(); i++) {
                assert !columns.column(i).type().spec().fixedLength();

                body.append(valMarsh.getValue(classDef.getType(), scope.getVariable("val"), i)).putVariable(tmp);
                body.append(new IfStatement().condition(BytecodeExpressions.isNotNull(tmp)).ifTrue(
                    new BytecodeBlock()
                        .append(varlenValueCols.increment())
                        .append(BytecodeExpressions.add(
                            varlenValueColsSize,
                            getColumnValueSize(tmp, valCols, i))
                        )
                        .putVariable(varlenValueColsSize))
                );
            }
        }

        body.append(BytecodeExpressions.newInstance(TupleAssembler.class,
            methodDef.getThis().getField("schema", SchemaDescriptor.class),
            BytecodeExpressions.invokeStatic(TupleAssembler.class, "tupleSize", int.class,
                keyCols, varlenKeyCols, varlenKeyColsSize,
                valCols, varlenValueCols, varlenValueColsSize),
            varlenKeyCols,
            varlenValueCols));

        body.retObject();
    }

    @NotNull private BytecodeExpression getColumnValueSize(Variable obj, Variable cols, int i) {
        return BytecodeExpressions.invokeStatic(MarshallerUtil.class, "getValueSize",
            int.class,
            Arrays.asList(Object.class, NativeType.class),
            obj,
            cols.invoke("column", Column.class, BytecodeExpressions.constantInt(i))
                .invoke("type", NativeType.class)
        );
    }

    private void generateSerializeMethod(
        ClassDefinition classDef,
        MarshallerCodeGenerator keyMarsh,
        MarshallerCodeGenerator valMarsh
    ) throws ReflectiveOperationException {
        final MethodDefinition methodDef = classDef.declareMethod(
            EnumSet.of(Access.PROTECTED),
            "serialize0",
            ParameterizedType.type(byte[].class),
            Parameter.arg("asm", TupleAssembler.class),
            Parameter.arg("key", Object.class),
            Parameter.arg("val", Object.class)
        )
            .addException(SerializationException.class);
        methodDef.declareAnnotation(Override.class);

        final Variable asm = methodDef.getScope().getVariable("asm");

        methodDef.getBody()
            .append(new IfStatement().condition(BytecodeExpressions.isNull(asm)).ifTrue(
                new BytecodeBlock()
                    .append(BytecodeExpressions.newInstance(IllegalStateException.class, BytecodeExpressions.constantString("ASM can't be null.")))
                    .throwObject()
            ));

        methodDef.getBody()
            .append(
                keyMarsh.marshallObject(
                    classDef.getType(),
                    asm,
                    methodDef.getScope().getVariable("key"))
            )
            .append(
                valMarsh.marshallObject(
                    classDef.getType(),
                    asm,
                    methodDef.getScope().getVariable("val"))
            )
            .append(asm.invoke("build", byte[].class))
            .retObject();
    }

    private void generateDeserializeKeyMethod(ClassDefinition classDef,
        MarshallerCodeGenerator keyMarsh) throws ReflectiveOperationException {
        final MethodDefinition methodDef = classDef.declareMethod(
            EnumSet.of(Access.PROTECTED),
            "deserializeKey0",
            ParameterizedType.type(Object.class),
            Parameter.arg("tuple", Tuple.class)
        )
            .addException(SerializationException.class);
        methodDef.declareAnnotation(Override.class);

        final Variable obj = methodDef.getScope().declareVariable(Object.class, "obj");

        methodDef.getBody()
            .append(keyMarsh.unmarshallObject(classDef.getType(), methodDef.getScope().getVariable("tuple"), obj))
            .append(obj)
            .retObject();
    }

    private void generateDeserializeValueMethod(ClassDefinition classDef,
        MarshallerCodeGenerator valMarsh) throws ReflectiveOperationException {
        final MethodDefinition methodDef = classDef.declareMethod(
            EnumSet.of(Access.PROTECTED),
            "deserializeValue0",
            ParameterizedType.type(Object.class),
            Parameter.arg("tuple", Tuple.class)
        )
            .addException(SerializationException.class);
        methodDef.declareAnnotation(Override.class);

        final Variable obj = methodDef.getScope().declareVariable(Object.class, "obj");

        methodDef.getBody()
            .append(valMarsh.unmarshallObject(classDef.getType(), methodDef.getScope().getVariable("tuple"), obj))
            .append(obj)
            .retObject();
    }

//    /**
//     * @param valMarsh Value marshaller code generator.
//     * @return Deserialize value method spec.
//     */
//    private MethodSpec generateDeserializeValueMethod(MarshallerCodeGenerator valMarsh) {
//        return MethodSpec
//            .methodBuilder("deserializeValue0")
//            .addAnnotation(Override.class)
//            .addModifiers(Modifier.PROTECTED, Modifier.FINAL)
//            .addParameter(Tuple.class, "tuple", Modifier.FINAL)
//            .addException(SerializationException.class)
//            .returns(TypeName.OBJECT)
//
//            .beginControlFlow("try")
//            .addCode(valMarsh.unmarshallObjectCode("tuple"))
//            .nextControlFlow("catch($T th)", Throwable.class)
//            .addStatement("throw new $T(th)", SerializationException.class)
//            .endControlFlow()
//            .build();
//    }
//
//    /**
//     * @param keyMarsh Key marshaller code generator.
//     * @return Deserialize key method spec.
//     */
//    private MethodSpec generateDeserializeKeyMethod(MarshallerCodeGenerator keyMarsh) {
//        return MethodSpec
//            .methodBuilder("deserializeKey0")
//            .addAnnotation(Override.class)
//            .addModifiers(Modifier.PROTECTED, Modifier.FINAL)
//            .addParameter(Tuple.class, "tuple", Modifier.FINAL)
//            .addException(SerializationException.class)
//            .returns(TypeName.OBJECT)
//
//            .beginControlFlow("try")
//            .addCode(keyMarsh.unmarshallObjectCode("tuple"))
//            .nextControlFlow("catch($T th)", Throwable.class)
//            .addStatement("throw new $T(th)", SerializationException.class)
//            .endControlFlow()
//            .build();
//    }
//
//    /**
//     * Creates marshaller code generator for given class.
//     *
//     * @param tClass Target class.
//     * @param factoryRefVar Object factory variable.
//     * @param columns Columns that tClass mapped to.
//     * @param firstColIdx First column absolute index in schema.
//     * @return Marshaller code generator.
//     */
//    private MarshallerCodeGenerator createObjectMarshaller(
//        Class<?> tClass,
//        @Nullable String factoryRefVar,
//        Columns columns,
//        int firstColIdx
//    ) {
//        BinaryMode mode = MarshallerUtil.mode(tClass);
//
//        if (mode != null) // Simple type.
//            return new IdentityObjectMarshallerExprGenerator(TupleColumnAccessCodeGenerator.createAccessor(mode, firstColIdx));
//        else
//            return new ObjectMarshallerCodeGenerator(tClass, factoryRefVar, columns, firstColIdx);
//    }

    private static BytecodeExpression cretaObjectFactoryExpr(MethodDefinition constrDef, String aClass) {
        return BytecodeExpressions.invokeStatic(
            MarshallerUtil.class,
            "factoryForClass",
            ObjectFactory.class,
            constrDef.getScope().getVariable(aClass)
        );
    }

    private static ClassLoader getClassLoader() {
        return Thread.currentThread().getContextClassLoader() == null ?
            ClassLoader.getSystemClassLoader() :
            Thread.currentThread().getContextClassLoader();
    }
}
