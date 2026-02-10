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

package org.apache.ignite.internal.schema.marshaller.asm;

import static com.facebook.presto.bytecode.expression.BytecodeExpressions.add;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantBoolean;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantInt;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantString;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.defaultValue;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.invokeStatic;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.isNull;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.newInstance;

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
import com.facebook.presto.bytecode.control.TryCatch;
import com.facebook.presto.bytecode.expression.BytecodeExpression;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.annotation.processing.Generated;
import jdk.jfr.Experimental;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.marshaller.BinaryMode;
import org.apache.ignite.internal.marshaller.Marshaller;
import org.apache.ignite.internal.marshaller.MarshallerColumn;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.marshaller.KvMarshaller;
import org.apache.ignite.internal.schema.marshaller.MarshallerFactory;
import org.apache.ignite.internal.schema.marshaller.MarshallerUtil;
import org.apache.ignite.internal.schema.marshaller.RecordMarshaller;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.util.ObjectFactory;
import org.apache.ignite.lang.MarshallerException;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.table.mapper.PojoMapper;

/**
 * {@link Marshaller} code generator.
 */
@Experimental
public class AsmMarshallerGenerator implements MarshallerFactory {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(AsmMarshallerGenerator.class);

    /** Marshaller package name. */
    public static final String MARSHALLER_PACKAGE_NAME = "org.apache.ignite.internal.schema.marshaller";

    /** Marshaller package name prefix. */
    public static final String MARSHALLER_CLASS_NAME_PREFIX = "MarshallerForSchema_";
    /** Dump generated code. */
    private final boolean dumpCode = LOG.isTraceEnabled();

    /** {@inheritDoc} */
    @Override
    public <K, V> KvMarshaller<K, V> create(SchemaDescriptor schema, Mapper<K> keyMapper, Mapper<V> valueMapper) {
        final String className = MARSHALLER_CLASS_NAME_PREFIX + schema.version();

        Class<K> keyClass = keyMapper.targetType();
        Class<V> valClass = valueMapper.targetType();
        final StringWriter writer = new StringWriter();
        try {
            // Generate Marshaller code.
            long generation = System.nanoTime();

            final ClassDefinition classDef = generateMarshallerClass(className, schema, keyMapper, valueMapper);
            long compilationTime = System.nanoTime();
            generation = compilationTime - generation;

            ClassGenerator generator = ClassGenerator.classGenerator(getClassLoader());

            if (dumpCode) {
                generator = generator.outputTo(writer)
                        .fakeLineNumbers(true)
                        .runAsmVerifier(true)
                        .dumpRawBytecode(true);
            }

            final Class<? extends KvMarshaller> aClass = generator.defineClass(classDef, KvMarshaller.class);
            compilationTime = System.nanoTime() - compilationTime;

            if (LOG.isTraceEnabled()) {
                LOG.trace("ASM marshaller created: codeGenStage={}us, compileStage={}us. Code: {}",
                        TimeUnit.NANOSECONDS.toMicros(generation), TimeUnit.NANOSECONDS.toMicros(compilationTime), writer);
            } else if (LOG.isDebugEnabled()) {
                LOG.debug("ASM marshaller created: codeGenStage={}us, compileStage={}us.",
                        TimeUnit.NANOSECONDS.toMicros(generation), TimeUnit.NANOSECONDS.toMicros(compilationTime));
            }

            // Instantiate marshaller.
            //noinspection unchecked
            return aClass
                    .getDeclaredConstructor(
                            SchemaDescriptor.class,
                            ObjectFactory.class,
                            ObjectFactory.class)
                    .newInstance(
                            schema,
                            MarshallerUtil.factoryForClass(keyClass),
                            MarshallerUtil.factoryForClass(valClass));

        } catch (LinkageError | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new MarshallerException("Failed to create marshaller for key-value pair: schemaVer=" + schema.version()
                    + ", keyClass=" + keyClass.getSimpleName() + ", valueClass=" + valClass.getSimpleName(), e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public <R> RecordMarshaller<R> create(SchemaDescriptor schema, Mapper<R> mapper) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
     * Generates marshaller class definition.
     *
     * @param className Marshaller class name.
     * @param schema    Schema descriptor.
     * @param keyMapper Key mapper.
     * @param valMapper Value mapper.
     * @return Generated java class definition.
     */
    private ClassDefinition generateMarshallerClass(
            String className,
            SchemaDescriptor schema,
            Mapper<?> keyMapper,
            Mapper<?> valMapper
    ) {
        MarshallerCodeGenerator keyMarsh = createMarshaller(keyMapper, schema.keyColumns(), 0);
        MarshallerCodeGenerator valMarsh = createMarshaller(valMapper, schema.valueColumns(), schema.keyColumns().size());

        final ClassDefinition classDef = new ClassDefinition(
                EnumSet.of(Access.PUBLIC),
                MARSHALLER_PACKAGE_NAME.replace('.', '/') + '/' + className,
                ParameterizedType.type(Object.class),
                ParameterizedType.type(KvMarshaller.class)
        );

        classDef.declareAnnotation(Generated.class).setValue("value", getClass().getCanonicalName());

        keyMarsh.initStaticHandlers(classDef);
        valMarsh.initStaticHandlers(classDef);

        generateFieldsAndConstructor(classDef);
        generateAssemblerFactoryMethod(classDef, schema, keyMarsh, valMarsh);

        generateSchemaVersionMethod(classDef, schema);

        generateMarshalMethod(classDef, keyMarsh, valMarsh);
        generateUnmarshalKeyMethod(classDef, keyMarsh);
        generateUnmarshalValueMethod(classDef, valMarsh);
        return classDef;
    }

    /**
     * Creates interface method.
     *
     * @param classDef Marshaller class definition.
     * @param schema   Marshaller schema.
     */
    private void generateSchemaVersionMethod(ClassDefinition classDef, SchemaDescriptor schema) {
        final MethodDefinition methodDef = classDef.declareMethod(
                EnumSet.of(Access.PUBLIC),
                "schemaVersion",
                ParameterizedType.type(int.class));

        methodDef.declareAnnotation(Override.class);

        methodDef.getBody().push(schema.version()).retInt();
    }

    /**
     * Creates marshaller code generator for given class.
     *
     * @param mapper      Mapper.
     * @param columns     Columns that cls mapped to.
     * @param firstColIdx First column absolute index in schema.
     * @return Marshaller code generator.
     */
    private static MarshallerCodeGenerator createMarshaller(
            Mapper<?> mapper,
            List<Column> columns,
            int firstColIdx
    ) {
        BinaryMode mode = BinaryMode.forClass(mapper.targetType());

        if (mode == BinaryMode.POJO) {
            MarshallerColumn[] marshallerColumns = MarshallerUtil.toMarshallerColumns(columns);

            return new ObjectMarshallerCodeGenerator(marshallerColumns, (PojoMapper<?>) mapper, firstColIdx);
        } else {
            return new IdentityMarshallerCodeGenerator(ColumnAccessCodeGenerator.createAccessor(mode, null, firstColIdx));
        }
    }

    /**
     * Generates fields and constructor.
     *
     * @param classDef Marshaller class definition.
     */
    private void generateFieldsAndConstructor(ClassDefinition classDef) {
        classDef.declareField(EnumSet.of(Access.PRIVATE, Access.FINAL), "keyFactory", ParameterizedType.type(ObjectFactory.class));
        classDef.declareField(EnumSet.of(Access.PRIVATE, Access.FINAL), "valFactory", ParameterizedType.type(ObjectFactory.class));
        classDef.declareField(EnumSet.of(Access.PRIVATE, Access.FINAL), "schema", ParameterizedType.type(SchemaDescriptor.class));

        final MethodDefinition constrDef = classDef.declareConstructor(
                EnumSet.of(Access.PUBLIC),
                Parameter.arg("schema", SchemaDescriptor.class),
                Parameter.arg("keyFactory", ParameterizedType.type(ObjectFactory.class)),
                Parameter.arg("valFactory", ParameterizedType.type(ObjectFactory.class))
        );

        constrDef.getBody()
                .append(constrDef.getThis())
                .invokeConstructor(classDef.getSuperClass())
                .append(constrDef.getThis().setField("schema", constrDef.getScope().getVariable("schema")))
                .append(constrDef.getThis().setField("keyFactory", constrDef.getScope().getVariable("keyFactory")))
                .append(constrDef.getThis().setField("valFactory", constrDef.getScope().getVariable("valFactory")))
                .ret();
    }

    /**
     * Generates helper method.
     *
     * @param classDef Marshaller class definition.
     * @param schema   Schema descriptor.
     * @param keyMarsh Key marshaller code generator.
     * @param valMarsh Value marshaller code generator.
     */
    private void generateAssemblerFactoryMethod(
            ClassDefinition classDef,
            SchemaDescriptor schema,
            MarshallerCodeGenerator keyMarsh,
            MarshallerCodeGenerator valMarsh
    ) {
        final MethodDefinition methodDef = classDef.declareMethod(
                EnumSet.of(Access.PRIVATE),
                "createAssembler",
                ParameterizedType.type(RowAssembler.class),
                Parameter.arg("key", Object.class),
                Parameter.arg("val", Object.class)
        );

        final Scope scope = methodDef.getScope();
        final BytecodeBlock body = methodDef.getBody();

        Variable estimatedValueSize = scope.declareVariable("estimatedValueSize", body, defaultValue(int.class));

        BytecodeExpression schemaField = methodDef.getThis().getField("schema", SchemaDescriptor.class);

        Variable keyCols = scope.declareVariable("keyCols", body,
                schemaField.invoke("keyColumns", ParameterizedType.type(List.class, Column.class), List.of()));
        Variable valCols = scope.declareVariable("valCols", body,
                schemaField.invoke("valueColumns", ParameterizedType.type(List.class, Column.class), List.of()));

        List<Column> columns = schema.keyColumns();
        Variable value = scope.createTempVariable(Object.class);
        boolean exactSizeEstimate = true;
        for (int i = 0; i < columns.size(); i++) {
            body.append(keyMarsh.getValue(classDef.getType(), scope.getVariable("key"), i)).putVariable(value);
            NativeType type = columns.get(i).type();
            BytecodeExpression valueSize = type.fixedLength()
                    ? constantInt(type.sizeInBytes())
                    : getValueSize(value, getColumnType(keyCols, i));
            exactSizeEstimate = exactSizeEstimate && type.fixedLength();
            body.append(new IfStatement().condition(isNull(value)).ifFalse(plusEquals(estimatedValueSize, valueSize)));
        }

        columns = schema.valueColumns();

        for (int i = 0; i < columns.size(); i++) {
            body.append(valMarsh.getValue(classDef.getType(), scope.getVariable("val"), i)).putVariable(value);
            NativeType type = columns.get(i).type();
            BytecodeExpression valueSize = type.fixedLength()
                    ? constantInt(type.sizeInBytes())
                    : getValueSize(value, getColumnType(valCols, i));
            exactSizeEstimate = exactSizeEstimate && type.fixedLength();
            body.append(new IfStatement().condition(isNull(value)).ifFalse(plusEquals(estimatedValueSize, valueSize)));
        }

        body.append(newInstance(RowAssembler.class, schemaField, estimatedValueSize, constantBoolean(exactSizeEstimate)));

        body.retObject();
    }

    private static BytecodeExpression getValueSize(Variable val, BytecodeExpression type) {
        return invokeStatic(MarshallerUtil.class, "getValueSize", int.class, val, type);
    }

    private static BytecodeExpression getColumnType(Variable columns, int i) {
        return columns.invoke("get", Object.class, constantInt(i))
                .cast(Column.class)
                .invoke("type", NativeType.class);
    }

    private static BytecodeExpression plusEquals(Variable var, BytecodeExpression expression) {
        return var.set(add(var, expression));
    }

    /**
     * Generates marshal method.
     *
     * @param classDef Marshaller class definition.
     * @param keyMarsh Key marshaller code generator.
     * @param valMarsh Value marshaller code generator.
     */
    private void generateMarshalMethod(
            ClassDefinition classDef,
            MarshallerCodeGenerator keyMarsh,
            MarshallerCodeGenerator valMarsh
    ) {
        final MethodDefinition methodDef = classDef.declareMethod(
                EnumSet.of(Access.PUBLIC),
                "marshal",
                ParameterizedType.type(Row.class),
                Parameter.arg("key", Object.class),
                Parameter.arg("val", Object.class)
        ).addException(MarshallerException.class);

        methodDef.declareAnnotation(Override.class);

        final Variable asm = methodDef.getScope().createTempVariable(RowAssembler.class);

        methodDef.getBody()
                .append(asm.set(methodDef.getScope().getThis().invoke("createAssembler",
                        RowAssembler.class,
                        methodDef.getScope().getVariable("key"),
                        methodDef.getScope().getVariable("val"))))
                .append(new IfStatement().condition(isNull(asm)).ifTrue(
                        new BytecodeBlock()
                                .append(newInstance(IgniteInternalException.class,
                                        constantString("ASM can't be null.")))
                                .throwObject()
                ));

        final BytecodeBlock block = new BytecodeBlock();
        block.append(
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
                .append(
                        invokeStatic(
                                Row.class,
                                "wrapBinaryRow",
                                Row.class,
                                methodDef.getThis().getField("schema", SchemaDescriptor.class),
                                asm.invoke("build", BinaryRow.class))
                )
                .retObject();

        final Variable ex = methodDef.getScope().createTempVariable(Throwable.class);
        BytecodeExpression message = ex.invoke("getMessage", String.class);
        methodDef.getBody().append(new TryCatch(
                block,
                new BytecodeBlock()
                        .putVariable(ex)
                        .append(newInstance(MarshallerException.class, message, ex))
                        .throwObject(),
                ParameterizedType.type(Throwable.class)
        ));

    }

    /**
     * Generates unmarshal key method.
     *
     * @param classDef Marshaller class definition.
     * @param keyMarsh Key marshaller code generator.
     */
    private void generateUnmarshalKeyMethod(ClassDefinition classDef, MarshallerCodeGenerator keyMarsh) {
        final MethodDefinition methodDef = classDef.declareMethod(
                EnumSet.of(Access.PUBLIC),
                "unmarshalKey",
                ParameterizedType.type(Object.class),
                Parameter.arg("row", Row.class)
        ).addException(MarshallerException.class);

        methodDef.declareAnnotation(Override.class);

        final Variable objVar = methodDef.getScope().declareVariable(Object.class, "obj");
        final Variable objFactory = methodDef.getScope().declareVariable("factory",
                methodDef.getBody(), methodDef.getThis().getField("keyFactory", ObjectFactory.class));

        methodDef.getBody()
                .append(keyMarsh.unmarshallObject(classDef.getType(), methodDef.getScope().getVariable("row"), objVar, objFactory))
                .append(objVar)
                .retObject();
    }

    /**
     * Generates unmarshal value method.
     *
     * @param classDef Marshaller class definition.
     * @param valMarsh Value marshaller code generator.
     */
    private void generateUnmarshalValueMethod(ClassDefinition classDef, MarshallerCodeGenerator valMarsh) {
        final MethodDefinition methodDef = classDef.declareMethod(
                EnumSet.of(Access.PUBLIC),
                "unmarshalValue",
                ParameterizedType.type(Object.class),
                Parameter.arg("row", Row.class)
        ).addException(MarshallerException.class);

        methodDef.declareAnnotation(Override.class);

        final Variable obj = methodDef.getScope().declareVariable(Object.class, "obj");
        final Variable objFactory = methodDef.getScope().declareVariable("factory",
                methodDef.getBody(), methodDef.getThis().getField("valFactory", ObjectFactory.class));

        methodDef.getBody()
                .append(valMarsh.unmarshallObject(classDef.getType(), methodDef.getScope().getVariable("row"), obj, objFactory))
                .append(obj)
                .retObject();
    }

    /**
     * Resolves current classloader.
     *
     * @return Classloader.
     */
    public static ClassLoader getClassLoader() {
        return Thread.currentThread().getContextClassLoader() == null
                ? ClassLoader.getSystemClassLoader() : Thread.currentThread().getContextClassLoader();
    }
}
