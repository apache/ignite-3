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

package org.apache.ignite.internal.schema.marshaller.generator;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import org.apache.ignite.internal.schema.Columns;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.marshaller.BinaryMode;
import org.apache.ignite.internal.schema.marshaller.MarshallerUtil;
import org.apache.ignite.internal.schema.marshaller.Serializer;
import org.apache.ignite.internal.schema.marshaller.SerializerFactory;
import org.apache.ignite.internal.util.IgniteUnsafeUtils;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IClassBodyEvaluator;
import org.jetbrains.annotations.Nullable;

/**
 * {@link Serializer} code generator backed with Janino.
 */
public class JaninoSerializerGenerator implements SerializerFactory {
    /** Tabulate. */
    static final String TAB = "    ";

    /** Line feed. */
    static final char LF = '\n';

    /** String buffer initial size. */
    public static final int INITIAL_BUFFER_SIZE = 8 * 1024;

    /** Debug flag. */
    private static final boolean enabledDebug = true;

    /** {@inheritDoc} */
    @Override public Serializer create(
        SchemaDescriptor schema,
        Class<?> keyClass,
        Class<?> valClass
    ) {
        try {
            final IClassBodyEvaluator ce = CompilerFactoryFactory.getDefaultCompilerFactory().newClassBodyEvaluator();

            // Generate Serializer code.
            String code = generateSerializerClassCode(ce, schema, keyClass, valClass);

            //TODO: pass code to logger on trace level.

            if (enabledDebug) {
                ce.setDebuggingInformation(true, true, true);

                //TODO: dump code to log.
//                System.out.println(code);
            }

            try {  // Compile and load class.
                ce.setParentClassLoader(getClass().getClassLoader());
                ce.cook(code);

                // Create and return Serializer instance.
                final Constructor<Serializer> ctor = (Constructor<Serializer>)ce.getClazz()
                    .getDeclaredConstructor(schema.getClass(), Class.class, Class.class);

                return ctor.newInstance(schema, keyClass, valClass);
            }
            catch (Exception ex) {
                if (enabledDebug)
                    throw new IllegalStateException("Failed to compile/instantiate generated Serializer: code=" +
                        LF + code + LF, ex);
                else
                    throw new IllegalStateException("Failed to compile/instantiate generated Serializer.", ex);
            }
        }
        catch (Exception ex) {
            //TODO: fallback to java serializer?
            throw new IllegalStateException(ex);
        }
    }

    /**
     * Generates serializer code.
     *
     * @param ce Class body evaluator.
     * @param schema Schema descriptor.
     * @param keyClass Key class.
     * @param valClass Value class.
     * @return Generated class code.
     */
    private String generateSerializerClassCode(
        IClassBodyEvaluator ce,
        SchemaDescriptor schema,
        Class<?> keyClass,
        Class<?> valClass
    ) {
        final String packageName = "org.apache.ignite.internal.schema.marshaller.";
        final String className = "JaninoSerializerForSchema_" + schema.version();

        // Prerequisites.
        ce.setClassName(packageName + className);
        ce.setImplementedInterfaces(new Class[] {Serializer.class});
        ce.setDefaultImports(
            "java.util.UUID",
            "java.util.BitSet",

            "org.apache.ignite.internal.schema.ByteBufferTuple",
            "org.apache.ignite.internal.schema.Columns",
            "org.apache.ignite.internal.schema.SchemaDescriptor",
            "org.apache.ignite.internal.schema.Tuple",
            "org.apache.ignite.internal.schema.TupleAssembler",
            "org.apache.ignite.internal.util.IgniteUnsafeUtils",
            "org.apache.ignite.internal.util.ObjectFactory"
        );

        // Build field accessor generators.
        final MarshallerExprGenerator keyMarsh = createObjectMarshaller(keyClass, "keyFactory", schema.keyColumns(), 0);
        final MarshallerExprGenerator valMarsh = createObjectMarshaller(valClass, "valFactory", schema.valueColumns(), schema.keyColumns().length());

        // Create buffer.
        final StringBuilder sb = new StringBuilder(INITIAL_BUFFER_SIZE);

        // Append class fields desctiption.
        sb.append("private final SchemaDescriptor schema;" + LF);

        if (!keyMarsh.isSimpleTypeMarshaller())
            sb.append("private final ObjectFactory keyFactory;" + LF);
        if (!valMarsh.isSimpleTypeMarshaller())
            sb.append("private final ObjectFactory valFactory;" + LF);

        // Append constructor code.
        sb.append(LF + "public ").append(className).append("(SchemaDescriptor schema, Class kClass, Class vClass) {" + LF);
        sb.append(TAB + "this.schema = schema; " + LF);
        if (!keyMarsh.isSimpleTypeMarshaller())
            sb.append(TAB + "keyFactory = new ObjectFactory(kClass);" + LF);
        if (!valMarsh.isSimpleTypeMarshaller())
            sb.append(TAB + "valFactory = new ObjectFactory(vClass);" + LF);
        sb.append("}" + LF);

        // Generate and append helper-methods.
        generateTupleFactoryMethod(sb, schema, keyMarsh, valMarsh);

        // Generate and append Serializer interface methods.
        appendSerializeMethod(sb, keyMarsh, valMarsh);
        writeDeserializeKeyMethod(sb, keyMarsh);
        writeDeserializeValueMethod(sb, valMarsh);

        return sb.toString();
    }

    /**
     * Creates marshal/unmarshall expressions generator for object.
     *
     * @param aClass Object class.
     * @param factoryRefExpr Factory reference expression.
     * @param columns Columns that aClass mapped to.
     * @param firstColIdx First column absolute index in schema.
     * @return Marshal/unmarshall expression generator.
     */
    private MarshallerExprGenerator createObjectMarshaller(
        Class<?> aClass,
        @Nullable String factoryRefExpr,
        Columns columns,
        int firstColIdx
    ) {
        BinaryMode mode = MarshallerUtil.mode(aClass);

        if (mode != null)
            return new IdentityObjectMarshallerExprGenerator(FieldAccessExprGenerator.createIdentityAccessor(mode, firstColIdx));

        FieldAccessExprGenerator[] accessors = new FieldAccessExprGenerator[columns.length()];
        try {
            for (int i = 0; i < columns.length(); i++) {
                final Field field = aClass.getDeclaredField(columns.column(i).name());

                accessors[i] = FieldAccessExprGenerator.createAccessor(
                    MarshallerUtil.mode(field.getType()),
                    firstColIdx + i /* schma absolute index. */,
                    IgniteUnsafeUtils.objectFieldOffset(field));
            }
        }
        catch (NoSuchFieldException ex) {
            throw new IllegalStateException(ex);
        }

        return new MarshallerExprGenerator(factoryRefExpr, accessors);
    }

    /**
     * Appends {@link Serializer#serialize(Object, Object)} method code.
     *
     * @param sb String buffer to append to.
     * @param keyMarsh Marshall expression generator for key.
     * @param valMarsh Marshall expression generator for value.
     */
    private void appendSerializeMethod(
        StringBuilder sb,
        MarshallerExprGenerator keyMarsh,
        MarshallerExprGenerator valMarsh
    ) {
        // Mehtod signature.
        sb.append(LF + "@Override public byte[] serialize(Object key, Object val) throws SerializationException {" + LF);
        sb.append(TAB + "TupleAssembler asm = createAssembler(key, val);" + LF);

        // Key marshal script.
        sb.append(TAB + "{" + LF);
        sb.append(TAB + TAB + "Object obj = key;" + LF);
        keyMarsh.appendMarshallObjectExpr(sb, TAB + TAB);
        sb.append(TAB + "}" + LF);

        // Value marshal script.
        sb.append(TAB + " {" + LF);
        sb.append(TAB + TAB + "Object obj = val;" + LF);
        valMarsh.appendMarshallObjectExpr(sb, TAB + TAB);
        sb.append(TAB + "}" + LF);

        // Return statement.
        sb.append(TAB + "return asm.build();" + LF);
        sb.append("}" + LF);
    }

    /**
     * Appends {@link Serializer#deserializeKey(byte[])} method code.
     *
     * @param sb String buffer to append to.
     * @param keyMarsh Unmarshall expression generator for key.
     */
    private void writeDeserializeKeyMethod(StringBuilder sb, MarshallerExprGenerator keyMarsh) {
        // Mehtod signature.
        sb.append(LF + "@Override public Object deserializeKey(byte[] data) throws SerializationException {" + LF);
        sb.append(TAB + "Tuple tuple = new ByteBufferTuple(schema, data);" + LF);

        // Key unmarshal script.
        keyMarsh.appendUnmarshallObjectExpr(sb, TAB);

        // Return statement.
        sb.append(TAB + "return obj;" + LF);
        sb.append("}" + LF);
    }

    /**
     * Appends {@link Serializer#deserializeValue(byte[])} method code.
     *
     * @param sb String buffer to append to.
     * @param valMarsh Unmarshall expression generator for value.
     */
    private void writeDeserializeValueMethod(StringBuilder sb, MarshallerExprGenerator valMarsh) {
        // Mehtod signature.
        sb.append(LF + "@Override public Object deserializeValue(byte[] data) throws SerializationException {" + LF);
        sb.append(TAB + "Tuple tuple = new ByteBufferTuple(schema, data);" + LF);

        // Key unmarshal script.
        valMarsh.appendUnmarshallObjectExpr(sb, TAB);

        // Return statement.
        sb.append(TAB + "return obj;" + LF);
        sb.append("}" + LF);
    }

    /**
     * Appends helper methods code.
     *
     * @param sb String buffer to append to.
     * @param schema Schema descriptor.
     * @param keyMarsh Marshall expression generator for key.
     * @param valMarsh Marshall expression generator for value.
     */
    private void generateTupleFactoryMethod(
        StringBuilder sb,
        SchemaDescriptor schema,
        MarshallerExprGenerator keyMarsh,
        MarshallerExprGenerator valMarsh
    ) {
        // Method signature.
        sb.append(LF + "TupleAssembler createAssembler(Object key, Object val) {" + LF);
        // Local variables.
        sb.append(TAB + "int nonNullVarlenKeys = 0; int nonNullVarlenValues = 0;" + LF);
        sb.append(TAB + "int nonNullVarlenKeysSize = 0; int nonNullVarlenValuesSize = 0;" + LF);
        sb.append(LF);
        sb.append(TAB + "Columns keyCols = schema.keyColumns();" + LF);
        sb.append(TAB + "Columns valCols = schema.valueColumns();" + LF);
        sb.append(LF);

        Columns keyCols = schema.keyColumns();
        if (keyCols.firstVarlengthColumn() >= 0) {
            // Appends key analyzer code-block.
            sb.append(TAB + "{" + LF);
            sb.append(TAB + TAB + "Object fVal, obj = key;" + LF); // Temporary vars.

            for (int i = keyCols.firstVarlengthColumn(); i < keyCols.length(); i++) {
                assert !keyCols.column(i).type().spec().fixedLength();

                sb.append(TAB + TAB + "assert !keyCols.column(").append(i).append(").type().spec().fixedLength();" + LF);
                sb.append(TAB + TAB + "fVal = ").append(keyMarsh.accessors[i].getFieldExpr()).append(";" + LF);
                sb.append(TAB + TAB + "if (fVal != null) {" + LF);
                sb.append(TAB + TAB + TAB + "nonNullVarlenKeysSize += MarshallerUtil.getValueSize(fVal, keyCols.column(").append(i).append(").type());").append(LF);
                sb.append(TAB + TAB + TAB + "nonNullVarlenKeys++;" + LF);
                sb.append(TAB + TAB + "}" + LF);
            }

            sb.append(TAB + "}" + LF);
        }

        Columns valCols = schema.valueColumns();
        if (valCols.firstVarlengthColumn() >= 0) {
            // Appends value analyzer code-block.
            sb.append(TAB + "{" + LF);
            sb.append(TAB + TAB + "Object fVal, obj = val;" + LF); // Temporary vars.

            for (int i = valCols.firstVarlengthColumn(); i < valCols.length(); i++) {
                assert !valCols.column(i).type().spec().fixedLength();

                sb.append(TAB + TAB + "assert !valCols.column(").append(i).append(").type().spec().fixedLength();" + LF);
                sb.append(TAB + TAB + "fVal = ").append(valMarsh.accessors[i].getFieldExpr()).append(";" + LF);
                sb.append(TAB + TAB + "if (fVal != null) {" + LF);
                sb.append(TAB + TAB + TAB + "nonNullVarlenValuesSize += MarshallerUtil.getValueSize(fVal, valCols.column(").append(i).append(").type());" + LF);
                sb.append(TAB + TAB + TAB + "nonNullVarlenValues++;" + LF);
                sb.append(TAB + TAB + "}" + LF);
            }
            sb.append(TAB + "}" + LF);
        }

        // Calculate tuple size.
        sb.append(LF);
        sb.append(TAB + "int size = TupleAssembler.tupleSize(" + LF);
        sb.append(TAB + TAB + "keyCols, nonNullVarlenKeys, nonNullVarlenKeysSize, " + LF);
        sb.append(TAB + TAB + "valCols, nonNullVarlenValues, nonNullVarlenValuesSize); " + LF);
        sb.append(LF);

        // Return statement.
        sb.append(TAB + "return new TupleAssembler(schema, size, nonNullVarlenKeys, nonNullVarlenValues);" + LF);
        sb.append("}" + LF);
    }
}
