package org.apache.ignite.internal.schema.marshaller.generator;

import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.TypeSpec;

/**
 * Marshaller code generator.
 */
interface MarshallerCodeGenerator {
    /**
     * @return {@code true} if it is simple object marshaller, {@code false} otherwise.
     */
    boolean isSimpleType();

    /**
     * @param tupleExpr Tuple to read from.
     * @return Unmarshall object code.
     */
    CodeBlock unmarshallObjectCode(String tupleExpr);

    /**
     * @param asm Tuple assembler to write to.
     * @param objVar Object to serialize.
     * @return Marshall object code.
     */
    CodeBlock marshallObjectCode(String asm, String objVar);

    /**
     * @param objVar Object var.
     * @param colIdx Column index.
     * @return Object field value for given column.
     */
    CodeBlock getValueCode(String objVar, int colIdx);

    /**
     * @param classBuilder Class builder.
     * @param staticInitBuilder Static initializer builder.
     */
    void initStaticHandlers(TypeSpec.Builder classBuilder, CodeBlock.Builder staticInitBuilder);

    /**
     * @return Marshaller target class.
     */
    default Class<?> getClazz() {
        return null;
    }
}
