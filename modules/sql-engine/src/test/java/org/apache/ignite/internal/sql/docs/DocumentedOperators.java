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

package org.apache.ignite.internal.sql.docs;

import static org.junit.jupiter.api.Assertions.fail;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;

/** Collection of operators to validate SQL operator signatures. */
class DocumentedOperators {

    private static final Pattern TYPE_NAME_PATTERN = Pattern.compile("<[^>]*>");

    private final String name;

    private final List<DocumentedOperator> operators = new ArrayList<>();

    private boolean include = true;

    /** Creates a collection of operators. */
    DocumentedOperators(String name) {
        this.name = name;
    }

    /** Adds a public operator. */
    DocumentedOperator add(SqlOperator operator) {
        return addOp(operator, operator.getName(), false);
    }

    /** Adds a public operator under the specified name. */
    DocumentedOperator add(SqlOperator operator, String syntaxName) {
        return addOp(operator, syntaxName, false);
    }

    /** Adds an internal operator. */
    DocumentedOperator internal(SqlOperator operator) {
        return addOp(operator, operator.getName(), true);
    }

    /** Adds an internal operator under the specified name. */
    DocumentedOperator internal(SqlOperator operator, String syntaxName) {
        return addOp(operator, syntaxName, true);
    }

    /** Marks this collection of operators as excluded from a comparison. */
    DocumentedOperators exclude() {
        include = false;
        return this;
    }

    private DocumentedOperator addOp(SqlOperator operator, String syntaxName, boolean internal) {
        DocumentedOperator op = new DocumentedOperator(operator, syntaxName, internal);
        operators.add(op);
        return op;
    }

    static void printOperators(PrintWriter pw, List<DocumentedOperators> operators) {
        operators.stream().filter(ops -> ops.include).forEach(ops -> {
            pw.println("=== " + ops.name);
            pw.println();
            ops.operators.forEach(op -> printSignature(pw, op));
            pw.println();
        });
        pw.flush();
    }

    /** Validates the given operator table against a list of operators. */
    static void validateOperatorList(
            SqlOperatorTable operatorTable,
            List<DocumentedOperators> operators,
            String resourceName
    ) {
        Set<SqlOperator> annotated = operators.stream()
                .flatMap(ops -> ops.operators.stream().map(f -> f.operator)).collect(Collectors.toSet());

        List<SqlOperator> notDocumented = new ArrayList<>();

        for (SqlOperator operator : operatorTable.getOperatorList()) {
            if (!annotated.contains(operator)) {
                notDocumented.add(operator);
            }
        }

        if (!notDocumented.isEmpty()) {
            StringBuilder sb = new StringBuilder();

            sb.append("Signatures of the following operators are missing from ")
                    .append(resourceName)
                    .append(". Add these operators to as public (add(OperatorTable.MY_OP))")
                    .append(" or include them as internal (call .hide(OperatorTable.MY_OP):")
                    .append(System.lineSeparator())
                    .append("Review the difference and file an issue to update SQL documentation if necessary.")
                    .append(System.lineSeparator());

            notDocumented.forEach(o -> sb.append(describeOperator(o)).append(System.lineSeparator()));

            fail(sb.toString());
        }
    }

    private static String describeOperator(SqlOperator operator) {
        return operator.getName() + " class: " + operator.getClass().getCanonicalName();
    }

    /** Operator. */
    static class DocumentedOperator {
        final SqlOperator operator;

        final String syntaxName;

        final boolean internal;

        final List<Signature> sigs = new ArrayList<>();

        DocumentedOperator(SqlOperator operator, String displayName, boolean internal) {
            this.operator = operator;
            this.syntaxName = displayName;
            this.internal = internal;
        }

        /** Adds a signature. */
        DocumentedOperator sig(String sigStr) {
            Signature sig = new Signature(syntaxName, sigStr);
            sigs.add(sig);
            return this;
        }
    }

    static void printSignature(PrintWriter pw, DocumentedOperator op) {
        Signatures sigs = Signatures.makeSignatures(op);

        if (op.internal) {
            pw.println("[internal]");
        }

        for (String sig : sigs.sigs) {
            pw.print(sig);
            if (sigs.manual) {
                pw.print(" ***");
            }
            pw.println();
        }
        pw.println("#" + sigs.hash);
        pw.println();
    }

    static class Signatures {

        final List<String> sigs;

        final String hash;

        final boolean manual;

        Signatures(List<String> sigs, String hash, boolean manual) {
            this.sigs = sigs;
            this.hash = hash;
            this.manual = manual;
        }

        static Signatures makeSignatures(DocumentedOperator op) {
            String opHash = computeOperatorHash(op);

            if (!op.sigs.isEmpty()) {
                List<String> sigs = op.sigs.stream()
                        .map(s -> s.fullSig)
                        .collect(Collectors.toList());

                return new Signatures(sigs, opHash, true);
            }

            try {
                String allowedSignatures = op.operator.getAllowedSignatures();
                String[] signatures;

                String opName = op.operator.getName();

                // Coverts "'<t1> op <t2> <t3>'" into <t1> <t2> <t3>
                // Specifically handles <t1> > <t2> / <t1> < <t2>
                if ("<".equals(opName) || ">".equals(opName)) {
                    signatures = allowedSignatures.replace("'", "")
                            .replace(" " + opName + " ", " ")
                            .split("\\n");
                } else {
                    signatures = allowedSignatures.replace("'", "")
                            .replace(opName, "")
                            .split("\\n");
                }

                List<String> signaturesList = SignatureFormat.formatAll(SignatureFormat.ARGS, op, signatures);
                List<String> sigs;

                if (signaturesList.isEmpty()) {
                    sigs = List.of(op.syntaxName);
                } else if (signaturesList.size() == 1) {
                    sigs = List.of(SignatureFormat.FULL.format(op, signatures[0]));
                } else {
                    sigs = new ArrayList<>(SignatureFormat.formatAll(SignatureFormat.FULL, op, signatures));
                }

                return new Signatures(sigs, opHash, false);
            } catch (Throwable error) {
                String syntaxName = op.syntaxName + " <UNABLE TO PARSE SIGNATURE: " + error.getMessage() + ">";

                return new Signatures(List.of(syntaxName), opHash, false);
            }
        }
    }

    private static class Signature {

        final String fullSig;

        final String argSig;

        Signature(String opName, String sigStr) {
            Matcher matcher = TYPE_NAME_PATTERN.matcher(sigStr);
            List<String> params = new ArrayList<>();

            while (matcher.find()) {
                String type = matcher.group();
                params.add(type);
            }

            this.fullSig = opName + sigStr;
            this.argSig = params.stream().collect(Collectors.joining(", ", "(", ")"));
        }
    }

    enum SignatureFormat {
        /** Operator name ands arguments. Example: for {@code f(a, b)} full format produces {@code f(a, b)}. */
        FULL,
        /** Only arguments. Example: for {@code f(a, b}} this format produces {@code (a, b)}. */
        ARGS;

        static List<String> formatAll(SignatureFormat format, DocumentedOperator op, String[] sigs) {
            return Arrays.stream(sigs)
                    .map(DocumentedOperators::formatTypeNames)
                    .map(s -> format.format(op, s))
                    .distinct()
                    .collect(Collectors.toList());
        }

        String format(DocumentedOperator op, String sig) {
            switch (this) {
                case FULL:
                    return full(op, sig);
                case ARGS:
                    return args(op, sig);
                default:
                    throw new IllegalArgumentException("Unknown format: " + this);
            }
        }

        private static String full(DocumentedOperator op, String in) {
            String s = formatTypeNames(in);
            SqlSyntax syntax = op.operator.getSyntax();

            if (syntax == SqlSyntax.PREFIX) {
                return op.syntaxName + " " + s;
            } else if (syntax == SqlSyntax.POSTFIX) {
                return s + op.syntaxName;
            } else if (syntax == SqlSyntax.BINARY) {
                String[] args = s.split("\\s+");
                return args[0] + " " + op.syntaxName + " " + args[1];
            } else if (syntax == SqlSyntax.FUNCTION_ID) {
                return op.syntaxName;
            } else {
                return op.syntaxName + s;
            }
        }

        private static String args(DocumentedOperator op, String in) {
            String s = formatTypeNames(in);
            SqlSyntax syntax = op.operator.getSyntax();

            if (syntax == SqlSyntax.BINARY) {
                String[] args = s.split("\\s+");
                return args[0] + " " + args[1];
            } else if (syntax == SqlSyntax.FUNCTION_ID) {
                return "";
            } else {
                return s;
            }
        }
    }

    /** Converts {@code FUNC (<TYPE1> AND <TYPE2>)} into {@code FUNC (<type1> AND <type2>)}}. */
    static String formatTypeNames(String input) {
        Matcher matcher = TYPE_NAME_PATTERN.matcher(input);

        StringBuilder result = new StringBuilder();

        while (matcher.find()) {
            String matchedSubstring = matcher.group();
            String replacement = matchedSubstring.toLowerCase();

            matcher.appendReplacement(result, replacement);
        }

        matcher.appendTail(result);

        return result.toString();
    }

    /** Computes hash of SQL operator. */
    private static String computeOperatorHash(DocumentedOperator op) {
        SqlOperator sqlOp = op.operator;

        StringBuilder sb = new StringBuilder();

        sb.append("name=").append(op.syntaxName)
                .append("opName=").append(sqlOp.getName())
                .append("syntax=").append(sqlOp.getSyntax())
                .append("kind=").append(sqlOp.kind)
                .append("signatures=");

        try {
            String allowedSigs = sqlOp.getAllowedSignatures();
            sb.append(allowedSigs);
        } catch (Throwable t) {
            // Assume that an operator signature changes, if an error message changes.
            sb.append("<ERROR: ").append(t.getMessage()).append('>');
        }

        // We can not use neither operandTypeChecker nor operandTypeInference because those properties
        // can be provided as lambdas that does not have stable textual representation.

        sb.append("operandCountRange=");
        // Assume that changes to operandCountRange are reflected in its properties.
        try {
            SqlOperandCountRange range = sqlOp.getOperandCountRange();
            sb.append(range.getMin()).append('-').append(range.getMax());
        } catch (UnsupportedOperationException ignore) {
            // Change from no operandCountRange to some operandCountRange should mean signatures might have been modified.
            sb.append("not_implemented");
        }

        // Include negated operator
        SqlOperator negated = sqlOp.not();
        if (negated != null) {
            sb.append("negated=").append(negated.getName());
        }

        // Include reverse operator
        SqlOperator reverse = sqlOp.reverse();
        if (reverse != null) {
            sb.append("reverse=").append(reverse.getName());
        }

        // Other properties

        sb.append("leftPrecedence=").append(sqlOp.getLeftPrec())
                .append("rightPrecedence=").append(sqlOp.getRightPrec())
                .append("symmetrical=").append(sqlOp.isSymmetrical())
                .append("deterministic=").append(sqlOp.isDeterministic())
                .append("aggregator=").append(sqlOp.isAggregator())
                .append("group=").append(sqlOp.isGroup())
                .append("groupAuxiliary=").append(sqlOp.isGroupAuxiliary())
                .append("allowsFraming=").append(sqlOp.allowsFraming())
                .append("requiresOrder=").append(sqlOp.requiresOrder())
                .append("requiresOver=").append(sqlOp.requiresOver());

        return SqlFunctions.sha1(sb.toString());
    }
}
