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

package org.apache.ignite.internal.checkstyle;

import com.puppycrawl.tools.checkstyle.api.AbstractCheck;
import com.puppycrawl.tools.checkstyle.api.DetailAST;
import com.puppycrawl.tools.checkstyle.api.TokenTypes;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Checks that logger fields are initialized with the enclosing class, not a copy-pasted different class.
 *
 * <p>Detects patterns like:
 * <pre>
 * class Bar {
 *     // BUG: should be Bar.class, not Foo.class
 *     private static final Logger LOG = LoggerFactory.getLogger(Foo.class);
 * }
 * </pre>
 */
public class LoggerClassMismatchCheck extends AbstractCheck {

    /** Key for the violation message. */
    public static final String MSG_KEY = "logger.class.mismatch";

    /** Default field name pattern. */
    private static final String DEFAULT_FIELD_NAME_PATTERN = "LOG|log|LOGGER|logger";

    /** Default factory method names. */
    private static final String DEFAULT_FACTORY_METHODS = "getLogger,forClass";

    /** Compiled field name pattern. */
    private Pattern fieldNamePattern = Pattern.compile(DEFAULT_FIELD_NAME_PATTERN);

    /** Set of factory method names. */
    private Set<String> factoryMethods = new HashSet<>(Arrays.asList(DEFAULT_FACTORY_METHODS.split(",")));

    /**
     * Sets the field name pattern.
     *
     * @param pattern regex pattern for matching logger field names.
     */
    public void setFieldNamePattern(String pattern) {
        this.fieldNamePattern = Pattern.compile(pattern);
    }

    /**
     * Sets the factory method names.
     *
     * @param methods comma-separated list of factory method names.
     */
    public void setFactoryMethods(String methods) {
        this.factoryMethods = new HashSet<>(Arrays.asList(methods.split(",")));
    }

    @Override
    public int[] getDefaultTokens() {
        return getRequiredTokens();
    }

    @Override
    public int[] getAcceptableTokens() {
        return getRequiredTokens();
    }

    @Override
    public int[] getRequiredTokens() {
        return new int[]{TokenTypes.VARIABLE_DEF};
    }

    @Override
    public void visitToken(DetailAST ast) {
        if (!isStaticFinal(ast)) {
            return;
        }

        String fieldName = getFieldName(ast);
        if (fieldName == null || !fieldNamePattern.matcher(fieldName).matches()) {
            return;
        }

        DetailAST methodCall = findLoggerFactoryCall(ast);
        if (methodCall == null) {
            return;
        }

        String argClassName = extractClassArgument(methodCall);
        if (argClassName == null) {
            return;
        }

        String enclosingClassName = findEnclosingClassName(ast);
        if (enclosingClassName == null) {
            return;
        }

        if (!argClassName.equals(enclosingClassName)) {
            log(ast, MSG_KEY, argClassName, enclosingClassName);
        }
    }

    /**
     * Checks if the variable definition has both 'static' and 'final' modifiers.
     */
    private static boolean isStaticFinal(DetailAST variableDef) {
        DetailAST modifiers = variableDef.findFirstToken(TokenTypes.MODIFIERS);
        if (modifiers == null) {
            return false;
        }

        boolean isStatic = false;
        boolean isFinal = false;

        for (DetailAST child = modifiers.getFirstChild(); child != null; child = child.getNextSibling()) {
            if (child.getType() == TokenTypes.LITERAL_STATIC) {
                isStatic = true;
            } else if (child.getType() == TokenTypes.FINAL) {
                isFinal = true;
            }
        }

        return isStatic && isFinal;
    }

    /**
     * Gets the field name from a variable definition.
     */
    private static String getFieldName(DetailAST variableDef) {
        DetailAST ident = variableDef.findFirstToken(TokenTypes.IDENT);
        return ident != null ? ident.getText() : null;
    }

    /**
     * Finds a logger factory method call in the variable assignment.
     * Looks for patterns like {@code LoggerFactory.getLogger(...)}.
     */
    private DetailAST findLoggerFactoryCall(DetailAST variableDef) {
        DetailAST assign = variableDef.findFirstToken(TokenTypes.ASSIGN);
        if (assign == null) {
            return null;
        }

        return findMethodCallInSubtree(assign);
    }

    /**
     * Recursively searches for a matching factory method call in the AST subtree.
     */
    private DetailAST findMethodCallInSubtree(DetailAST node) {
        if (node == null) {
            return null;
        }

        if (node.getType() == TokenTypes.METHOD_CALL) {
            String methodName = getMethodCallName(node);
            if (methodName != null && factoryMethods.contains(methodName)) {
                return node;
            }
        }

        for (DetailAST child = node.getFirstChild(); child != null; child = child.getNextSibling()) {
            DetailAST result = findMethodCallInSubtree(child);
            if (result != null) {
                return result;
            }
        }

        return null;
    }

    /**
     * Gets the method name from a METHOD_CALL node.
     * Handles both simple calls {@code getLogger(...)} and dotted calls {@code LoggerFactory.getLogger(...)}.
     */
    private static String getMethodCallName(DetailAST methodCall) {
        DetailAST firstChild = methodCall.getFirstChild();
        if (firstChild == null) {
            return null;
        }

        // Simple call: getLogger(...)
        if (firstChild.getType() == TokenTypes.IDENT) {
            return firstChild.getText();
        }

        // Dotted call: LoggerFactory.getLogger(...)
        if (firstChild.getType() == TokenTypes.DOT) {
            DetailAST methodIdent = firstChild.getLastChild();
            if (methodIdent != null && methodIdent.getType() == TokenTypes.IDENT) {
                return methodIdent.getText();
            }
        }

        return null;
    }

    /**
     * Extracts the class name from {@code X.class} argument in the method call.
     */
    private static String extractClassArgument(DetailAST methodCall) {
        DetailAST elist = methodCall.findFirstToken(TokenTypes.ELIST);
        if (elist == null) {
            return null;
        }

        DetailAST expr = elist.findFirstToken(TokenTypes.EXPR);
        if (expr == null) {
            return null;
        }

        // Look for DOT node representing X.class
        DetailAST dot = expr.findFirstToken(TokenTypes.DOT);
        if (dot == null) {
            return null;
        }

        DetailAST lastChild = dot.getLastChild();
        if (lastChild == null || lastChild.getType() != TokenTypes.LITERAL_CLASS) {
            return null;
        }

        // The class name is the first child of the DOT: could be IDENT (simple) or another DOT (qualified)
        DetailAST classNameNode = dot.getFirstChild();
        if (classNameNode == null) {
            return null;
        }

        // For simple names like Foo.class, the first child is IDENT
        if (classNameNode.getType() == TokenTypes.IDENT) {
            return classNameNode.getText();
        }

        return null;
    }

    /**
     * Finds the name of the nearest enclosing class definition.
     */
    private static String findEnclosingClassName(DetailAST node) {
        DetailAST parent = node.getParent();
        while (parent != null) {
            if (parent.getType() == TokenTypes.CLASS_DEF
                    || parent.getType() == TokenTypes.INTERFACE_DEF
                    || parent.getType() == TokenTypes.ENUM_DEF) {
                DetailAST ident = parent.findFirstToken(TokenTypes.IDENT);
                if (ident != null) {
                    return ident.getText();
                }
            }
            parent = parent.getParent();
        }
        return null;
    }
}
