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

package org.apache.ignite.internal.error.code.processor;

import static java.util.stream.Collectors.toSet;

import com.google.auto.service.AutoService;
import com.sun.source.tree.ExpressionTree;
import com.sun.source.tree.IdentifierTree;
import com.sun.source.tree.LiteralTree;
import com.sun.source.tree.MethodInvocationTree;
import com.sun.source.tree.ParenthesizedTree;
import com.sun.source.tree.TypeCastTree;
import com.sun.source.tree.VariableTree;
import com.sun.source.util.TreePathScanner;
import com.sun.source.util.Trees;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.TypeElement;
import org.apache.ignite.error.code.annotations.ErrorCodeGroup;
import org.apache.ignite.internal.error.code.generators.AbstractCodeGenerator;
import org.apache.ignite.internal.error.code.generators.CppGenerator;
import org.apache.ignite.internal.error.code.generators.CsharpGenerator;
import org.apache.ignite.internal.error.code.processor.ErrorCodeGroupDescriptor.DeprecatedAlias;
import org.apache.ignite.internal.error.code.processor.ErrorCodeGroupDescriptor.ErrorCode;

/**
 * Annotation processor that process @{@link ErrorCodeGroup} annotation.
 *
 * <p>
 * Collects all error groups and generates C++ and C# files with corresponding error codes groups.
 * </p>
 */
@AutoService(Processor.class)
public class ErrorCodeGroupProcessor extends AbstractProcessor {
    private Trees trees;

    @Override
    public void init(ProcessingEnvironment pe) {
        super.init(pe);
        this.trees = Trees.instance(pe);
    }

    /** {@inheritDoc} */
    @Override
    public Set<String> getSupportedAnnotationTypes() {
        return Set.of(ErrorCodeGroup.class.getName());
    }

    /** {@inheritDoc} */
    @Override
    public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.latest();
    }

    /** {@inheritDoc} */
    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        Set<TypeElement> errorGroups = annotations.stream()
                .map(roundEnv::getElementsAnnotatedWith)
                .flatMap(Collection::stream)
                .map(TypeElement.class::cast)
                .collect(toSet());

        if (errorGroups.isEmpty()) {
            return true;
        }

        List<ErrorCodeGroupDescriptor> descriptors = new ArrayList<>();

        for (TypeElement clazz : errorGroups) {
            ErrorCodeTreeScanner codeScanner = new ErrorCodeTreeScanner();
            codeScanner.scan(this.trees.getPath(clazz), this.trees);
            var descriptor = codeScanner.getDescriptor();
            descriptor.className = clazz.getSimpleName().toString();
            descriptors.add(descriptor);
        }

        generate(descriptors);

        return true;
    }

    private void generate(List<ErrorCodeGroupDescriptor> groups) {
        groups.sort(Comparator.comparing(g -> g.groupCode));
        for (var group : groups) {
            group.errorCodes.sort(Comparator.comparing(d -> d.code));
        }

        List<AbstractCodeGenerator> generators = List.of(
                new CppGenerator(processingEnv, "cpp/ignite/common/error_codes.h"),
                new CsharpGenerator(processingEnv, "dotnet/Apache.Ignite/ErrorCodes.g.cs")
        );

        for (var generator : generators) {
            generator.generate(groups);
        }
    }

    static class ErrorCodeTreeScanner extends TreePathScanner<Object, Trees> {
        ErrorCodeTreeScanner() {
            this.descriptor = new ErrorCodeGroupDescriptor();
        }

        private final ErrorCodeGroupDescriptor descriptor;

        private ErrorCodeGroupProcessorException ex;

        ErrorCodeGroupDescriptor getDescriptor() {
            if (ex != null) {
                throw ex;
            }

            return descriptor;
        }

        private Object visitErrorCodeField(VariableTree variableTree, Trees trees) {
            // example: initializer = COMMON_ERR_GROUP.registerErrorCode((short) 1).
            var initializer = variableTree.getInitializer();
            var name = variableTree.getName().toString();
            try {
                if (MethodInvocationTree.class.isAssignableFrom(initializer.getClass())) {
                    // example: args = {"(short) 1"} as List<ExpressionTree>.
                    var args = ((MethodInvocationTree) initializer).getArguments();
                    // example: expr = "(short) 1" as TypeCastTree.
                    var expr = ((TypeCastTree) args.get(0)).getExpression();
                    // example: if expr is "(short) (1)" we should remove parentheses
                    if (expr instanceof ParenthesizedTree) {
                        expr = ((ParenthesizedTree) expr).getExpression();
                    }
                    // example: extract 1 from "(short) 1" expression.
                    this.descriptor.errorCodes.add(new ErrorCode((Integer) ((LiteralTree) expr).getValue(), name));
                } else if (IdentifierTree.class.isAssignableFrom(initializer.getClass())) {
                    boolean hasDeprecated = variableTree.getModifiers().getAnnotations().stream()
                            .anyMatch(annotation -> "Deprecated".contentEquals(annotation.getAnnotationType().toString()));
                    if (!hasDeprecated) {
                        ex = new ErrorCodeGroupProcessorException(String.format("Alias %s must be marked as @Deprecated", name));
                    } else {
                        var identifier = ((IdentifierTree) initializer).getName().toString();

                        descriptor.deprecatedAliases.add(new DeprecatedAlias(name, identifier));
                    }
                } else {
                    ex = new ErrorCodeGroupProcessorException(
                            String.format("AST parsing error: Expected MethodInvocationTree or IdentifierTree in initializer, but got %s",
                                    initializer.getClass().getSimpleName()));
                }
            } catch (Exception e) {
                ex = new ErrorCodeGroupProcessorException("AST parsing error", e);
            }

            return super.visitVariable(variableTree, trees);
        }

        private Object visitErrorCodeGroupField(VariableTree variableTree, Trees trees) {
            var initializer = variableTree.getInitializer();
            try {
                var args = ((MethodInvocationTree) initializer).getArguments();

                String errorPrefix;
                LiteralTree groupNameExpr;
                ExpressionTree groupCodeExpr;

                switch (args.size()) {
                    case 2:
                        errorPrefix = "IGN";
                        groupNameExpr = ((LiteralTree) args.get(0));
                        groupCodeExpr = ((TypeCastTree) args.get(1)).getExpression();
                        break;
                    case 3:
                        errorPrefix = ((LiteralTree) args.get(0)).getValue().toString();
                        groupNameExpr = ((LiteralTree) args.get(1));
                        groupCodeExpr = ((TypeCastTree) args.get(2)).getExpression();
                        break;
                    default:
                        throw new IllegalArgumentException("Unexpected arguments count " + args.size());
                }

                if (groupCodeExpr instanceof ParenthesizedTree) {
                    groupCodeExpr = ((ParenthesizedTree) groupCodeExpr).getExpression();
                }
                this.descriptor.errorPrefix = errorPrefix;
                this.descriptor.groupName = (String) groupNameExpr.getValue();
                this.descriptor.groupCode = (Integer) ((LiteralTree) groupCodeExpr).getValue();
            } catch (Exception e) {
                ex = new ErrorCodeGroupProcessorException("AST parsing error", e);
            }

            return super.visitVariable(variableTree, trees);
        }

        /** {@inheritDoc} */
        @Override
        public Object visitVariable(VariableTree variableTree, Trees trees) {
            if (variableTree.getType().toString().equals("int")) {
                return visitErrorCodeField(variableTree, trees);
            }

            if (variableTree.getType().toString().equals("ErrorGroup")) {
                return visitErrorCodeGroupField(variableTree, trees);
            }

            return super.visitVariable(variableTree, trees);
        }
    }
}
