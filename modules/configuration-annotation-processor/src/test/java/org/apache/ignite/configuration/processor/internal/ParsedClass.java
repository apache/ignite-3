package org.apache.ignite.configuration.processor.internal;

import com.google.common.base.Functions;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import spoon.reflect.declaration.CtClass;
import spoon.reflect.declaration.CtConstructor;
import spoon.reflect.declaration.CtExecutable;
import spoon.reflect.declaration.CtMethod;
import spoon.reflect.declaration.CtParameter;
import spoon.reflect.reference.CtFieldReference;
import spoon.reflect.reference.CtReference;

public class ParsedClass {

    private final CtClass<?> cls;

    private final Map<String, CtFieldReference<?>> fields;

    private final Map<String, CtMethod<?>> methods;

    private final Map<String, CtConstructor<?>> constructors;

    public ParsedClass(CtClass<?> cls) {
        this.cls = cls;

        this.fields = cls.getAllFields()
            .stream()
            .collect(Collectors.toMap(CtReference::getSimpleName, Functions.identity()));

        this.methods = cls.getMethods()
            .stream()
            .collect(Collectors.toMap(this::getMethodName, Functions.identity()));

        this.constructors = cls.getConstructors()
            .stream()
            .collect(Collectors.toMap(this::getMethodName, Functions.identity()));
    }

    private String getMethodName(CtExecutable<?> method) {
        final List<CtParameter<?>> parameters = method.getParameters();

        final String params = parameters.stream().map(parameter -> parameter.getType().getQualifiedName()).collect(Collectors.joining(", "));

        return method.getSimpleName() + "(" + params + ")";
    }

    public Map<String, CtFieldReference<?>> getFields() {
        return fields;
    }

    public Map<String, CtMethod<?>> getMethods() {
        return methods;
    }

    public Map<String, CtConstructor<?>> getConstructors() {
        return constructors;
    }
}
