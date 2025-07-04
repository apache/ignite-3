package org.apache.ignite.internal.client;

import java.util.Optional;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestInstanceFactory;
import org.junit.jupiter.api.extension.TestInstanceFactoryContext;
import org.junit.jupiter.api.extension.TestInstantiationException;

import static org.junit.platform.commons.util.ReflectionUtils.newInstance;

public class OldClientTestInstanceFactory implements TestInstanceFactory {
    @Override
    public Object createTestInstance(TestInstanceFactoryContext factoryContext, ExtensionContext extensionContext)
            throws TestInstantiationException {
        try {
            Optional<Object> outerInstance = factoryContext.getOuterInstance();
            Class<?> testClass = factoryContext.getTestClass();
            if (outerInstance.isPresent()) {
                System.out.println("createTestInstance() called for inner class: "
                        + testClass.getSimpleName());
                return newInstance(testClass, outerInstance.get());
            }
            else {
                System.out.println("createTestInstance() called for outer class: "
                        + testClass.getSimpleName());
                return newInstance(testClass);
            }
        }
        catch (Exception e) {
            throw new TestInstantiationException(e.getMessage(), e);
        }
    }
}
