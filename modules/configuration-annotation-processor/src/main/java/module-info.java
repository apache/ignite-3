module ignite.internal.processor.configuration {
    requires java.compiler;
    requires com.squareup.javapoet;
    requires ignite.configuration;
    exports org.apache.ignite.internal.processor.configuration;
    provides javax.annotation.processing.Processor with org.apache.ignite.internal.processor.configuration.Processor;
}