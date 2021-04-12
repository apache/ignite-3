module ignite.rest {
    requires java.validation;
    requires ignite.configuration;
    requires ignite.internal.processor.configuration;
    requires com.google.gson;
    requires org.jetbrains.annotations;
    requires io.netty.codec.http;
    requires io.netty.codec;
    requires io.netty.transport;
    requires io.netty.handler;
    requires org.slf4j;
    requires io.netty.common;
    requires io.netty.buffer;
    exports org.apache.ignite.rest;
}