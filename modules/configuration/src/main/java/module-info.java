module ignite.configuration {
    requires org.jetbrains.annotations;
    requires java.validation;
    requires ignite.core;
    exports org.apache.ignite.configuration.annotation;
    exports org.apache.ignite.configuration.tree;
    exports org.apache.ignite.configuration.internal;
    exports org.apache.ignite.configuration.internal.util;
    exports org.apache.ignite.configuration.internal.validation;
    exports org.apache.ignite.configuration.validation;
    exports org.apache.ignite.configuration.storage;
    exports org.apache.ignite.configuration.notifications;
    exports org.apache.ignite.configuration;
}