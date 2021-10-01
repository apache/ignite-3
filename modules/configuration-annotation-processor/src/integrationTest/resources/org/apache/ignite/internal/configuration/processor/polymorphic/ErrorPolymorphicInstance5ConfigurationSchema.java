package org.apache.ignite.internal.configuration.processor.polymorphic;

import org.apache.ignite.configuration.annotation.PolymorphicConfig;
import org.apache.ignite.configuration.annotation.PolymorphicConfigInstance;
import org.apache.ignite.configuration.annotation.Value;

/**
 * Class with {@link PolymorphicConfigInstance} should not contain duplicate fields
 * (by names) with {@link PolymorphicConfig}.
 */
@PolymorphicConfigInstance(id = "error")
public class ErrorPolymorphicInstance5ConfigurationSchema extends SimplePolymorphicConfigurationSchema {
    /** String value, duplicate {@link SimplePolymorphicConfigurationSchema#str}. */
    @Value
    public String str;
}
