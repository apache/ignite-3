package org.apache.ignite.internal.configuration.processor.injectedname;

import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.InjectedName;
import org.apache.ignite.configuration.annotation.Name;
import org.apache.ignite.configuration.annotation.NamedConfigValue;

/**
 * Successful schema using annotations {@link InjectedName} and {@link Name}.
 */
@Config
public class NameConfigurationSchema {
    @Name("default")
    @ConfigValue
    public SimpleConfigurationSchema simple;

    @NamedConfigValue
    public SimpleConfigurationSchema simpleNamed;
}
