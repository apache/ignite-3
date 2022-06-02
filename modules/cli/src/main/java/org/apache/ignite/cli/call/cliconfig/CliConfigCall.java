package org.apache.ignite.cli.call.cliconfig;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.ignite.cli.config.Config;
import org.apache.ignite.cli.core.call.Call;
import org.apache.ignite.cli.core.call.DefaultCallOutput;
import org.apache.ignite.cli.core.call.EmptyCallInput;

/**
 * Gets entire CLI configuration.
 */
@Singleton
public class CliConfigCall implements Call<EmptyCallInput, Config> {
    @Inject
    private Config config;

    @Override
    public DefaultCallOutput<Config> execute(EmptyCallInput input) {
        return DefaultCallOutput.success(config);
    }
}
