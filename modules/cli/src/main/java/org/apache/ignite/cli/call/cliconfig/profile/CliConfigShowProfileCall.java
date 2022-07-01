package org.apache.ignite.cli.call.cliconfig.profile;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.ignite.cli.config.ConfigManagerProvider;
import org.apache.ignite.cli.core.call.Call;
import org.apache.ignite.cli.core.call.CallOutput;
import org.apache.ignite.cli.core.call.DefaultCallOutput;
import org.apache.ignite.cli.core.call.EmptyCallInput;

/**
 * Get current profile call.
 */
@Singleton
public class CliConfigShowProfileCall implements Call<EmptyCallInput, String> {

    @Inject
    private ConfigManagerProvider provider;

    @Override
    public CallOutput<String> execute(EmptyCallInput input) {
        String profileName = provider.get().getCurrentProfile().getName();
        return DefaultCallOutput.success("Current profile: " + profileName);
    }
}
