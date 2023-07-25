package org.apache.ignite.internal.cli.commands.connect;

import static org.apache.ignite.internal.cli.commands.Options.Constants.PASSWORD_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.PASSWORD_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.PASSWORD_OPTION_SHORT;
import static org.apache.ignite.internal.cli.commands.Options.Constants.USERNAME_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.USERNAME_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.USERNAME_OPTION_SHORT;

import org.jetbrains.annotations.Nullable;
import picocli.CommandLine.Option;

/**
 * Mixin class for connect command options.
 */
public class ConnectOptions {

    @Option(names = {USERNAME_OPTION,
            USERNAME_OPTION_SHORT}, description = USERNAME_OPTION_DESC, required = true, defaultValue = Option.NULL_VALUE)
    private String username;

    @Option(names = {PASSWORD_OPTION,
            PASSWORD_OPTION_SHORT}, description = PASSWORD_OPTION_DESC, required = true, defaultValue = Option.NULL_VALUE)
    private String password;

    @Nullable
    public String username() {
        return username;
    }

    @Nullable
    public String password() {
        return password;
    }
}
