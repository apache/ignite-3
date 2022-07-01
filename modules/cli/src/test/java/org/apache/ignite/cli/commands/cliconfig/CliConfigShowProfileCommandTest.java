package org.apache.ignite.cli.commands.cliconfig;

import static org.junit.jupiter.api.Assertions.assertAll;

import jakarta.inject.Inject;
import org.apache.ignite.cli.commands.CliCommandTestBase;
import org.apache.ignite.cli.commands.cliconfig.profile.CliConfigShowProfileCommand;
import org.apache.ignite.cli.config.ini.IniConfigManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CliConfigShowProfileCommandTest extends CliCommandTestBase {

    @Inject
    TestConfigManagerProvider configManagerProvider;

    @Override
    protected Class<?> getCommandClass() {
        return CliConfigShowProfileCommand.class;
    }

    @BeforeEach
    void configManagerRefresh() {
        configManagerProvider.configManager = new IniConfigManager(TestConfigManagerHelper.createSectionWithInternalPart());
    }

    @Test
    public void testWithInternalSection() {
        execute();

        assertAll(
                () -> assertOutputContains("Current profile: database"),
                this::assertErrOutputIsEmpty
        );
    }

    @Test
    public void testWithoutInternalSection() {
        configManagerProvider.configManager = new IniConfigManager(TestConfigManagerHelper.createSectionWithoutInternalPart());
        execute();

        assertAll(
                () -> assertOutputContains("Current profile: owner"),
                this::assertErrOutputIsEmpty
        );
    }
}
