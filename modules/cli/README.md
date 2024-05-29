# Ignite 3 CLI

## Ignite 3 CLI architecture overview

### Non-interactive command frontend

Non-interactive (or non-REPL) command is a command that executes under `ignite` top command. Example of non-interactive command:

```bash
> ignite node status --url http://localhost:10300
```

The non-REPL fronted is responsible for:

* finding the command to execute
* parsing arguments and options
* calling the command

### Interactive command frontend

Interactive (or REPL, Read-Eval-Print-Loop) mode can be activated by `ignite` command. Example of interactive command:

```bash
> ignite
[diconnected]> node status --url http://localhost:10300
```

The REPL frontend is responsible for:

* show completions while a user types a command and options
* after the user presses enter, parse the command and its arguments and options
* call the command

### Common Execution Backend

After the command is found and all options are parsed, the command class (class annotated with `@Command`) builds a 
[`CallExecutionPipeline`](src/main/java/org/apache/ignite/internal/cli/core/call/CallExecutionPipeline.java) and runs it.

#### Call Execution Pipeline

This is where both REPL and non-REPL frontends meet and start to use common code. Call Execution Pipeline consists of:

* [`CallInput`](src/main/java/org/apache/ignite/internal/cli/core/call/CallInput.java). 
The object that will be passed to the [`Call`](src/main/java/org/apache/ignite/internal/cli/core/call/Call.java). 
Example [`UrlCallInput`](src/main/java/org/apache/ignite/internal/cli/core/call/UrlCallInput.java).
* [`Call`](src/main/java/org/apache/ignite/internal/cli/core/call/Call.java). Executable part of the command: call the REST, read the file, 
check the node status, etc. Example [`PhysicalTopologyCall`](src/main/java/org/apache/ignite/internal/cli/call/cluster/topology/PhysicalTopologyCall.java).
* Output writer.
* Error Output writer.
* Output decorator. [`Decorator`](src/main/java/org/apache/ignite/internal/cli/core/decorator/Decorator.java) that gets the output from 
[`Call`](src/main/java/org/apache/ignite/internal/cli/core/call/Call.java) and transforms it to human-readable output. 
Example [`TableDecorator`](src/main/java/org/apache/ignite/internal/cli/decorators/TableDecorator.java).
* [`ExceptionHandlers`](src/main/java/org/apache/ignite/internal/cli/core/exception/ExceptionHandlers.java). Registry of exception handlers 
that will handle all exceptions that might be thrown during the call. 
Example [`SqlExceptionHandler`](src/main/java/org/apache/ignite/internal/cli/core/exception/handler/SqlExceptionHandler.java).

### Flow

For the interactive mode there is a common situation when a user is not connected to any node and executes some command.
The user might forget to connect to the node and always type `--url` option instead of connecting to the node once and type only
commands.
So, it is useful to ask the user if he/she wants to connect to the node with the last `--url` value.

It might be implemented as several checks and read-line operations in every interactive command.
To avoid code duplication the [`Flow`](src/main/java/org/apache/ignite/internal/cli/core/flow/Flow.java) was introduced.
[`Flow`](src/main/java/org/apache/ignite/internal/cli/core/flow/Flow.java) is a DSL for building user interactions such as dialogs, 
questions, etc.
[`Flow`](src/main/java/org/apache/ignite/internal/cli/core/flow/Flow.java) can be easily integrated with 
[`Call`](src/main/java/org/apache/ignite/internal/cli/core/call/Call.java). A simplified example of
[`Flow`](src/main/java/org/apache/ignite/internal/cli/core/flow/Flow.java) that asks a user to connect to the last connected node on the
CLI start:

```java
String clusterUrl = stateConfigProvider.get().getProperty(ConfigConstants.LAST_CONNECTED_URL);
QuestionUiComponent question = QuestionUiComponent.fromQuestion(
        "Do you want to connect to the last connected node %s? %s ",UiElements.url(lastConnectedUrl),UiElements.yesNo()
);

Flows.acceptQuestion(question, ()->new ConnectCallInput(clusterUrl))
        .then(Flows.fromCall(connectCall))
        .print()
        .start(Flowable.empty());
```

An example of interactive and non-interactive commands that use the common backend:

`ignite node config show`
```java 
@Command(name = "show", description = "Shows node configuration")
public class NodeConfigShowCommand extends BaseCommand implements Callable<Integer> {
    /** Node URL option. */
    @Mixin
    private NodeUrlProfileMixin nodeUrl;

    /** Configuration selector option. */
    @Parameters(arity = "0..1", description = "Configuration path selector")
    private String selector;

    @Inject
    private NodeConfigShowCall call;

    /** {@inheritDoc} */
    @Override
    public Integer call() {
        return CallExecutionPipeline.builder(call)
                .inputProvider(this::buildCallInput)
                .output(spec.commandLine().getOut())
                .errOutput(spec.commandLine().getErr())
                .decorator(new JsonDecorator())
                .build()
                .runPipeline();
    }

    private NodeConfigShowCallInput buildCallInput() {
        return NodeConfigShowCallInput.builder()
                .nodeUrl(nodeUrl.getNodeUrl())
                .selector(selector)
                .build();
    }
}
```

`node config show`
```java
@Command(name = "show", description = "Shows node configuration")
public class NodeConfigShowReplCommand extends BaseCommand implements Runnable {
    /** Node URL option. */
    @Mixin
    private NodeUrlMixin nodeUrl;

    /** Configuration selector option. */
    @Parameters(arity = "0..1", description = "Configuration path selector")
    private String selector;

    @Inject
    private NodeConfigShowCall call;

    @Inject
    private ConnectToClusterQuestion question;

    /** {@inheritDoc} */
    @Override
    public void run() {
        question.askQuestionIfNotConnected(nodeUrl.getNodeUrl())
                .map(this::nodeConfigShowCallInput)
                .then(Flows.fromCall(call))
                .print()
                .start();
    }

    private NodeConfigShowCallInput nodeConfigShowCallInput(String nodeUrl) {
        return NodeConfigShowCallInput.builder().selector(selector).nodeUrl(nodeUrl).build();
    }
}
```

As you can see, both classes use the same `NodeConfigShowCall call` that performs the logic of fetching the node configuration.


### Completions

Completions in non-interactive mode are generated as shell scripts that have to be sourced into the user’s terminal. See
`generateAutocompletionScript` in [`build.gradle`](build.gradle).

But interactive mode is different. There is runtime information that can be used to provide more advanced completions 
([`DynamicCompleter`](src/main/java/org/apache/ignite/internal/cli/core/repl/completer/DynamicCompleter.java)).
For example, if a user is connected to the cluster and wants to see a part of cluster configuration using `--selector` option. 
The configuration key might be fetched from the cluster and passed as a completion candidate. 
See [`HoconDynamicCompleter`](src/main/java/org/apache/ignite/internal/cli/core/repl/completer/HoconDynamicCompleter.java).

## How to

### How to run the CLI

```bash
./gradlew clean cliDistZip -x test
cd packaging/build/distributions
unzip ignite3-cli-3.0.0-SNAPSHOT.zip
cd ignite3-cli-3.0.0-SNAPSHOT/
./bin/ignite3-cli 
```

### How to develop and test non-interactive command

You can take as an example `ignite node status` implementation – 
[`NodeStatusCommand`](src/main/java/org/apache/ignite/internal/cli/commands/node/status/NodeStatusCommand.java). 
As a REST call implementation – [`NodeStatusCall`](src/main/java/org/apache/ignite/internal/cli/call/node/status/NodeStatusCall.java). 
To enable the command in CLI, put it as a subcommand to 
[`TopLevelCliCommand`](src/main/java/org/apache/ignite/internal/cli/commands/TopLevelCliCommand.java).

[`ItClusterStatusCommandInitializedTest`](src/integrationTest/java/org/apache/ignite/internal/cli/commands/cluster/status/ItClusterStatusCommandInitializedTest.java) 
is an example of integration test for command class.

### How to develop and test interactive command

You can take as an example `node status` implementation – [`NodeStatusReplCommand`](src/main/java/org/apache/ignite/internal/cli/commands/node/status/NodeStatusReplCommand.java).
As a REST call implementation – [`NodeStatusCall`](src/main/java/org/apache/ignite/internal/cli/call/node/status/NodeStatusCall.java). 
To enable the command in CLI, put it as a subcommand to [`TopLevelCliReplCommand`](src/main/java/org/apache/ignite/internal/cli/commands/TopLevelCliReplCommand.java).

[`ItConnectToClusterTest`](src/integrationTest/java/org/apache/ignite/internal/cli/commands/questions/ItConnectToClusterTest.java) 
is an example of integration test for interactive command.

