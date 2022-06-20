# Ignite CLI DEVNOTES

## How to build module and add bash/zsh autocompletion to your shell

Build the ignite-3 and cli modules:
```bash
mvn clean install -DskipTests=true
```

Cd to the build directory:
```bash
cd modules/cli/target
```

Install autocompletion script to your shell:
```bash 
source target/ignite_completion.sh 
```

Add `ignite` alias:
```bash
alias ignite='./ignite'
```

For more info, see [Autocomplete for Java Command Line Applications](https://picocli.info/autocomplete.html).
