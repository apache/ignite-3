# Ignite CLI DEVNOTES

## How to build module and add bash/zsh autocompletion to your shell

Cd to module directory and build the module:
```bash
cd modules/cli
mvn clean package
```

Install autocompletion script to your shell:
```bash 
source target/ignite_completion.sh 
```

Add `ignite` alias:
```bash
alias ignite='sh ignite.sh'
```

For more info, see [Autocomplete for Java Command Line Applications](https://picocli.info/autocomplete.html).