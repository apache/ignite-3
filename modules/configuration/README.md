# Configuration
This modules provides API classes and implementation for Ignite configuration framework. The idea is to have so called
_Unified configuration_ - a common way of configuring both local Ignite node and Ignite clusters. Original concept can
be seen in [IEP-55](https://cwiki.apache.org/confluence/display/IGNITE/IEP-55+Unified+Configuration).

The core concept behind it is the _Configuration Schema_. It's required to generate public interfaces for the API and
internal implementations that end user won't face. This way developers avoids writing boilerplate code.

There are already meny examples throughout the code that looks somewhat like this:
```
@ConfigurationRoot(rootName = "root", type = ConfigurationType.LOCAL)
public static class ParentConfigurationSchema {
    @NamedConfigValue
    private NamedElementConfigurationSchema elements;

    @ConfigValue
    private ChildConfigurationSchema child;
}

@Config
public static class ChildConfigurationSchema {
    @Value
    public String str;
}
...
```

Main parts of what's present in this snippet are:
* `@ConfigurationRoot` marks root schemas. In general, configuration consists of several things:
  * property `type` shows that configuration can be either `LOCAL` or `DISTRIBUTED`. Main difference is that it'll be
    stored in different _storages_ - `Vault` or `Metastorage`. But this information isn't known to configuration module,
    only `ConfigurationStorage` interface is used. Other modules and tests just implement it and assign to corresponding
    type constant.
  * `rootName` gives this configuration a unique name. Basically all Ignite configuration is represented by a forest.
    Every node in that forest has a name, usually referred to as a _key_. `rootName` assigns a _key_ to the root node of
    the tree that will represent this particular configuration schema.
* `@Config` has the same internal structure as `@ConfigurationRoot` but represents inner configuration node, not a root.
  Things that it can have:
  * `@ConfigValue` fields must be other schemas. Cyclic dependencies are not allowed.
  * `@NamedConfigValue` fields must be other schemas as well. The main difference is that such fields will basically
    become `Map<String, declared_schema>`.
  * `@Value` fields must be of following types:
    * `boolean` or `boolean[]`
    * `int` or `int[]`
    * `long` or `long[]`
    * `double` or `double[]`
    * `String` or `String[]`

All _leaves_ **cannot be nulls**, that's a strict restriction for now. `@Value` fields might have following unique
options:
* `@Value(hasDefault = true)` - in this case schema field must be initialized in schema constructor. This initialized
  value will be used as default if no value was provided by the user.
* `@Immutable` - these properties cannot be changed once they are initialized either manually or with default value.

Another important concept is a `RootKey`. It represents type-safe object that holds name of the root node. Instances of
this interface are generated automatically and are mandatory for registering roots in the framework.

`ConfigurationRegistry` is like a public facade of the module, you should use it as an entry point.