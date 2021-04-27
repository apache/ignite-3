# Configuration

## Concepts
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

## Generated API
Imagine that you have schema from the example above. Then you'll have following interfaces generated:

These are main interfaces to manage your configuration:
```
public interface ParentConfiguration extends ConfigurationTree<ParentView, ParentChange> {
    RootKey<ParentConfiguration, ParentView> KEY = ...;

    NamedConfigurationTree<NamedElementConfiguration, NamedElementView, NamedElementChange> elements();
            
    ChildConfiguration child();

    ParentView value();

    Future<Void> change(Consumer<ParentChange> change);
}

public interface ChildConfiguration extends ConfigurationTree<ChildView, ChildChange> {
    ConfigurationValue<String> str();

    ChildView value();

    Future<Void> change(Consumer<ChildChange> change);
}
...
```
* they have methods to access their child nodes;
* they have methods to tak a configuration _snapshot_ - immutable "view" object;
* they have methods to update configuration values in every individual node;
* root interface has generated `RootKey` constant.

View interfaces look like this. All they have are getters for all declared properties.
```
public interface ParentView {
    NamedListView<? extends NamedElementView> elements();

    ChildView child();
}

public interface ChildView {
    String str();
}
...
```

Change interfaces look like this:
```
public interface ParentChange {
    ParentChange changeElements(Consumer<NamedListChange<NamedElementChange>> elements);

    ParentChange changeChild(Consumer<ChildChange> child);
}

public interface ChildChange {
    ChildChange changeStr(String str);
}
...
```
I think it's easier to demonstrate using this small example. Any subtree of any tree can be updated in a single
_transaction_:
```
ParentConfiguration parentCfg = ...;

parentConfiguration.change(parent ->
    parent.changeChild(child ->
        child.changeStr("newStr1")
    )
).get();

ChildConfiguration childCfg = parentCfg.child();

childCfg.changeStr("newStr2").get();
```
Every `change` object is basically a change request that's going to be processed asyncronously and transactionally.
It's important to note that there's a technical possibility to execute several change requests for different roots in a
single transaction, but all these roots _must have the same storage type_.