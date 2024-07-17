# Configuration

This module contains the API classes and the implementation for the Ignite Configuration framework.
The idea is to provide the so-called _Unified Configuration_ — a common way of configuring both local Ignite nodes
and remote Ignite clusters. The original concept is described in
[IEP-55](https://cwiki.apache.org/confluence/display/IGNITE/IEP-55+Unified+Configuration).

## Concepts

### Configuration Schema

Type-safe schema of a configuration, which is used for generating public API interfaces and
internal implementations to avoid writing boilerplate code. 

All schema classes must end with the `ConfigurationSchema` suffix.

### Configuration Registry

`ConfigurationRegistry` is the entry point of the module. It is used to register root keys, validators, storages,
polymorphic extensions and to start / stop the component. Refer to the class javadocs for more details.

### Root Key

All Ignite configuration instances can be represented by a forest, where every node has a name, usually referred
to as a _key_. `RootKey` interface represents a type-safe object that holds the _key_ of the root node of the 
configuration tree. 

Instances of this interface are generated automatically and are mandatory for registering the configuration roots.

### Example Schema

An example configuration schema may look like the following:

```java
@ConfigurationRoot(rootName = "rootLocal", type = ConfigurationType.LOCAL)
public static class ParentConfigurationSchema {
    @NamedConfigValue
    public NamedElementConfigurationSchema elements;

    @ConfigValue
    public ChildConfigurationSchema child;
    
    @ConfigValue
    public PolymorphicConfigurationSchema polymorphicChild;

    @ConfigValue
    public SecondChildConfigurationSchema secondChild;
}

@ConfigurationRoot(rootName = "rootDistributed", type = ConfigurationType.DISTRIBUTED)
public static class SecondParentConfigurationSchema extends AbstractRootConfigurationSchema { 
    @ConfigValue
    public ChildConfigurationSchema child;

    @ConfigValue
    public SecondChildConfigurationSchema secondChild;
}

@Config
public static class ChildConfigurationSchema {
    @Value(hasDefault = true)
    public String str1 = "foobar";

    @Value
    @Immutable
    public String str2;
}

@Config
public static class SecondChildConfigurationSchema extends AbstractConfigurationSchema {
    @Value(hasDefault = true)
    public long longVal = 0;
}

@PolymorphicConfig
public static class PolymorphicConfigurationSchema {
    @PolymorphicId(hasDefault = true)
    public String typeId = "first";
}

@PolymorphicConfigInstance("first")
public static class FirstPolymorphicInstanceConfigurationSchema extends PolymorphicConfigurationSchema {
    @Value(hasDefault = true)
    public int intVal = 0;
}

@AbstractConfiguration
public static class AbstractRootConfigurationSchema {
    @Value(hasDefault = true)
    public String strVal = "foobar";
}

@AbstractConfiguration
public static class AbstractConfigurationSchema {
  @Value(hasDefault = true)
  public int intVal = 0;
}

@ConfigurationExtension
public static class ExtendedChildConfigurationSchema extends ChildConfigurationSchema {
  @Value(hasDefault = true)
  public int intVal = 0;
}

@ConfigurationExtension(internal = true)
public static class InternalConfigurationSchema extends ChildConfigurationSchema {
  @Value(hasDefault = true)
  public String strVal = "foo";
}
```

* `@ConfigurationRoot` marks the root schema. It contains the following properties:
  * `type` property, which can either be `LOCAL` or `DISTRIBUTED`. This property dictates the _storage_ type used 
    to persist the schema — `Vault` or `Metastorage`. `Vault` stores data locally, while `Metastorage` is a distributed
    system that should store only cluster-wide configuration properties;
  * `rootName` property assigns a _key_ to the root node of the tree that will represent 
    the corresponding configuration schema;
* `@Config` is similar to the `@ConfigurationRoot` but represents an inner configuration node;
* `@PolymorphicConfig` is similar to the `@Config` and an abstract class in java, i.e. it cannot be instantiated, but it can be subclassed;
* `@PolymorphicConfigInstance` marks an inheritor of a polymorphic configuration. This annotation has a single property called `value` - 
   a unique identifier among the inheritors of one polymorphic configuration, used to define the type (schema) of the polymorphic configuration we are dealing with now;
* `@AbstractConfiguration` is similar to `@PolymorphicConfig` but its type cannot be changed and its inheritors must be annotated with
  either `@Config` or `@ConfigurationRoot`. Configuration schemas with this annotation cannot be used as a nested (sub)configuration;
* `@ConfigurationExtension` allows to extend existing `@Config` or `@ConfigurationRoot` configurations.
* `@ConfigValue` marks a nested schema field. Cyclic dependencies are not allowed;
* `@NamedConfigValue` is similar to `@ConfigValue`, but such fields represent a collection of properties, not a single
  instance. Every element of the collection will have a `String` name, similar to a `Map`.
  `NamedListConfiguration` interface is used to represent this field in the generated configuration classes. 
* `@Value` annotation marks the _leaf_ values. `hasDefault` property can be used to set default values for fields:
  if set to `true`, the default value will be used to initialize the annotated configuration field in case no value 
  has been provided explicitly. This annotation can only be present on fields of the Java primitive or `String` type.
    
  All _leaves_ must be public and corresponding configuration values **must not be null**;
* `@PolymorphicId` is similar to the `@Value`, but is used to store the type of polymorphic configuration (`@PolymorphicConfigInstance#value`) and must be a `String`.
* `@Immutable` annotation can only be present on fields marked with the `@Value` annotation. Annotated fields cannot be 
  changed after they have been initialized (either manually or by assigning a default value).

### Polymorphic configuration

This is the ability to create various forms of the same configuration.

Let's take an example of an SQL column configuration, suppose it can be one of the following types:
* varchar(max) - string with a maximum length;
* decimal(p,s) - decimal number with a fixed precision (p) and a scale (s);
* datetime(fsp) - date and time with a fractional seconds precision (fsp).

If you do not use polymorphic configuration, then the scheme will look something like this:

```java
@Config
public static class ColumnConfigurationSchema { 
    @Value
    public String type;

    @Value
    public String name;
    
    @Value
    public int maxLength;

    @Value
    public int precision;

    @Value
    public int scale;

    @Value
    public int fsp;
}
```

Such a scheme is redundant and can be confusing when using it, since it is not obvious which fields
are needed for each type of column. Instead, one can use a polymorphic configuration
that will look something like this:

```java
@PolymorphicConfig
public static class ColumnConfigurationSchema { 
    @PolymorphicId
    public String type;

    @Value
    public String name;
}

@PolymorphicConfigInstance("varchar")
public static class VarcharColumnConfigurationSchema extends ColumnConfigurationSchema {
    @Value
    public int maxLength;
}

@PolymorphicConfigInstance("decimal")
public static class DecimalColumnConfigurationSchema extends ColumnConfigurationSchema {
    @Value
    public int precision;

    @Value
    public int scale;
}

@PolymorphicConfigInstance("datetime")
public static class DatetimeColumnConfigurationSchema extends ColumnConfigurationSchema {
    @Value
    public int fsp;
}
```

Thus, a column can only be one of these (varchar, decimal and datetime) types and will contain the
type, name and fields specific to it.

### Configuration extension

Sometimes it is necessary to extend a configuration with a new field, 
but it is not desirable (or possible) to modify the original configuration. 

Suppose we have a `security` module and want to add one more authentication component that is located 
in a different module that depends on `security`.

```java
@ConfigurationRoot(rootName = "security", type = ConfigurationType.DISTRIBUTED)
public class SecurityConfigurationSchema {
    @ConfigValue
    public AuthenticationConfigurationSchema authentication;
}

@Config
public class AuthenticationConfigurationSchema {
  @Value(hasDefault = true)
  public final boolean enabled = false;
}
```

What we need to do is to subclass the configuration we want to extend.

```java
@ConfigurationExtension
public class UserSecurityConfigurationSchema extends SecurityConfigurationSchema {
  @Value
  public final String user;
}
```
And the resulting configuration will look as if the field `user` was declared directly in `SecurityConfigurationSchema`:

```json
{
  "security": {
    "authentication": {
      "enabled": false
    },
    "user": "admin"
  }
}
```

### Internal extensions

Sometimes it's necessary to have configuration values that are hidden form user:
- these configuration values are available from internal code only
- they are not accessible in JSON or any other configuration view representation
- they can't be updated via CLI's HOCON update requests or any other public API calls

To achieve this, one can use `@ConfigurationExtension(internal = true)` annotation on a configuration schema.

Following the previous example with `security` module, let's add an extension to `SecurityConfigurationSchema`:

```java
@ConfigurationExtension(internal = true)
public class SecurityUpgradeConfigurationSchema extends SecurityConfigurationSchema {
  @Value
  public final String version;
}
```

### Additional annotations

* `@InjectedName` - allows to get the key associated with the configuration in the named list, see javadoc for details.
* `@InternalId` - allows to get an internal id in a named list, see javadoc for details.

## Generated API

Configuration interfaces are generated at compile time. For the example above, the following code would be generated: 

```java
public interface ParentConfiguration extends ConfigurationTree<ParentView, ParentChange> {
    RootKey<ParentConfiguration, ParentView> KEY = ...;

    NamedConfigurationTree<NamedElementConfiguration, NamedElementView, NamedElementChange> elements();
            
    ChildConfiguration child();

    PolymorphicConfiguration polymorphicChild();

    ParentView value();

    Future<Void> change(Consumer<ParentChange> change);
}

public interface ChildConfiguration extends ConfigurationTree<ChildView, ChildChange> {
    ConfigurationValue<String> str();

    ChildView value();

    Future<Void> change(Consumer<ChildChange> change);
}

public interface PolymorphicConfiguration extends ConfigurationTree<PolymorphicView, PolymorphicChange> {
    // Read only.  
    ConfigurationValue<String> typeId();

    PolymorphicView value();

    Future<Void> change(Consumer<PolymorphicChange> change);
}

public interface FirstPolymorphicInstanceConfiguration extends PolymorphicConfiguration {
    ConfigurationValue<Integer> intVal();
}
```

* `KEY` constant uniquely identifies the configuration root;
* `child()` method can be used to access the child node;
* `value()` method creates a corresponding _snapshot_ (an immutable view) of the configuration node;
* `change()` method should be used to update the values in the configuration tree.

### Configuration Snapshots

`value()` methods return a read-only view of the configuration tree, represented by a special set of _View_ interfaces.
For the example above, the following interfaces would be generated:

```java
public interface ParentView {
    NamedListView<? extends NamedElementView> elements();

    ChildView child();

    PolymorphicView polymorphicChild();
}

public interface ChildView {
    String str();
}

public interface PolymorphicView {
    String typeId();
}

public interface FirstPolymorphicInstanceView extends PolymorphicView {
    int intVal();
}
```

`ParentView#polymorphicChild()` will return a view of a specific type of polymorphic configuration, for example `FirstPolymorphicInstanceView`.

### Dynamic configuration defaults

Configuration defaults are defined in the configuration schema. However, it is not possible define them there in the following cases:

* the value is a list (`NamedListConfiguration`).
* the default value is not known at compile time and it depends on some external factors.

In such cases, one can override `ConfigurationModule.patchConfigurationWithDynamicDefaults` method to provide the defaults. The method will
be called on cluster initialization with the user-provided configuration tree as an argument.

Note, that dynamic defaults are not supported for node local configuration.

```java
public class MyConfigurationModule extends AbstractConfigurationModule {
  @Override
  protected void patchConfigurationWithDynamicDefaults(SuperRootChange rootChange) {
    rootChange.changeRoot(SecurityConfiguration.KEY).changeAuthentication(authenticationChange -> {
      if (authenticationChange.changeProviders().size() == 0) {
        authenticationChange.changeProviders().create(DEFAULT_PROVIDER_NAME, change -> {
          change.convert(BasicAuthenticationProviderChange.class)
                  .changeUsername(DEFAULT_USERNAME)
                  .changePassword(DEFAULT_PASSWORD)
                  .changeRoles(AuthorizationConfigurationSchema.DEFAULT_ROLE);
        });
      }
    });
  }
}
```

### Configuration initialization

Custom configuration initialization can be done by calling `ConfigurationRegistry#initializeConfigurationWith` method. The method accepts
initial configuration that will be used as a base for the configuration tree. If the configuration is not provided, the default
configuration will be used. The method should be called before `ConfigurationRegistry#start` method. If the method is called after the
start, the provided configuration will be ignored.

### Changing the configuration

To modify the configuration tree, one should use the `change` method, which executes the update requests 
asynchronously and in a transactional manner. Update requests are represented by a set of `Change` interfaces.
For the example above, the following interfaces would be generated:

```java
public interface ParentChange extends ParentView { 
    ParentChange changeElements(Consumer<NamedListChange<NamedElementChange>> elements);
    NamedListChange<NamedElementChange> changeElements();

    ParentChange changeChild(Consumer<ChildChange> child);
    ChildChange changeChild();

    ParentChange changePolymorphicChild(Consumer<PolymorphicChange> polymorphicChild);
    PolymorphicChange changePolymorphicChild();
}

public interface ChildChange extends ChildView {
    ChildChange changeStr(String str);
}

public interface PolymorphicChange extends FirstPolymorphicView {
    <T extends PolymorphicChange> T convert(Class<T> changeClass);
}

public interface FirstPolymorphicInstanceChange extends FirstPolymorphicInstanceView, PolymorphicChange {
    FirstPolymorphicInstanceChange changeIntVal(int intVal);
}
```

Example of updating all child nodes of the parent configuration in a single transaction:

```java
ParentConfiguration parentCfg = ...;

parentCfg.change(parent ->
    parent.changeChild(child ->
        child.changeStr("newStr1")
    )
).get();

parentCfg.change(parent ->
    parent.changeChild().changeStr("newStr2")
).get();

ChildConfiguration childCfg = parentCfg.child();

childCfg.changeStr("newStr3").get();
```

Example of changing the type of a polymorphic configuration:

```java
ParentConfiguration parentCfg = ...;

parentCfg.polymorphicChild()
        .change(polymorphicCfg -> 
            polymorphicCfg.convert(FirstPolymorphicInstanceChange.class).changeIntVal(100)
        ).get();
```

It is possible to execute several change requests for different roots in a single transaction, but all these roots 
_must have the same storage type_. However, this is only possible using the command line tool via the REST API, 
there's no public Java API at the moment.

### Accessing up-to-date configuration properties directly from storage
Sometimes it's desirable to have a peek into the future, to read the configuration state that has not yet been processed by the current
node. There's API for this purpose.

Please refer to `ConfigurationUtil#directProxy(ConfigurationProperty)` for details. There are many usages of this method in tests. It
should provide the context.
