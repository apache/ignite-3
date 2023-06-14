# ignite-rest

This module defines [RestComponent](src/main/java/org/apache/ignite/internal/rest/RestComponent.java) that is responsible for:

- aggregating all REST API definitions by `@OpenAPIInclude` annotation at compile time
- creating a micronaut context at runtime and injecting all needed beans into the context
- starting the micronaut server at the configured port range

During the build time, the [Open API spec](https://spec.openapis.org/oas/v3.1.0) is generated from all API definitions that are included in
`@OpenAPIInclude` annotation. The spec is located in `ignite-rest/openapi`.

## How to create a new REST Endpoint

- define API in `ignite-rest-api` module
- link the API interface in `RestComponent`'s `@OpenAPIInclude` section
- implement an API interface in the module that is responsible for the API scope
- define micronaut factory that is responsible for the creation of beans that are needed for your Controller
- for extending the exception handling mechanism, implement micronaut's `ExceptionHandler` and include it in the micronaut factory
- create the instance of the RestFactory in the IgniteImpl constructor and put the factory to the RestComponent's constructor

> The class that implements the API should be a valid micronaut controller.
> Module with the API implementation should configure `micronaut-inject-java` annotation processor.

> Micronaut factories have to implement `RestFactory` from `ignite-rest-api` 

## Example of REST Endpoint

- API definition [ClusterManagementApi](../rest-api/src/main/java/org/apache/ignite/internal/rest/api/cluster/ClusterManagementApi.java)
- API implementation [ClusterManagementController](../cluster-management/src/main/java/org/apache/ignite/internal/cluster/management/rest/ClusterManagementController.java)
- `RestFactory` implementation [ClusterManagementRestFactory](../cluster-management/src/main/java/org/apache/ignite/internal/cluster/management/rest/ClusterManagementRestFactory.java)
- `ExceptionHandler` implementation [ClusterNotInitializedExceptionHandler](../cluster-management/src/main/java/org/apache/ignite/internal/cluster/management/rest/exception/handler/ClusterNotInitializedExceptionHandler.java)

## Architecture of REST server

`ignite-rest-api` module defines only API without any implementation. 
The main value of this module is to provide all needed information to generate an Open API spec.

REST server is started by [RestComponent](src/main/java/org/apache/ignite/internal/rest/RestComponent.java) in `ignite-rest` module.
This module also configures `micronaut-openapi` generator and generates an Open API spec during the build time.
But the only dependency of the `ignite-rest` is `ignite-rest-api`. **There is no dependency on any module that provides API implementation.**

REST Controllers together with RestFactories are set to RestComponent at runtime by IgniteImpl through the constructor.


## How to test REST Endpoints

Unit tests can be defined at the same module where the Controller is implemented, see [ClusterConfigurationControllerTest](../configuration/src/test/java/org/apache/ignite/internal/rest/configuration/ClusterConfigurationControllerTest.java).
To make them work test dependencies should include `micronaut-test-junit5`, `micronaut-http-client`, `micronaut-http-server-netty`. 
Also, `micronaut-inject-java` annotation processor should be configured.

Integration tests could be defined in runner module, see `ItInitializedClusterRestTest` and `ItNotInitializedClusterRestTest`.

