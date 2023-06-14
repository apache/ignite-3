# ignite-rest-api

This module defines REST APIs that might be provided by Ignite 3. Also, common
DTOs and error handlers are defined.

## API definition

The API definition is a java interface annotated with micronaut `@Controller`  
annotations and several `swagger-annotations`. All those annotations are 
needed to generate a valid [Open API spec](https://spec.openapis.org/oas/v3.1.0) from these interfaces.

[ClusterManagementApi](src/main/java/org/apache/ignite/internal/rest/api/cluster/ClusterManagementApi.java) is an example of API definition.

## Error handling

Ignite 3 implements the [application/problem+json](https://www.rfc-editor.org/rfc/rfc7807.html) in all endpoints. That's why
problem definition and common problem handling are defined in this module. Here is how it works:

- [`IgniteException`](../api/src/main/java/org/apache/ignite/lang/IgniteException.java) is thrown in any Ignite component
- REST Controller might not handle this exception 
- [`IgniteExceptionHandler`](src/main/java/org/apache/ignite/internal/rest/exception/handler/IgniteExceptionHandler.java) 
is invoked by micronaut infrastructure
- [`IgniteExceptionHandler`](src/main/java/org/apache/ignite/internal/rest/exception/handler/IgniteExceptionHandler.java)  handles 
[`IgniteException`](../api/src/main/java/org/apache/ignite/lang/IgniteException.java) and returns a valid `application/problem+json`

> Make sure that [`IgniteExceptionHandler`](src/main/java/org/apache/ignite/internal/rest/exception/handler/IgniteExceptionHandler.java)
> has been loaded into micronaut context otherwise this class won't be invoked.

If you want to implement your exception handler the best place to do it 
is the module where you define the REST controller. Don't put your handlers in
`ignite-rest-api` unless it is needed for all REST endpoints.
