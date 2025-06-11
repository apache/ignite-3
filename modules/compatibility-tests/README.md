# Ignite compatibility tests 

This module contains tests that verify Ignite cluster upgrades.

## Base test
The `PersistenceTestBase` serves as a base compatibility test. It starts and inits a cluster of a specified previous version, calls 
`setupBaseVersion` method with the Ignite client connected to the first node of the cluster. Then it stops the cluster and starts it in the
embedded mode using current sources. This is done once per class, similar to the `ClusterPerClassIntegrationTest`. This base test is
parameterized using the list of versions from the `versions.json` resource file. By default the test takes two latest versions as the base
version. If the `testAllVersions` system property is defined, then all the versions are tested.

## Describing versions
When new version is released, add new object to the `versions` array in the `versions.json` file like so:
```json
{
  "version": "3.1.0"
}
```
In case there's a need for the specific node configuration override for that version, the `configOverrides` object can be added:
```json
{
  "version": "3.0.0",
  "configOverrides": {
    "ignite.network.membership.scaleCube.metadataTimeout": 10000
  }
}
```

## Running from TeamCity
Before executing `test` task, `resolveCompatibilityTestDependencies` task should be started to download all necessary dependencies from the
Maven repository.
