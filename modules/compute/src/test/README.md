# ignite-compute

## Unit tests
`modules/compute/src/test/resources/units/test-units-1.0-SNAPSHOT-src.zip` contains a zip archive with a 
test project which was used to create jars for `org.apache.ignite.internal.compute.loader.JobClassLoaderFactoryTest` tests.

[unit1-1.0-SNAPSHOT.jar](resources%2Funits%2Funit2%2F1.0.0%2Funit1-1.0-SNAPSHOT.jar) contains two classes 
which are used in tests:
* `org.apache.ignite.internal.compute.unit1.Unit1` - extends Runnable and returns 1 as Integer.
* `org.my.job.compute.unit.Job1Utility`

[unit2-1.0-SNAPSHOT.jar](resources%2Funits%2Funit2%2F2.0.0%2Funit2-1.0-SNAPSHOT.jar) contains two classes
which are used in tests:
* `org.apache.ignite.internal.compute.unit1.Unit2` - extends Runnable and returns "Hello World!" as String.
* `org.my.job.compute.unit.Job2Utility`
