# ignite-compute

## Unit tests
[ignite-jobs-1.0-SNAPSHOT-src.zip](resources%2Funits%2Fignite-jobs-1.0-SNAPSHOT-src.zip) contains a zip archive with a 
test project which was used to create jars for `org.apache.ignite.internal.compute.loader.JobClassLoaderFactoryTest` tests.

[ignite-ut-job1-1.0-SNAPSHOT.jar](resources%2Funits%2Funit1%2F1.0.0%2Fignite-ut-job1-1.0-SNAPSHOT.jar) contains two classes 
which are used in tests:
* `org.apache.ignite.internal.compute.unit1.Unit1` - `extends org.apache.ignite.compute.ComputeJob` and returns 1 as Integer.
* `org.my.job.compute.unit.Job1Utility`

[ignite-ut-job2-1.0-SNAPSHOT.jar](resources%2Funits%2Funit1%2F2.0.0%2Fignite-ut-job2-1.0-SNAPSHOT.jar) contains two classes
which are used in tests:
* `org.apache.ignite.internal.compute.unit1.Unit2` - extends `org.apache.ignite.compute.ComputeJob` and returns "Hello World!" as String.
* `org.my.job.compute.unit.Job2Utility`
