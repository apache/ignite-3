# Tests for NativeAOT Compilation

## Run Tests

```shell
dotnet publish --runtime linux-x64 --configuration Release
bin/Release/net8.0/linux-x64/publish/Apache.Ignite.Tests.Aot
```

## Design Considerations

* Do not use [MSTest.SourceGeneration](https://devblogs.microsoft.com/dotnet/testing-your-native-aot-dotnet-apps/)
  * Beta
  * Unclear licensing
