using ZincFlow.Tests;

TestRunner.Pass = 0; TestRunner.Fail = 0;
Console.WriteLine("=== zinc-flow-csharp test suite ===");
Console.WriteLine();

CoreTests.RunAll();
ProcessorTests.RunAll();
CodecTests.RunAll();
ExpressionTests.RunAll();
ValidatorTests.RunAll();
EmbeddedSchemaRegistryTests.RunAll();
FlowFileV3BoundaryTests.RunAll();
FabricTests.RunAll();
SourceTests.RunAll();
PipelineTests.RunAll();
E2ETests.RunAll();

Console.WriteLine();
Console.WriteLine($"=== {TestRunner.Pass} passed, {TestRunner.Fail} failed ({TestRunner.Pass + TestRunner.Fail} total) ===");
return TestRunner.Fail > 0 ? 1 : 0;
