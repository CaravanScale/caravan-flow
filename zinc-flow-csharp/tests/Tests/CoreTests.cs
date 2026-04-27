using System.Text;
using ZincFlow.Core;
using ZincFlow.Fabric;
using ZincFlow.StdLib;
using static ZincFlow.Tests.TestRunner;
using static ZincFlow.Tests.Helpers;

namespace ZincFlow.Tests;

public static class CoreTests
{
    public static void RunAll()
    {
        TestFlowFileBasics();
        TestContentTypes();
        TestContentStore();
        TestResolveErrors();
        TestV3Roundtrip();
        TestV3MultipleRoundtrip();
        TestV3EmptyAttributes();
        TestV3EmptyContent();
    }

    static void TestFlowFileBasics()
    {
        Console.WriteLine("--- FlowFile Basics ---");
        var ff = FlowFile.Create("hello"u8, new() { ["type"] = "order", ["source"] = "test" });
        AssertTrue("id starts with ff-", ff.Id.StartsWith("ff-"));
        AssertTrue("type attr", ff.Attributes.TryGetValue("type", out var t) && t == "order");
        AssertTrue("source attr", ff.Attributes.TryGetValue("source", out var s) && s == "test");

        var ff2 = FlowFile.WithAttribute(ff, "env", "dev");
        AssertTrue("added env", ff2.Attributes.TryGetValue("env", out var e) && e == "dev");
        AssertTrue("same id after WithAttribute", ff2.NumericId == ff.NumericId);
        AssertTrue("original attrs preserved", ff2.Attributes.TryGetValue("type", out var t2) && t2 == "order");

        var ff3 = FlowFile.WithContent(ff, new Raw("new data"u8));
        AssertTrue("same id after WithContent", ff3.NumericId == ff.NumericId);
    }

    static void TestContentTypes()
    {
        Console.WriteLine("--- Content Types ---");
        var raw = new Raw("data"u8);
        AssertFalse("raw is not record", raw is RecordContent);
        AssertIntEqual("raw size", raw.Size, 4);

        var claim = new ClaimContent("claim-1", 1024);
        AssertFalse("claim is not record", claim is RecordContent);
        AssertIntEqual("claim size", claim.Size, 1024);
    }

    static void TestContentStore()
    {
        Console.WriteLine("--- Content Store ---");
        var store = new MemoryContentStore();
        var claimId = store.Store("hello world"u8.ToArray());
        AssertTrue("claim id not empty", claimId != "");
        AssertTrue("claim exists", store.Exists(claimId));

        var retrieved = store.Retrieve(claimId);
        AssertEqual("retrieve content", Encoding.UTF8.GetString(retrieved), "hello world");

        store.Delete(claimId);
        AssertFalse("deleted claim gone", store.Exists(claimId));

        // maybeOffload — small content stays Raw
        var small = ContentHelpers.MaybeOffload(store, "small"u8.ToArray());
        AssertTrue("small stays raw", small is Raw);

        // resolve Raw
        var (data, err) = ContentHelpers.Resolve(store, new Raw("test"u8));
        AssertEqual("resolve raw", Encoding.UTF8.GetString(data), "test");
        AssertEqual("resolve raw no error", err, "");
    }

    static void TestResolveErrors()
    {
        Console.WriteLine("--- Resolve Errors ---");
        var store = new MemoryContentStore();

        var (data1, err1) = ContentHelpers.Resolve(store, new ClaimContent("nonexistent", 100));
        AssertIntEqual("missing claim returns empty", data1.Length, 0);

        var (_, err2) = ContentHelpers.Resolve(store, new RecordContent(new Schema("empty", []), new List<Record>()));
        AssertTrue("records resolve has error", err2 != "");

        var cid = store.Store("claimed data"u8.ToArray());
        var (data3, err3) = ContentHelpers.Resolve(store, new ClaimContent(cid, 12));
        AssertEqual("resolve claim", Encoding.UTF8.GetString(data3), "claimed data");
        AssertEqual("resolve claim no error", err3, "");
    }

    static void TestV3Roundtrip()
    {
        Console.WriteLine("--- V3 Roundtrip ---");
        var ff = FlowFile.Create("Hello, NiFi!"u8, new() { ["filename"] = "test.txt", ["type"] = "doc" });
        var (content, _) = ContentHelpers.Resolve(new MemoryContentStore(), ff.Content);
        var packed = FlowFileV3.Pack(ff, content);
        AssertTrue("packed not empty", packed.Length > 0);
        AssertEqual("magic header", Encoding.UTF8.GetString(packed, 0, 7), "NiFiFF3");

        var (unpacked, _, error) = FlowFileV3.Unpack(packed, 0);
        AssertEqual("no unpack error", error, "");
        AssertTrue("filename survives", unpacked!.Attributes.TryGetValue("filename", out var fn) && fn == "test.txt");
        AssertTrue("type survives", unpacked.Attributes.TryGetValue("type", out var tp) && tp == "doc");

        var (bytes, _) = ContentHelpers.Resolve(new MemoryContentStore(), unpacked.Content);
        AssertEqual("content survives", Encoding.UTF8.GetString(bytes), "Hello, NiFi!");
    }

    static void TestV3MultipleRoundtrip()
    {
        Console.WriteLine("--- V3 Multiple Roundtrip ---");
        var ff1 = FlowFile.Create("first"u8, new() { ["index"] = "1" });
        var ff2 = FlowFile.Create("second"u8, new() { ["index"] = "2" });
        var store = new MemoryContentStore();
        var (c1, _) = ContentHelpers.Resolve(store, ff1.Content);
        var (c2, _) = ContentHelpers.Resolve(store, ff2.Content);
        var packed = FlowFileV3.PackMultiple([ff1, ff2], [c1, c2]);
        var all = FlowFileV3.UnpackAll(packed);
        AssertIntEqual("unpacked count", all.Count, 2);
        AssertTrue("first index", all[0].Attributes.TryGetValue("index", out var i1) && i1 == "1");
        AssertTrue("second index", all[1].Attributes.TryGetValue("index", out var i2) && i2 == "2");
    }

    static void TestV3EmptyAttributes()
    {
        Console.WriteLine("--- V3 Empty Attributes ---");
        var ff = FlowFile.Create("payload"u8, new());
        var (content, _) = ContentHelpers.Resolve(new MemoryContentStore(), ff.Content);
        var packed = FlowFileV3.Pack(ff, content);
        var (unpacked, _, error) = FlowFileV3.Unpack(packed, 0);
        AssertEqual("no error", error, "");
        AssertIntEqual("zero attrs", unpacked!.Attributes.Count, 0);
    }

    static void TestV3EmptyContent()
    {
        Console.WriteLine("--- V3 Empty Content ---");
        var ff = FlowFile.Create(ReadOnlySpan<byte>.Empty, new() { ["tag"] = "empty" });
        var packed = FlowFileV3.Pack(ff, []);
        var (unpacked, _, error) = FlowFileV3.Unpack(packed, 0);
        AssertEqual("no error", error, "");
        AssertTrue("tag preserved", unpacked!.Attributes.TryGetValue("tag", out var tag) && tag == "empty");
    }
}
