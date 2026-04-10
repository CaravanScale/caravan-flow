namespace ZincFlow.Tests;

public static class TestRunner
{
    public static int Pass, Fail;

    public static void AssertTrue(string label, bool value)
    {
        if (value) { Pass++; Console.WriteLine($"  PASS: {label}"); }
        else { Fail++; Console.WriteLine($"  FAIL: {label} — expected true"); }
    }

    public static void AssertFalse(string label, bool value) => AssertTrue(label, !value);

    public static void AssertEqual(string label, string actual, string expected)
    {
        if (actual == expected) { Pass++; Console.WriteLine($"  PASS: {label}"); }
        else { Fail++; Console.WriteLine($"  FAIL: {label} — expected '{expected}', got '{actual}'"); }
    }

    public static void AssertIntEqual(string label, int actual, int expected)
    {
        if (actual == expected) { Pass++; Console.WriteLine($"  PASS: {label}"); }
        else { Fail++; Console.WriteLine($"  FAIL: {label} — expected {expected}, got {actual}"); }
    }
}
