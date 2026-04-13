using ZincFlow.Core;
using ZincFlow.StdLib;
using static ZincFlow.Tests.TestRunner;

namespace ZincFlow.Tests;

public static class ExpressionTests
{
    public static void RunAll()
    {
        TestLiterals();
        TestArithmeticTypePromotion();
        TestArithmeticEdgeCases();
        TestStringConcatenation();
        TestComparisons();
        TestLogicalOperators();
        TestUnaryOperators();
        TestPrecedenceAndParens();
        TestIdentifierResolution();
        TestStringFunctions();
        TestSubstringFunction();
        TestMathFunctions();
        TestCoalesceAndIf();
        TestTypeCasts();
        TestNullSemantics();
        TestRecordResolver();
        TestAttributeResolver();
        TestComplexExpressions();
        TestParseErrors();
    }

    private sealed class MapResolver : IValueResolver
    {
        private readonly Dictionary<string, EvalValue> _map;
        public MapResolver(Dictionary<string, EvalValue> map) => _map = map;
        public EvalValue Resolve(string name) => _map.TryGetValue(name, out var v) ? v : EvalValue.Null;
    }

    private static IValueResolver Empty() => new MapResolver(new());

    static void TestLiterals()
    {
        Console.WriteLine("--- Expression: literals ---");
        var ctx = Empty();
        AssertEqual("int literal", ExpressionEngine.Evaluate("42", ctx).AsString(), "42");
        AssertEqual("double literal", ExpressionEngine.Evaluate("3.14", ctx).AsString(), "3.14");
        AssertEqual("string literal", ExpressionEngine.Evaluate("\"hello\"", ctx).AsString(), "hello");
        AssertEqual("single-quoted string", ExpressionEngine.Evaluate("'world'", ctx).AsString(), "world");
        AssertTrue("true literal", ExpressionEngine.Evaluate("true", ctx).BoolVal);
        AssertFalse("false literal", ExpressionEngine.Evaluate("false", ctx).BoolVal);
        AssertTrue("null literal", ExpressionEngine.Evaluate("null", ctx).Type == EvalValue.Kind.Null);
        AssertEqual("scientific", ExpressionEngine.Evaluate("1.5e3", ctx).AsString(), "1500");
    }

    static void TestArithmeticTypePromotion()
    {
        Console.WriteLine("--- Expression: arithmetic type promotion ---");
        var ctx = Empty();
        var r1 = ExpressionEngine.Evaluate("2 + 3", ctx);
        AssertTrue("int+int = long", r1.Type == EvalValue.Kind.Long);
        AssertEqual("int+int value", r1.AsString(), "5");

        var r2 = ExpressionEngine.Evaluate("2 + 3.0", ctx);
        AssertTrue("int+double = double", r2.Type == EvalValue.Kind.Double);
        AssertEqual("int+double value", r2.AsString(), "5");

        var r3 = ExpressionEngine.Evaluate("10 * 5", ctx);
        AssertTrue("int*int = long", r3.Type == EvalValue.Kind.Long && r3.LongVal == 50);

        var r4 = ExpressionEngine.Evaluate("10 / 3", ctx);
        AssertTrue("long div = long truncated", r4.Type == EvalValue.Kind.Long && r4.LongVal == 3);

        var r5 = ExpressionEngine.Evaluate("10.0 / 3", ctx);
        AssertTrue("double div = double", r5.Type == EvalValue.Kind.Double && Math.Abs(r5.DoubleVal - 3.333333) < 0.001);

        var r6 = ExpressionEngine.Evaluate("10 % 3", ctx);
        AssertTrue("modulo", r6.Type == EvalValue.Kind.Long && r6.LongVal == 1);
    }

    static void TestArithmeticEdgeCases()
    {
        Console.WriteLine("--- Expression: arithmetic edge cases ---");
        var ctx = Empty();
        var div0 = ExpressionEngine.Evaluate("10 / 0", ctx);
        AssertTrue("long /0 returns 0", div0.Type == EvalValue.Kind.Long && div0.LongVal == 0);

        var dDiv0 = ExpressionEngine.Evaluate("10.0 / 0.0", ctx);
        AssertTrue("double /0 returns NaN", dDiv0.Type == EvalValue.Kind.Double && double.IsNaN(dDiv0.DoubleVal));

        var negate = ExpressionEngine.Evaluate("-5 + 3", ctx);
        AssertTrue("unary minus", negate.Type == EvalValue.Kind.Long && negate.LongVal == -2);

        var doubleNeg = ExpressionEngine.Evaluate("- -5", ctx);
        AssertTrue("double negation", doubleNeg.LongVal == 5);
    }

    static void TestStringConcatenation()
    {
        Console.WriteLine("--- Expression: string concatenation ---");
        var ctx = Empty();
        AssertEqual("str+str", ExpressionEngine.Evaluate("\"a\" + \"b\"", ctx).AsString(), "ab");
        AssertEqual("str+num", ExpressionEngine.Evaluate("\"price=\" + 42", ctx).AsString(), "price=42");
        AssertEqual("num+str", ExpressionEngine.Evaluate("42 + \"!\"", ctx).AsString(), "42!");
    }

    static void TestComparisons()
    {
        Console.WriteLine("--- Expression: comparisons ---");
        var ctx = Empty();
        AssertTrue("== equal", ExpressionEngine.Evaluate("5 == 5", ctx).BoolVal);
        AssertFalse("!= equal", ExpressionEngine.Evaluate("5 != 5", ctx).BoolVal);
        AssertTrue("numeric eq across types", ExpressionEngine.Evaluate("5 == 5.0", ctx).BoolVal);
        AssertTrue("< lt", ExpressionEngine.Evaluate("3 < 5", ctx).BoolVal);
        AssertFalse("< not lt", ExpressionEngine.Evaluate("5 < 3", ctx).BoolVal);
        AssertTrue("<= eq", ExpressionEngine.Evaluate("5 <= 5", ctx).BoolVal);
        AssertTrue("> gt", ExpressionEngine.Evaluate("5 > 3", ctx).BoolVal);
        AssertTrue(">= eq", ExpressionEngine.Evaluate("5 >= 5", ctx).BoolVal);
        AssertTrue("string eq", ExpressionEngine.Evaluate("\"abc\" == \"abc\"", ctx).BoolVal);
        AssertTrue("string lex lt", ExpressionEngine.Evaluate("\"abc\" < \"abd\"", ctx).BoolVal);
    }

    static void TestLogicalOperators()
    {
        Console.WriteLine("--- Expression: logical operators ---");
        var ctx = Empty();
        AssertTrue("true && true", ExpressionEngine.Evaluate("true && true", ctx).BoolVal);
        AssertFalse("true && false", ExpressionEngine.Evaluate("true && false", ctx).BoolVal);
        AssertTrue("false || true", ExpressionEngine.Evaluate("false || true", ctx).BoolVal);
        AssertFalse("false || false", ExpressionEngine.Evaluate("false || false", ctx).BoolVal);
        AssertFalse("not true", ExpressionEngine.Evaluate("!true", ctx).BoolVal);
        AssertTrue("not false", ExpressionEngine.Evaluate("!false", ctx).BoolVal);
        AssertTrue("truthy long", ExpressionEngine.Evaluate("5 && 1", ctx).BoolVal);
        AssertFalse("falsy long", ExpressionEngine.Evaluate("0 && 1", ctx).BoolVal);
        AssertFalse("empty string falsy", ExpressionEngine.Evaluate("\"\" && true", ctx).BoolVal);
    }

    static void TestUnaryOperators()
    {
        Console.WriteLine("--- Expression: unary operators ---");
        var ctx = Empty();
        AssertTrue("unary minus on int", ExpressionEngine.Evaluate("-7", ctx).LongVal == -7);
        AssertTrue("unary minus on double", Math.Abs(ExpressionEngine.Evaluate("-3.14", ctx).DoubleVal + 3.14) < 0.001);
        AssertTrue("not on truthy", !ExpressionEngine.Evaluate("!1", ctx).BoolVal);
        AssertTrue("not on falsy", ExpressionEngine.Evaluate("!0", ctx).BoolVal);
    }

    static void TestPrecedenceAndParens()
    {
        Console.WriteLine("--- Expression: precedence + parens ---");
        var ctx = Empty();
        AssertTrue("mul before add", ExpressionEngine.Evaluate("2 + 3 * 4", ctx).LongVal == 14);
        AssertTrue("paren overrides", ExpressionEngine.Evaluate("(2 + 3) * 4", ctx).LongVal == 20);
        AssertTrue("nested parens", ExpressionEngine.Evaluate("((1 + 2) * (3 + 4))", ctx).LongVal == 21);
        AssertTrue("comparison after add", ExpressionEngine.Evaluate("2 + 3 == 5", ctx).BoolVal);
        AssertTrue("logical after compare", ExpressionEngine.Evaluate("5 > 3 && 2 < 4", ctx).BoolVal);
    }

    static void TestIdentifierResolution()
    {
        Console.WriteLine("--- Expression: identifier resolution ---");
        var ctx = new MapResolver(new()
        {
            ["x"] = EvalValue.From(10L),
            ["y"] = EvalValue.From(2.5),
            ["name"] = EvalValue.From("Alice")
        });

        var r1 = ExpressionEngine.Evaluate("x + 5", ctx);
        AssertTrue("long ref + literal", r1.LongVal == 15);

        var r2 = ExpressionEngine.Evaluate("x * y", ctx);
        AssertTrue("long * double = double", r2.Type == EvalValue.Kind.Double && Math.Abs(r2.DoubleVal - 25.0) < 0.001);

        var r3 = ExpressionEngine.Evaluate("\"Hi \" + name", ctx);
        AssertEqual("string concat with ref", r3.AsString(), "Hi Alice");

        var missing = ExpressionEngine.Evaluate("missing_field + 1", ctx);
        AssertTrue("missing ident → null", missing.Type == EvalValue.Kind.Null);
    }

    static void TestStringFunctions()
    {
        Console.WriteLine("--- Expression: string functions ---");
        var ctx = Empty();
        AssertEqual("upper", ExpressionEngine.Evaluate("upper(\"hello\")", ctx).AsString(), "HELLO");
        AssertEqual("lower", ExpressionEngine.Evaluate("lower(\"HELLO\")", ctx).AsString(), "hello");
        AssertEqual("trim", ExpressionEngine.Evaluate("trim(\"  hi  \")", ctx).AsString(), "hi");
        AssertTrue("length", ExpressionEngine.Evaluate("length(\"abcd\")", ctx).LongVal == 4);
        AssertEqual("replace", ExpressionEngine.Evaluate("replace(\"foo bar\", \"foo\", \"baz\")", ctx).AsString(), "baz bar");
        AssertEqual("concat varargs", ExpressionEngine.Evaluate("concat(\"a\", \"-\", \"b\", \"-\", 42)", ctx).AsString(), "a-b-42");
        AssertTrue("contains true", ExpressionEngine.Evaluate("contains(\"hello world\", \"world\")", ctx).BoolVal);
        AssertFalse("contains false", ExpressionEngine.Evaluate("contains(\"hello\", \"xyz\")", ctx).BoolVal);
        AssertTrue("startsWith", ExpressionEngine.Evaluate("startsWith(\"prefix-x\", \"prefix\")", ctx).BoolVal);
        AssertTrue("endsWith", ExpressionEngine.Evaluate("endsWith(\"foo.csv\", \".csv\")", ctx).BoolVal);
    }

    static void TestSubstringFunction()
    {
        Console.WriteLine("--- Expression: substring ---");
        var ctx = Empty();
        AssertEqual("substr from", ExpressionEngine.Evaluate("substring(\"hello\", 2)", ctx).AsString(), "llo");
        AssertEqual("substr range", ExpressionEngine.Evaluate("substring(\"hello\", 1, 3)", ctx).AsString(), "ell");
        AssertEqual("substr clamps end", ExpressionEngine.Evaluate("substring(\"hi\", 0, 100)", ctx).AsString(), "hi");
        AssertEqual("substr clamps start", ExpressionEngine.Evaluate("substring(\"hi\", -5)", ctx).AsString(), "hi");
    }

    static void TestMathFunctions()
    {
        Console.WriteLine("--- Expression: math functions ---");
        var ctx = Empty();
        AssertTrue("abs neg long", ExpressionEngine.Evaluate("abs(-7)", ctx).LongVal == 7);
        AssertTrue("abs neg double", Math.Abs(ExpressionEngine.Evaluate("abs(-2.5)", ctx).DoubleVal - 2.5) < 0.001);
        AssertTrue("min", ExpressionEngine.Evaluate("min(3, 7)", ctx).LongVal == 3);
        AssertTrue("max", ExpressionEngine.Evaluate("max(3, 7)", ctx).LongVal == 7);
        AssertTrue("floor", ExpressionEngine.Evaluate("floor(3.7)", ctx).LongVal == 3);
        AssertTrue("ceil", ExpressionEngine.Evaluate("ceil(3.2)", ctx).LongVal == 4);
        AssertTrue("round up", ExpressionEngine.Evaluate("round(3.6)", ctx).LongVal == 4);
        AssertTrue("round half away", ExpressionEngine.Evaluate("round(2.5)", ctx).LongVal == 3);
        AssertTrue("pow", Math.Abs(ExpressionEngine.Evaluate("pow(2, 10)", ctx).DoubleVal - 1024.0) < 0.001);
        AssertTrue("sqrt", Math.Abs(ExpressionEngine.Evaluate("sqrt(16)", ctx).DoubleVal - 4.0) < 0.001);
    }

    static void TestCoalesceAndIf()
    {
        Console.WriteLine("--- Expression: coalesce + if ---");
        var ctx = new MapResolver(new()
        {
            ["a"] = EvalValue.Null,
            ["b"] = EvalValue.From(""),
            ["c"] = EvalValue.From("hit"),
            ["d"] = EvalValue.From("never")
        });
        AssertEqual("coalesce skips null+empty", ExpressionEngine.Evaluate("coalesce(a, b, c, d)", ctx).AsString(), "hit");
        AssertTrue("coalesce all empty → null", ExpressionEngine.Evaluate("coalesce(a, b)", ctx).Type == EvalValue.Kind.Null);

        AssertEqual("if true branch", ExpressionEngine.Evaluate("if(5 > 3, \"yes\", \"no\")", Empty()).AsString(), "yes");
        AssertEqual("if false branch", ExpressionEngine.Evaluate("if(5 < 3, \"yes\", \"no\")", Empty()).AsString(), "no");
        AssertTrue("if no else", ExpressionEngine.Evaluate("if(false, 1)", Empty()).Type == EvalValue.Kind.Null);
    }

    static void TestTypeCasts()
    {
        Console.WriteLine("--- Expression: type casts ---");
        var ctx = Empty();
        AssertTrue("int(\"42\")", ExpressionEngine.Evaluate("int(\"42\")", ctx).Type == EvalValue.Kind.Long
            && ExpressionEngine.Evaluate("int(\"42\")", ctx).LongVal == 42);
        AssertTrue("double(7)", ExpressionEngine.Evaluate("double(7)", ctx).Type == EvalValue.Kind.Double);
        AssertEqual("string(123)", ExpressionEngine.Evaluate("string(123)", ctx).AsString(), "123");
        AssertTrue("bool(1)", ExpressionEngine.Evaluate("bool(1)", ctx).BoolVal);
        AssertFalse("bool(0)", ExpressionEngine.Evaluate("bool(0)", ctx).BoolVal);
    }

    static void TestNullSemantics()
    {
        Console.WriteLine("--- Expression: null propagation ---");
        var ctx = new MapResolver(new() { ["missing"] = EvalValue.Null, ["x"] = EvalValue.From(10L) });
        AssertTrue("null + 5 = null", ExpressionEngine.Evaluate("missing + 5", ctx).Type == EvalValue.Kind.Null);
        AssertTrue("isNull(missing)", ExpressionEngine.Evaluate("isNull(missing)", ctx).BoolVal);
        AssertFalse("isNull(x)", ExpressionEngine.Evaluate("isNull(x)", ctx).BoolVal);
        AssertTrue("isEmpty(missing)", ExpressionEngine.Evaluate("isEmpty(missing)", ctx).BoolVal);
    }

    static void TestRecordResolver()
    {
        Console.WriteLine("--- Expression: RecordValueResolver ---");
        var schema = new Schema("order", [
            new Field("price", FieldType.Double),
            new Field("quantity", FieldType.Long),
            new Field("customer", FieldType.String)
        ]);
        var rec = new GenericRecord(schema);
        rec.SetField("price", 9.99);
        rec.SetField("quantity", 3L);
        rec.SetField("customer", "Alice");

        var ctx = new RecordValueResolver(rec);
        var total = ExpressionEngine.Evaluate("price * quantity", ctx);
        AssertTrue("total type is double", total.Type == EvalValue.Kind.Double);
        AssertTrue("total value", Math.Abs(total.DoubleVal - 29.97) < 0.001);

        var greet = ExpressionEngine.Evaluate("\"Hi \" + customer + \"!\"", ctx);
        AssertEqual("greeting from record", greet.AsString(), "Hi Alice!");
    }

    static void TestAttributeResolver()
    {
        Console.WriteLine("--- Expression: AttributeValueResolver ---");
        var attrs = new Dictionary<string, string>
        {
            ["filename"] = "data.csv",
            ["size"] = "1024"
        };
        var ctx = new AttributeValueResolver(attrs);
        AssertTrue("attr lookup", ExpressionEngine.Evaluate("endsWith(filename, \".csv\")", ctx).BoolVal);
        AssertTrue("attr to int math", ExpressionEngine.Evaluate("int(size) * 2 == 2048", ctx).BoolVal);
    }

    static void TestComplexExpressions()
    {
        Console.WriteLine("--- Expression: complex compositions ---");
        var ctx = new MapResolver(new()
        {
            ["price"] = EvalValue.From(100.0),
            ["discount"] = EvalValue.From(0.1),
            ["region"] = EvalValue.From("US")
        });

        // Tiered pricing: discount applies only in US
        var net = ExpressionEngine.Evaluate("if(region == \"US\", price * (1 - discount), price)", ctx);
        AssertTrue("tiered price US", Math.Abs(net.DoubleVal - 90.0) < 0.001);

        // Compose multiple operations
        var label = ExpressionEngine.Evaluate("upper(region) + \":\" + string(round(price))", ctx);
        AssertEqual("composed label", label.AsString(), "US:100");

        // Compiled reuse
        var compiled = ExpressionEngine.Compile("price > 50 && region == \"US\"");
        AssertTrue("compiled reuse 1", compiled.Eval(ctx).BoolVal);
        var ctx2 = new MapResolver(new() { ["price"] = EvalValue.From(20.0), ["region"] = EvalValue.From("US") });
        AssertFalse("compiled reuse 2", compiled.Eval(ctx2).BoolVal);
    }

    static void TestParseErrors()
    {
        Console.WriteLine("--- Expression: parse errors ---");
        var ctx = Empty();

        var threw = false;
        try { ExpressionEngine.Compile("("); }
        catch (FormatException) { threw = true; }
        AssertTrue("unmatched ( throws", threw);

        threw = false;
        try { ExpressionEngine.Compile("1 + )"); }
        catch (FormatException) { threw = true; }
        AssertTrue("unmatched ) throws", threw);

        threw = false;
        try { ExpressionEngine.Compile("\"unterminated"); }
        catch (FormatException) { threw = true; }
        AssertTrue("unterminated string throws", threw);

        threw = false;
        try { ExpressionEngine.Compile("a @ b"); }
        catch (FormatException) { threw = true; }
        AssertTrue("unknown char throws", threw);

        // Unknown function name → returns null at eval time, not parse time
        var unknown = ExpressionEngine.Evaluate("nope(1)", ctx);
        AssertTrue("unknown fn → null", unknown.Type == EvalValue.Kind.Null);
    }
}
