using System.Globalization;
using System.Text;

namespace ZincFlow.StdLib;

/// <summary>
/// Tagged value type — preserves type information through expression evaluation.
/// Avoids object boxing on the hot path; the consumer reads Type and the matching field.
/// </summary>
public readonly struct EvalValue : IEquatable<EvalValue>
{
    public enum Kind { Null, Bool, Long, Double, String }

    public Kind Type { get; }
    public long LongVal { get; }
    public double DoubleVal { get; }
    public string? StringVal { get; }
    public bool BoolVal { get; }

    private EvalValue(Kind t, long l, double d, string? s, bool b)
    { Type = t; LongVal = l; DoubleVal = d; StringVal = s; BoolVal = b; }

    public static readonly EvalValue Null = new(Kind.Null, 0, 0, null, false);
    public static EvalValue From(bool v) => new(Kind.Bool, 0, 0, null, v);
    public static EvalValue From(long v) => new(Kind.Long, v, 0, null, false);
    public static EvalValue From(int v) => new(Kind.Long, v, 0, null, false);
    public static EvalValue From(double v) => new(Kind.Double, 0, v, null, false);
    public static EvalValue From(float v) => new(Kind.Double, 0, v, null, false);
    public static EvalValue From(string? v) => v is null ? Null : new(Kind.String, 0, 0, v, false);

    /// <summary>Wraps an arbitrary CLR object (typically a record field value) into an EvalValue.</summary>
    public static EvalValue FromObject(object? v) => v switch
    {
        null => Null,
        bool b => From(b),
        int i => From(i),
        long l => From(l),
        short sh => From(sh),
        byte by => From(by),
        float f => From(f),
        double d => From(d),
        decimal m => From((double)m),
        string s => From(s),
        _ => From(v.ToString())
    };

    /// <summary>Unwraps to a CLR object suitable for storing back into a record field.</summary>
    public object? ToObject() => Type switch
    {
        Kind.Null => null,
        Kind.Bool => BoolVal,
        Kind.Long => LongVal,
        Kind.Double => DoubleVal,
        Kind.String => StringVal,
        _ => null
    };

    public bool IsNumeric => Type == Kind.Long || Type == Kind.Double;
    public bool IsTruthy => Type switch
    {
        Kind.Null => false,
        Kind.Bool => BoolVal,
        Kind.Long => LongVal != 0,
        Kind.Double => DoubleVal != 0.0 && !double.IsNaN(DoubleVal),
        Kind.String => !string.IsNullOrEmpty(StringVal),
        _ => false
    };

    public double AsDouble() => Type switch
    {
        Kind.Long => LongVal,
        Kind.Double => DoubleVal,
        Kind.Bool => BoolVal ? 1.0 : 0.0,
        Kind.String => double.TryParse(StringVal, NumberStyles.Any, CultureInfo.InvariantCulture, out var d) ? d : double.NaN,
        _ => double.NaN
    };

    public long AsLong() => Type switch
    {
        Kind.Long => LongVal,
        Kind.Double => (long)DoubleVal,
        Kind.Bool => BoolVal ? 1 : 0,
        Kind.String => long.TryParse(StringVal, NumberStyles.Any, CultureInfo.InvariantCulture, out var l) ? l : 0,
        _ => 0
    };

    public string AsString() => Type switch
    {
        Kind.Null => "",
        Kind.Bool => BoolVal ? "true" : "false",
        Kind.Long => LongVal.ToString(CultureInfo.InvariantCulture),
        Kind.Double => DoubleVal.ToString("R", CultureInfo.InvariantCulture),
        Kind.String => StringVal ?? "",
        _ => ""
    };

    public bool Equals(EvalValue other)
    {
        if (Type != other.Type) return false;
        return Type switch
        {
            Kind.Null => true,
            Kind.Bool => BoolVal == other.BoolVal,
            Kind.Long => LongVal == other.LongVal,
            Kind.Double => DoubleVal == other.DoubleVal,
            Kind.String => StringVal == other.StringVal,
            _ => false
        };
    }

    public override bool Equals(object? obj) => obj is EvalValue v && Equals(v);
    public override int GetHashCode() => HashCode.Combine((int)Type, LongVal, DoubleVal, StringVal, BoolVal);
    public override string ToString() => $"{Type}:{AsString()}";
}

/// <summary>
/// Resolves identifier references in an expression to values. Implementations bind
/// expressions to a context — record fields, FlowFile attributes, or any other map.
/// </summary>
public interface IValueResolver
{
    EvalValue Resolve(string name);
}

internal enum TokenKind : byte
{
    Number, String, Bool, Null, Ident,
    LParen, RParen, Comma,
    Plus, Minus, Star, Slash, Percent,
    Eq, Neq, Lt, Gt, Le, Ge,
    And, Or, Not,
    UnaryMinus, Function
}

internal readonly struct Token
{
    public readonly TokenKind Kind;
    public readonly string? Text;
    public readonly long L;
    public readonly double D;
    public readonly bool B;
    public readonly bool IsLong;   // disambiguates Number literals
    public readonly int Arity;     // for Function tokens

    public Token(TokenKind k, string? text = null, long l = 0, double d = 0, bool b = false, bool isLong = false, int arity = 0)
    { Kind = k; Text = text; L = l; D = d; B = b; IsLong = isLong; Arity = arity; }
}

/// <summary>
/// Hand-rolled expression parser + evaluator. No reflection, AOT-safe.
///
/// Grammar (precedence low → high):
///   logicalOr  := logicalAnd  ('||' logicalAnd)*
///   logicalAnd := equality    ('&amp;&amp;' equality)*
///   equality   := comparison  (('=='|'!=') comparison)*
///   comparison := additive    (('&lt;'|'&gt;'|'&lt;='|'&gt;=') additive)*
///   additive   := multiplicative (('+'|'-') multiplicative)*
///   multiplicative := unary (('*'|'/'|'%') unary)*
///   unary      := ('-'|'!') unary | primary
///   primary    := Number | String | Bool | Null | Ident | Function | '(' logicalOr ')'
///   Function   := Ident '(' (logicalOr (',' logicalOr)*)? ')'
///
/// Type rules:
///   long  + long  → long
///   double + any number → double
///   string + any → string concat
///   division by zero → 0 for long, NaN for double (not thrown — data-engineering friendly)
///   missing identifier → EvalValue.Null
/// </summary>
public sealed class CompiledExpression
{
    private readonly Token[] _rpn;
    internal CompiledExpression(Token[] rpn) => _rpn = rpn;

    public EvalValue Eval(IValueResolver context)
    {
        var stack = new Stack<EvalValue>(_rpn.Length);

        foreach (var tok in _rpn)
        {
            switch (tok.Kind)
            {
                case TokenKind.Number:
                    stack.Push(tok.IsLong ? EvalValue.From(tok.L) : EvalValue.From(tok.D));
                    break;
                case TokenKind.String:
                    stack.Push(EvalValue.From(tok.Text ?? ""));
                    break;
                case TokenKind.Bool:
                    stack.Push(EvalValue.From(tok.B));
                    break;
                case TokenKind.Null:
                    stack.Push(EvalValue.Null);
                    break;
                case TokenKind.Ident:
                    stack.Push(context.Resolve(tok.Text ?? ""));
                    break;
                case TokenKind.UnaryMinus:
                    stack.Push(Negate(stack.Pop()));
                    break;
                case TokenKind.Not:
                    stack.Push(EvalValue.From(!stack.Pop().IsTruthy));
                    break;
                case TokenKind.Plus: ApplyAdd(stack); break;
                case TokenKind.Minus: ApplyArith(stack, '-'); break;
                case TokenKind.Star: ApplyArith(stack, '*'); break;
                case TokenKind.Slash: ApplyArith(stack, '/'); break;
                case TokenKind.Percent: ApplyArith(stack, '%'); break;
                case TokenKind.Eq: ApplyComparison(stack, op: 0); break;
                case TokenKind.Neq: ApplyComparison(stack, op: 1); break;
                case TokenKind.Lt: ApplyComparison(stack, op: 2); break;
                case TokenKind.Gt: ApplyComparison(stack, op: 3); break;
                case TokenKind.Le: ApplyComparison(stack, op: 4); break;
                case TokenKind.Ge: ApplyComparison(stack, op: 5); break;
                case TokenKind.And:
                {
                    var b = stack.Pop(); var a = stack.Pop();
                    stack.Push(EvalValue.From(a.IsTruthy && b.IsTruthy));
                    break;
                }
                case TokenKind.Or:
                {
                    var b = stack.Pop(); var a = stack.Pop();
                    stack.Push(EvalValue.From(a.IsTruthy || b.IsTruthy));
                    break;
                }
                case TokenKind.Function:
                    stack.Push(CallFunction(tok.Text ?? "", PopArgs(stack, tok.Arity)));
                    break;
            }
        }

        return stack.Count > 0 ? stack.Pop() : EvalValue.Null;
    }

    private static EvalValue[] PopArgs(Stack<EvalValue> stack, int arity)
    {
        var args = new EvalValue[arity];
        for (int i = arity - 1; i >= 0; i--) args[i] = stack.Pop();
        return args;
    }

    private static EvalValue Negate(EvalValue v) => v.Type switch
    {
        EvalValue.Kind.Long => EvalValue.From(-v.LongVal),
        EvalValue.Kind.Double => EvalValue.From(-v.DoubleVal),
        EvalValue.Kind.String => double.TryParse(v.StringVal, NumberStyles.Any, CultureInfo.InvariantCulture, out var d) ? EvalValue.From(-d) : EvalValue.Null,
        _ => EvalValue.Null
    };

    private static void ApplyAdd(Stack<EvalValue> stack)
    {
        var b = stack.Pop(); var a = stack.Pop();
        // String concat if either operand is a string
        if (a.Type == EvalValue.Kind.String || b.Type == EvalValue.Kind.String)
        {
            stack.Push(EvalValue.From(a.AsString() + b.AsString()));
            return;
        }
        if (a.Type == EvalValue.Kind.Null || b.Type == EvalValue.Kind.Null)
        {
            stack.Push(EvalValue.Null);
            return;
        }
        if (a.Type == EvalValue.Kind.Double || b.Type == EvalValue.Kind.Double)
            stack.Push(EvalValue.From(a.AsDouble() + b.AsDouble()));
        else
            stack.Push(EvalValue.From(a.AsLong() + b.AsLong()));
    }

    private static void ApplyArith(Stack<EvalValue> stack, char op)
    {
        var b = stack.Pop(); var a = stack.Pop();
        if (a.Type == EvalValue.Kind.Null || b.Type == EvalValue.Kind.Null)
        {
            stack.Push(EvalValue.Null);
            return;
        }
        bool useDouble = a.Type == EvalValue.Kind.Double || b.Type == EvalValue.Kind.Double
            || (a.Type == EvalValue.Kind.String && a.AsDouble() % 1 != 0)
            || (b.Type == EvalValue.Kind.String && b.AsDouble() % 1 != 0);

        if (useDouble)
        {
            double da = a.AsDouble(), db = b.AsDouble();
            stack.Push(op switch
            {
                '-' => EvalValue.From(da - db),
                '*' => EvalValue.From(da * db),
                '/' => EvalValue.From(db == 0 ? double.NaN : da / db),
                '%' => EvalValue.From(db == 0 ? double.NaN : da % db),
                _ => EvalValue.Null
            });
        }
        else
        {
            long la = a.AsLong(), lb = b.AsLong();
            stack.Push(op switch
            {
                '-' => EvalValue.From(la - lb),
                '*' => EvalValue.From(la * lb),
                '/' => EvalValue.From(lb == 0 ? 0L : la / lb),
                '%' => EvalValue.From(lb == 0 ? 0L : la % lb),
                _ => EvalValue.Null
            });
        }
    }

    private static void ApplyComparison(Stack<EvalValue> stack, int op)
    {
        var b = stack.Pop(); var a = stack.Pop();
        bool result;

        // Equality / inequality use Equals semantics — types must match (or null check)
        if (op == 0 || op == 1)
        {
            // Allow numeric equality across long/double
            if (a.IsNumeric && b.IsNumeric)
                result = a.AsDouble() == b.AsDouble();
            else
                result = a.Equals(b);
            if (op == 1) result = !result;
            stack.Push(EvalValue.From(result));
            return;
        }

        // Ordered comparisons: numeric or string-lex
        if (a.Type == EvalValue.Kind.Null || b.Type == EvalValue.Kind.Null)
        {
            stack.Push(EvalValue.From(false));
            return;
        }

        int cmp;
        if (a.Type == EvalValue.Kind.String && b.Type == EvalValue.Kind.String)
            cmp = string.CompareOrdinal(a.StringVal, b.StringVal);
        else
            cmp = a.AsDouble().CompareTo(b.AsDouble());

        result = op switch
        {
            2 => cmp < 0,
            3 => cmp > 0,
            4 => cmp <= 0,
            5 => cmp >= 0,
            _ => false
        };
        stack.Push(EvalValue.From(result));
    }

    private static EvalValue CallFunction(string name, EvalValue[] args)
    {
        switch (name)
        {
            // String functions
            case "upper": return args.Length >= 1 ? EvalValue.From(args[0].AsString().ToUpperInvariant()) : EvalValue.Null;
            case "lower": return args.Length >= 1 ? EvalValue.From(args[0].AsString().ToLowerInvariant()) : EvalValue.Null;
            case "trim": return args.Length >= 1 ? EvalValue.From(args[0].AsString().Trim()) : EvalValue.Null;
            case "length":
                if (args.Length < 1) return EvalValue.From(0L);
                return args[0].Type == EvalValue.Kind.String
                    ? EvalValue.From((long)(args[0].StringVal?.Length ?? 0))
                    : EvalValue.From((long)args[0].AsString().Length);
            case "substring":
            {
                if (args.Length < 2) return EvalValue.Null;
                var s = args[0].AsString();
                int start = (int)args[1].AsLong();
                if (start < 0) start = 0;
                if (start > s.Length) start = s.Length;
                if (args.Length >= 3)
                {
                    int len = (int)args[2].AsLong();
                    if (len < 0) len = 0;
                    if (start + len > s.Length) len = s.Length - start;
                    return EvalValue.From(s.Substring(start, len));
                }
                return EvalValue.From(s.Substring(start));
            }
            case "replace":
                return args.Length >= 3
                    ? EvalValue.From(args[0].AsString().Replace(args[1].AsString(), args[2].AsString()))
                    : EvalValue.Null;
            case "concat":
            {
                var sb = new StringBuilder();
                foreach (var a in args) sb.Append(a.AsString());
                return EvalValue.From(sb.ToString());
            }
            case "contains":
                return args.Length >= 2 ? EvalValue.From(args[0].AsString().Contains(args[1].AsString())) : EvalValue.From(false);
            case "startsWith":
                return args.Length >= 2 ? EvalValue.From(args[0].AsString().StartsWith(args[1].AsString())) : EvalValue.From(false);
            case "endsWith":
                return args.Length >= 2 ? EvalValue.From(args[0].AsString().EndsWith(args[1].AsString())) : EvalValue.From(false);

            // Coalesce: first non-null, non-empty argument
            case "coalesce":
                foreach (var a in args)
                    if (a.Type != EvalValue.Kind.Null && !(a.Type == EvalValue.Kind.String && string.IsNullOrEmpty(a.StringVal)))
                        return a;
                return EvalValue.Null;

            // Conditional: if(cond, then, else)
            case "if":
                if (args.Length < 2) return EvalValue.Null;
                if (args[0].IsTruthy) return args[1];
                return args.Length >= 3 ? args[2] : EvalValue.Null;

            // Type casts — explicit, preserve target type
            case "int":
            case "long": return args.Length >= 1 ? EvalValue.From(args[0].AsLong()) : EvalValue.Null;
            case "double": return args.Length >= 1 ? EvalValue.From(args[0].AsDouble()) : EvalValue.Null;
            case "string": return args.Length >= 1 ? EvalValue.From(args[0].AsString()) : EvalValue.Null;
            case "bool": return args.Length >= 1 ? EvalValue.From(args[0].IsTruthy) : EvalValue.From(false);

            // Math
            case "abs":
                if (args.Length < 1) return EvalValue.Null;
                return args[0].Type == EvalValue.Kind.Long
                    ? EvalValue.From(Math.Abs(args[0].LongVal))
                    : EvalValue.From(Math.Abs(args[0].AsDouble()));
            case "min":
                return args.Length >= 2 ? (args[0].AsDouble() <= args[1].AsDouble() ? args[0] : args[1]) : EvalValue.Null;
            case "max":
                return args.Length >= 2 ? (args[0].AsDouble() >= args[1].AsDouble() ? args[0] : args[1]) : EvalValue.Null;
            case "floor": return args.Length >= 1 ? EvalValue.From((long)Math.Floor(args[0].AsDouble())) : EvalValue.Null;
            case "ceil": return args.Length >= 1 ? EvalValue.From((long)Math.Ceiling(args[0].AsDouble())) : EvalValue.Null;
            case "round": return args.Length >= 1 ? EvalValue.From((long)Math.Round(args[0].AsDouble(), MidpointRounding.AwayFromZero)) : EvalValue.Null;
            case "pow": return args.Length >= 2 ? EvalValue.From(Math.Pow(args[0].AsDouble(), args[1].AsDouble())) : EvalValue.Null;
            case "sqrt": return args.Length >= 1 ? EvalValue.From(Math.Sqrt(args[0].AsDouble())) : EvalValue.Null;

            // Null / missing checks
            case "isNull": return args.Length >= 1 ? EvalValue.From(args[0].Type == EvalValue.Kind.Null) : EvalValue.From(true);
            case "isEmpty":
                if (args.Length < 1) return EvalValue.From(true);
                return args[0].Type == EvalValue.Kind.Null
                    ? EvalValue.From(true)
                    : EvalValue.From(string.IsNullOrEmpty(args[0].AsString()));

            default:
                return EvalValue.Null;
        }
    }
}

public static class ExpressionEngine
{
    /// <summary>Compile + evaluate in one shot. Use <see cref="Compile"/> to amortize parsing.</summary>
    public static EvalValue Evaluate(string expression, IValueResolver context)
        => Compile(expression).Eval(context);

    public static CompiledExpression Compile(string expression)
    {
        var tokens = Tokenize(expression);
        var rpn = ToRpn(tokens);
        return new CompiledExpression(rpn);
    }

    // --- Tokenizer ---

    private static List<Token> Tokenize(string src)
    {
        var tokens = new List<Token>();
        int i = 0;
        while (i < src.Length)
        {
            char c = src[i];
            if (char.IsWhiteSpace(c)) { i++; continue; }

            if (c >= '0' && c <= '9') { i = ReadNumber(src, i, tokens); continue; }
            if (c == '"' || c == '\'') { i = ReadString(src, i, tokens); continue; }
            if (c == '_' || char.IsLetter(c)) { i = ReadIdent(src, i, tokens); continue; }

            switch (c)
            {
                case '(': tokens.Add(new Token(TokenKind.LParen)); i++; break;
                case ')': tokens.Add(new Token(TokenKind.RParen)); i++; break;
                case ',': tokens.Add(new Token(TokenKind.Comma)); i++; break;
                case '+': tokens.Add(new Token(TokenKind.Plus)); i++; break;
                case '-': tokens.Add(new Token(TokenKind.Minus)); i++; break;
                case '*': tokens.Add(new Token(TokenKind.Star)); i++; break;
                case '/': tokens.Add(new Token(TokenKind.Slash)); i++; break;
                case '%': tokens.Add(new Token(TokenKind.Percent)); i++; break;
                case '=':
                    if (i + 1 < src.Length && src[i + 1] == '=') { tokens.Add(new Token(TokenKind.Eq)); i += 2; }
                    else throw new FormatException($"unexpected '=' at position {i} (use '==' for equality)");
                    break;
                case '!':
                    if (i + 1 < src.Length && src[i + 1] == '=') { tokens.Add(new Token(TokenKind.Neq)); i += 2; }
                    else { tokens.Add(new Token(TokenKind.Not)); i++; }
                    break;
                case '<':
                    if (i + 1 < src.Length && src[i + 1] == '=') { tokens.Add(new Token(TokenKind.Le)); i += 2; }
                    else { tokens.Add(new Token(TokenKind.Lt)); i++; }
                    break;
                case '>':
                    if (i + 1 < src.Length && src[i + 1] == '=') { tokens.Add(new Token(TokenKind.Ge)); i += 2; }
                    else { tokens.Add(new Token(TokenKind.Gt)); i++; }
                    break;
                case '&':
                    if (i + 1 < src.Length && src[i + 1] == '&') { tokens.Add(new Token(TokenKind.And)); i += 2; }
                    else throw new FormatException($"unexpected '&' at position {i} (use '&&' for logical and)");
                    break;
                case '|':
                    if (i + 1 < src.Length && src[i + 1] == '|') { tokens.Add(new Token(TokenKind.Or)); i += 2; }
                    else throw new FormatException($"unexpected '|' at position {i} (use '||' for logical or)");
                    break;
                default:
                    throw new FormatException($"unexpected character '{c}' at position {i}");
            }
        }
        return tokens;
    }

    private static int ReadNumber(string src, int start, List<Token> tokens)
    {
        int i = start;
        bool isDouble = false;
        while (i < src.Length && (src[i] >= '0' && src[i] <= '9')) i++;
        if (i < src.Length && src[i] == '.')
        {
            isDouble = true;
            i++;
            while (i < src.Length && (src[i] >= '0' && src[i] <= '9')) i++;
        }
        if (i < src.Length && (src[i] == 'e' || src[i] == 'E'))
        {
            isDouble = true;
            i++;
            if (i < src.Length && (src[i] == '+' || src[i] == '-')) i++;
            while (i < src.Length && (src[i] >= '0' && src[i] <= '9')) i++;
        }

        var text = src.Substring(start, i - start);
        if (isDouble)
            tokens.Add(new Token(TokenKind.Number, d: double.Parse(text, NumberStyles.Any, CultureInfo.InvariantCulture), isLong: false));
        else
            tokens.Add(new Token(TokenKind.Number, l: long.Parse(text, NumberStyles.Any, CultureInfo.InvariantCulture), isLong: true));
        return i;
    }

    private static int ReadString(string src, int start, List<Token> tokens)
    {
        char quote = src[start];
        int i = start + 1;
        var sb = new StringBuilder();
        while (i < src.Length && src[i] != quote)
        {
            if (src[i] == '\\' && i + 1 < src.Length)
            {
                char esc = src[i + 1];
                sb.Append(esc switch
                {
                    'n' => '\n',
                    't' => '\t',
                    'r' => '\r',
                    '\\' => '\\',
                    '"' => '"',
                    '\'' => '\'',
                    _ => esc
                });
                i += 2;
            }
            else
            {
                sb.Append(src[i]);
                i++;
            }
        }
        if (i >= src.Length)
            throw new FormatException($"unterminated string literal starting at {start}");
        tokens.Add(new Token(TokenKind.String, text: sb.ToString()));
        return i + 1;
    }

    private static int ReadIdent(string src, int start, List<Token> tokens)
    {
        int i = start;
        while (i < src.Length && (char.IsLetterOrDigit(src[i]) || src[i] == '_' || src[i] == '.')) i++;
        var name = src.Substring(start, i - start);
        switch (name)
        {
            case "true": tokens.Add(new Token(TokenKind.Bool, b: true)); break;
            case "false": tokens.Add(new Token(TokenKind.Bool, b: false)); break;
            case "null": tokens.Add(new Token(TokenKind.Null)); break;
            default: tokens.Add(new Token(TokenKind.Ident, text: name)); break;
        }
        return i;
    }

    // --- Shunting-yard: infix tokens → RPN ---

    private static int Precedence(TokenKind k) => k switch
    {
        TokenKind.Or => 1,
        TokenKind.And => 2,
        TokenKind.Eq or TokenKind.Neq => 3,
        TokenKind.Lt or TokenKind.Gt or TokenKind.Le or TokenKind.Ge => 4,
        TokenKind.Plus or TokenKind.Minus => 5,
        TokenKind.Star or TokenKind.Slash or TokenKind.Percent => 6,
        TokenKind.UnaryMinus or TokenKind.Not => 7,
        _ => 0
    };

    private static bool IsRightAssoc(TokenKind k) => k == TokenKind.UnaryMinus || k == TokenKind.Not;

    private static Token[] ToRpn(List<Token> tokens)
    {
        var output = new List<Token>(tokens.Count);
        var ops = new Stack<Token>();
        var arity = new Stack<int>();      // current argument count per active function
        var sawValue = new Stack<bool>();  // tracks whether the current call has at least one arg

        TokenKind? prev = null;
        for (int idx = 0; idx < tokens.Count; idx++)
        {
            var t = tokens[idx];

            switch (t.Kind)
            {
                case TokenKind.Number:
                case TokenKind.String:
                case TokenKind.Bool:
                case TokenKind.Null:
                    output.Add(t);
                    if (sawValue.Count > 0) { sawValue.Pop(); sawValue.Push(true); }
                    break;

                case TokenKind.Ident:
                    // Function call if followed by '('
                    if (idx + 1 < tokens.Count && tokens[idx + 1].Kind == TokenKind.LParen)
                    {
                        ops.Push(new Token(TokenKind.Function, text: t.Text));
                        ops.Push(new Token(TokenKind.LParen));
                        arity.Push(0);
                        sawValue.Push(false);
                        idx++;                       // consume '('
                        prev = TokenKind.LParen;     // record the actual last-consumed token
                        continue;                    // skip the prev = t.Kind at the bottom
                    }
                    output.Add(t);
                    if (sawValue.Count > 0) { sawValue.Pop(); sawValue.Push(true); }
                    break;

                case TokenKind.Comma:
                    while (ops.Count > 0 && ops.Peek().Kind != TokenKind.LParen)
                        output.Add(ops.Pop());
                    if (arity.Count == 0)
                        throw new FormatException("comma outside function call");
                    if (sawValue.Count > 0 && sawValue.Pop())
                    {
                        arity.Push(arity.Pop() + 1);
                    }
                    sawValue.Push(false);
                    break;

                case TokenKind.LParen:
                    ops.Push(t);
                    break;

                case TokenKind.RParen:
                    while (ops.Count > 0 && ops.Peek().Kind != TokenKind.LParen)
                        output.Add(ops.Pop());
                    if (ops.Count == 0)
                        throw new FormatException("mismatched parentheses");
                    ops.Pop(); // discard LParen
                    // If the operator now on top is a Function, finalise its arity and emit.
                    if (ops.Count > 0 && ops.Peek().Kind == TokenKind.Function)
                    {
                        var fnTok = ops.Pop();
                        int arityCount = arity.Pop();
                        if (sawValue.Count > 0 && sawValue.Pop()) arityCount++;
                        output.Add(new Token(TokenKind.Function, text: fnTok.Text, arity: arityCount));
                        if (sawValue.Count > 0) { sawValue.Pop(); sawValue.Push(true); }
                    }
                    break;

                case TokenKind.Plus:
                case TokenKind.Minus:
                case TokenKind.Star:
                case TokenKind.Slash:
                case TokenKind.Percent:
                case TokenKind.Eq:
                case TokenKind.Neq:
                case TokenKind.Lt:
                case TokenKind.Gt:
                case TokenKind.Le:
                case TokenKind.Ge:
                case TokenKind.And:
                case TokenKind.Or:
                case TokenKind.Not:
                {
                    var opTok = t;
                    // Unary minus detection: minus at expression start, after operator, or after '(' or ','
                    if (t.Kind == TokenKind.Minus && (prev is null
                        || prev == TokenKind.LParen
                        || prev == TokenKind.Comma
                        || IsOperator(prev.Value)))
                    {
                        opTok = new Token(TokenKind.UnaryMinus);
                    }

                    while (ops.Count > 0)
                    {
                        var top = ops.Peek();
                        if (top.Kind == TokenKind.LParen || top.Kind == TokenKind.Function) break;
                        int topPrec = Precedence(top.Kind);
                        int curPrec = Precedence(opTok.Kind);
                        if (topPrec > curPrec || (topPrec == curPrec && !IsRightAssoc(opTok.Kind)))
                            output.Add(ops.Pop());
                        else
                            break;
                    }
                    ops.Push(opTok);
                    break;
                }
            }
            prev = t.Kind;
        }

        while (ops.Count > 0)
        {
            var op = ops.Pop();
            if (op.Kind == TokenKind.LParen || op.Kind == TokenKind.RParen)
                throw new FormatException("mismatched parentheses");
            output.Add(op);
        }

        return output.ToArray();
    }

    private static bool IsOperator(TokenKind k) => k is
        TokenKind.Plus or TokenKind.Minus or TokenKind.Star or TokenKind.Slash or TokenKind.Percent
        or TokenKind.Eq or TokenKind.Neq or TokenKind.Lt or TokenKind.Gt or TokenKind.Le or TokenKind.Ge
        or TokenKind.And or TokenKind.Or or TokenKind.Not
        or TokenKind.UnaryMinus or TokenKind.Comma;
}

// --- Built-in resolvers ---

/// <summary>Resolves identifiers against a record's fields. Supports dotted paths.</summary>
public sealed class RecordValueResolver : IValueResolver
{
    private readonly ZincFlow.Core.GenericRecord _record;
    public RecordValueResolver(ZincFlow.Core.GenericRecord record) => _record = record;
    public EvalValue Resolve(string name) => EvalValue.FromObject(ZincFlow.Core.RecordHelpers.GetByPath(_record, name));
}

/// <summary>Resolves identifiers against a FlowFile attribute map.</summary>
public sealed class AttributeValueResolver : IValueResolver
{
    private readonly IReadOnlyDictionary<string, string> _attrs;
    public AttributeValueResolver(IReadOnlyDictionary<string, string> attrs) => _attrs = attrs;
    public EvalValue Resolve(string name)
        => _attrs.TryGetValue(name, out var v) ? EvalValue.From(v) : EvalValue.Null;
}

/// <summary>
/// Resolves identifiers against a mutable dictionary. Used inside processors that
/// apply a sequence of operations to a record's field map and need each step to
/// observe the prior step's writes. Supports dotted paths walking nested
/// GenericRecord and Dictionary&lt;string, object?&gt; values.
/// </summary>
public sealed class DictValueResolver : IValueResolver
{
    private readonly IReadOnlyDictionary<string, object?> _dict;
    public DictValueResolver(IReadOnlyDictionary<string, object?> dict) => _dict = dict;

    public EvalValue Resolve(string name)
    {
        if (!name.Contains('.'))
            return _dict.TryGetValue(name, out var v) ? EvalValue.FromObject(v) : EvalValue.Null;

        var parts = name.Split('.');
        if (!_dict.TryGetValue(parts[0], out var cur)) return EvalValue.Null;
        for (int i = 1; i < parts.Length; i++)
        {
            switch (cur)
            {
                case null: return EvalValue.Null;
                case ZincFlow.Core.GenericRecord gr: cur = gr.GetField(parts[i]); break;
                case IDictionary<string, object?> d:
                    cur = d.TryGetValue(parts[i], out var v) ? v : null;
                    break;
                default: return EvalValue.Null;
            }
        }
        return EvalValue.FromObject(cur);
    }
}
