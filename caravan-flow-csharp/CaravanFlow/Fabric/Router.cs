using System.Runtime.CompilerServices;
using CaravanFlow.Core;

namespace CaravanFlow.Fabric;

public enum Operator
{
    Eq, Neq, Contains, StartsWith, EndsWith, Exists, Gt, Lt
}

public enum Joiner { And, Or }

// --- Rule condition: base or composite (AND/OR) ---

public abstract class RuleCondition
{
    public abstract bool Evaluate(AttributeMap attributes);
}

public sealed class BaseRule : RuleCondition
{
    public string Attribute { get; }
    public Operator Operator { get; }
    public string Value { get; }

    public BaseRule(string attribute, Operator op, string value)
    {
        Attribute = attribute;
        Operator = op;
        Value = value;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override bool Evaluate(AttributeMap attributes)
    {
        if (Operator == Operator.Exists)
            return attributes.ContainsKey(Attribute);

        if (!attributes.TryGetValue(Attribute, out var val))
            return false;

        return Operator switch
        {
            Operator.Eq => val == Value,
            Operator.Neq => val != Value,
            Operator.Contains => val.Contains(Value, StringComparison.Ordinal),
            Operator.StartsWith => val.StartsWith(Value, StringComparison.Ordinal),
            Operator.EndsWith => val.EndsWith(Value, StringComparison.Ordinal),
            Operator.Gt => string.Compare(val, Value, StringComparison.Ordinal) > 0,
            Operator.Lt => string.Compare(val, Value, StringComparison.Ordinal) < 0,
            _ => false
        };
    }
}

public sealed class CompositeRule : RuleCondition
{
    public RuleCondition Left { get; }
    public Joiner Joiner { get; }
    public RuleCondition Right { get; }

    public CompositeRule(RuleCondition left, Joiner joiner, RuleCondition right)
    {
        Left = left;
        Joiner = joiner;
        Right = right;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override bool Evaluate(AttributeMap attributes)
    {
        return Joiner == Joiner.And
            ? Left.Evaluate(attributes) && Right.Evaluate(attributes)
            : Left.Evaluate(attributes) || Right.Evaluate(attributes);
    }
}

// --- Routing rule ---

public sealed class RoutingRule
{
    public string Name { get; }
    public RuleCondition Condition { get; }
    public string Destination { get; }
    public bool Enabled { get; set; }

    // Full constructor with condition object
    public RoutingRule(string name, RuleCondition condition, string destination, bool enabled = true)
    {
        Name = name;
        Condition = condition;
        Destination = destination;
        Enabled = enabled;
    }

    // Convenience: simple BaseRule (backward compatible)
    public RoutingRule(string name, string attribute, Operator op, string value, string destination, bool enabled = true)
        : this(name, new BaseRule(attribute, op, value), destination, enabled) { }
}

// --- Rules engine ---

public sealed class RulesEngine
{
    private readonly Dictionary<string, List<RoutingRule>> _rulesets = new();

    public void AddOrReplaceRuleset(string name, List<RoutingRule> rules)
    {
        _rulesets[name] = rules;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void GetDestinations(AttributeMap attributes, List<string> destinations)
    {
        destinations.Clear();
        foreach (var rules in _rulesets.Values)
        {
            foreach (var rule in rules)
            {
                if (!rule.Enabled) continue;
                if (rule.Condition.Evaluate(attributes))
                    destinations.Add(rule.Destination);
            }
        }
    }

    public List<RoutingRule> GetAllRules()
    {
        var result = new List<RoutingRule>();
        foreach (var rules in _rulesets.Values)
            result.AddRange(rules);
        return result;
    }

    public List<string> GetRulesetNames() => new(_rulesets.Keys);

    public void ToggleRule(string ruleset, string ruleName)
    {
        if (_rulesets.TryGetValue(ruleset, out var rules))
        {
            foreach (var rule in rules)
            {
                if (rule.Name == ruleName)
                {
                    rule.Enabled = !rule.Enabled;
                    return;
                }
            }
        }
    }

    public bool RemoveRuleset(string name) => _rulesets.Remove(name);
}
