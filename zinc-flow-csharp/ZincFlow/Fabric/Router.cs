using System.Runtime.CompilerServices;
using ZincFlow.Core;

namespace ZincFlow.Fabric;

public enum Operator
{
    Eq, Neq, Contains, StartsWith, EndsWith, Exists, Gt, Lt
}

public sealed class RoutingRule
{
    public string Name { get; }
    public string Attribute { get; }
    public Operator Operator { get; }
    public string Value { get; }
    public string Destination { get; }
    public bool Enabled { get; set; }

    public RoutingRule(string name, string attribute, Operator op, string value, string destination, bool enabled = true)
    {
        Name = name;
        Attribute = attribute;
        Operator = op;
        Value = value;
        Destination = destination;
        Enabled = enabled;
    }
}

public sealed class RulesEngine
{
    private readonly Dictionary<string, List<RoutingRule>> _rulesets = new();

    public void AddOrReplaceRuleset(string name, List<RoutingRule> rules)
    {
        _rulesets[name] = rules;
    }

    // Hot path: zero-alloc — writes into caller's reusable buffer
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void GetDestinations(AttributeMap attributes, List<string> destinations)
    {
        destinations.Clear();
        foreach (var rules in _rulesets.Values)
        {
            foreach (var rule in rules)
            {
                if (!rule.Enabled) continue;
                if (Evaluate(rule, attributes))
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

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool Evaluate(RoutingRule rule, AttributeMap attributes)
    {
        if (rule.Operator == Operator.Exists)
            return attributes.ContainsKey(rule.Attribute);

        if (!attributes.TryGetValue(rule.Attribute, out var val))
            return false;

        return rule.Operator switch
        {
            Operator.Eq => val == rule.Value,
            Operator.Neq => val != rule.Value,
            Operator.Contains => val.Contains(rule.Value, StringComparison.Ordinal),
            Operator.StartsWith => val.StartsWith(rule.Value, StringComparison.Ordinal),
            Operator.EndsWith => val.EndsWith(rule.Value, StringComparison.Ordinal),
            Operator.Gt => string.Compare(val, rule.Value, StringComparison.Ordinal) > 0,
            Operator.Lt => string.Compare(val, rule.Value, StringComparison.Ordinal) < 0,
            _ => false
        };
    }
}
