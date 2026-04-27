using System.Text;
using System.Text.RegularExpressions;
using ZincFlow.Core;

namespace ZincFlow.StdLib;

/// <summary>
/// ReplaceText: regex find/replace on raw content bytes.
/// Config: pattern, replacement, mode (all|first).
/// </summary>
public sealed class ReplaceText : IProcessor
{
    private readonly Regex _regex;
    private readonly string _replacement;
    private readonly bool _replaceAll;
    private readonly IContentStore _store;

    public ReplaceText(string pattern, string replacement, string mode, IContentStore store)
    {
        _regex = new Regex(pattern, RegexOptions.Compiled);
        _replacement = replacement;
        _replaceAll = mode != "first";
        _store = store;
    }

    public ProcessorResult Process(FlowFile ff)
    {
        var (data, error) = ContentHelpers.Resolve(_store, ff.Content);
        if (error != "") return FailureResult.Rent(error, ff);

        var text = Encoding.UTF8.GetString(data);
        var result = _replaceAll
            ? _regex.Replace(text, _replacement)
            : _regex.Replace(text, _replacement, 1);

        var outBytes = Encoding.UTF8.GetBytes(result);
        var updated = FlowFile.WithContent(ff, Raw.Rent(outBytes));
        return SingleResult.Rent(updated);
    }
}

/// <summary>
/// ExtractText: regex capture groups from content → FlowFile attributes.
/// Config: pattern (with named or positional groups), groupNames (comma-separated attribute names for positional groups).
/// Named groups (?&lt;name&gt;...) automatically become attributes.
/// </summary>
public sealed class ExtractText : IProcessor
{
    private readonly Regex _regex;
    private readonly string[] _groupNames;
    private readonly IContentStore _store;

    public ExtractText(string pattern, string groupNames, IContentStore store)
    {
        _regex = new Regex(pattern, RegexOptions.Compiled);
        _groupNames = string.IsNullOrEmpty(groupNames)
            ? []
            : groupNames.Split(',', StringSplitOptions.TrimEntries);
        _store = store;
    }

    public ProcessorResult Process(FlowFile ff)
    {
        var (data, error) = ContentHelpers.Resolve(_store, ff.Content);
        if (error != "") return FailureResult.Rent(error, ff);

        var text = Encoding.UTF8.GetString(data);
        var match = _regex.Match(text);
        if (!match.Success)
            return SingleResult.Rent(ff);

        var result = ff;

        // Named groups → attributes
        foreach (var name in _regex.GetGroupNames())
        {
            if (int.TryParse(name, out _)) continue; // skip numeric groups
            var group = match.Groups[name];
            if (group.Success)
                result = FlowFile.WithAttribute(result, name, group.Value);
        }

        // Positional groups → mapped via groupNames config
        for (int i = 0; i < _groupNames.Length && i + 1 < match.Groups.Count; i++)
        {
            var group = match.Groups[i + 1]; // group 0 is full match
            if (group.Success && !string.IsNullOrEmpty(_groupNames[i]))
                result = FlowFile.WithAttribute(result, _groupNames[i], group.Value);
        }

        return SingleResult.Rent(result);
    }
}

/// <summary>
/// SplitText: split raw content by delimiter into multiple FlowFiles.
/// Config: delimiter (regex), headerLines (count of header lines to repeat in each split).
/// Returns MultipleResult.
/// </summary>
public sealed class SplitText : IProcessor
{
    private readonly Regex _delimiter;
    private readonly int _headerLines;
    private readonly IContentStore _store;

    public SplitText(string delimiter, int headerLines, IContentStore store)
    {
        _delimiter = new Regex(delimiter, RegexOptions.Compiled);
        _headerLines = headerLines;
        _store = store;
    }

    public ProcessorResult Process(FlowFile ff)
    {
        var (data, error) = ContentHelpers.Resolve(_store, ff.Content);
        if (error != "") return FailureResult.Rent(error, ff);

        var text = Encoding.UTF8.GetString(data);
        var parts = _delimiter.Split(text);

        if (parts.Length <= 1)
            return SingleResult.Rent(ff);

        // Extract header lines if configured
        string header = "";
        int dataStart = 0;
        if (_headerLines > 0)
        {
            var lines = text.Split('\n');
            if (lines.Length > _headerLines)
            {
                header = string.Join('\n', lines.Take(_headerLines)) + "\n";
                // Re-split remaining content
                var remaining = string.Join('\n', lines.Skip(_headerLines));
                parts = _delimiter.Split(remaining);
                dataStart = 0;
            }
        }

        var result = MultipleResult.Rent();
        for (int i = dataStart; i < parts.Length; i++)
        {
            if (string.IsNullOrWhiteSpace(parts[i])) continue;
            var content = header + parts[i];
            var splitFf = FlowFile.Create(
                Encoding.UTF8.GetBytes(content),
                new Dictionary<string, string>
                {
                    ["split.index"] = i.ToString(),
                    ["split.count"] = parts.Length.ToString()
                });
            result.FlowFiles.Add(splitFf);
        }

        if (result.FlowFiles.Count == 0)
            return SingleResult.Rent(ff);

        return result;
    }
}
