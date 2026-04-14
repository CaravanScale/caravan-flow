using System.Globalization;

namespace ZincFlow.Core;

/// <summary>
/// Small helpers for reading YAML config values without the silent-
/// fallback trap that <c>int.TryParse(..., out var n) ? n : fallback</c>
/// bakes in. Bad input here is an operator error — it must surface as
/// <see cref="ConfigException"/>, not quietly become the default.
///
/// Why these exist: a processor declared with
/// <c>batch_size: "seven"</c> previously loaded with
/// <c>batch_size = 1</c> (silent default), and the operator only noticed
/// when throughput was off. Now the factory throws at load time.
/// </summary>
public static class ConfigHelpers
{
    /// <summary>
    /// Parse <paramref name="raw"/> as an int. Empty / null → returns
    /// <paramref name="fallback"/> (the key wasn't set). Non-empty and
    /// unparseable → <see cref="ConfigException"/> with the offending
    /// value echoed back so the operator can grep the YAML.
    /// </summary>
    public static int ParseInt(string? raw, string key, int fallback)
    {
        if (string.IsNullOrEmpty(raw)) return fallback;
        if (int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var n))
            return n;
        throw new ConfigException($"config key '{key}' is not an integer: '{raw}'");
    }

    /// <summary>Same shape as <see cref="ParseInt"/> for <see cref="long"/>.</summary>
    public static long ParseLong(string? raw, string key, long fallback)
    {
        if (string.IsNullOrEmpty(raw)) return fallback;
        if (long.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var n))
            return n;
        throw new ConfigException($"config key '{key}' is not a long: '{raw}'");
    }

    /// <summary>
    /// Retrieve a required string config key, trimming surrounding
    /// whitespace. Missing / empty → <see cref="ConfigException"/>.
    /// Use for keys the factory can't meaningfully default — e.g.
    /// UpdateAttribute's "key" or PutFile's "output_dir".
    /// </summary>
    public static string RequireString(IReadOnlyDictionary<string, string> config, string key)
    {
        if (!config.TryGetValue(key, out var raw) || string.IsNullOrWhiteSpace(raw))
            throw new ConfigException($"missing required config key: {key}");
        return raw.Trim();
    }

    /// <summary>
    /// Retrieve an optional string with a fallback default. Missing or
    /// empty → <paramref name="fallback"/>. Distinct from
    /// <see cref="RequireString"/> — use when the key legitimately has
    /// a default (prefix, suffix, format, etc.).
    /// </summary>
    public static string GetString(IReadOnlyDictionary<string, string> config, string key, string fallback = "")
    {
        if (!config.TryGetValue(key, out var raw) || string.IsNullOrEmpty(raw))
            return fallback;
        return raw;
    }

    /// <summary>
    /// Validate that a string value is one of the allowed options.
    /// Throws <see cref="ConfigException"/> listing the valid options.
    /// Pattern for format-style configs (PutStdout, PutFile, etc.):
    /// <c>var fmt = ConfigHelpers.RequireOneOf(config, "format", "raw",
    /// new[] { "raw", "attrs", "hex", "v3" });</c>
    /// </summary>
    public static string RequireOneOf(IReadOnlyDictionary<string, string> config, string key, string fallback, string[] allowed)
    {
        var val = GetString(config, key, fallback);
        foreach (var a in allowed)
            if (val == a) return val;
        throw new ConfigException($"config key '{key}' must be one of [{string.Join(", ", allowed)}]; got: '{val}'");
    }

    /// <summary>Parse an int from a raw string (no config-dictionary), still throws on bad.</summary>
    public static int ParseIntRaw(string raw, string context)
    {
        if (int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var n))
            return n;
        throw new ConfigException($"{context}: not an integer: '{raw}'");
    }
}
