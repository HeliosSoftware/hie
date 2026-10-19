using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

namespace Hie;

public sealed class HieConfig
{
    public string FhirBaseUrl { get; set; } = "";
    public string Database { get; set; } = "data/hie.db";
    public string Listen { get; set; } = "http://127.0.0.1:8090";
    public string Python { get; set; } = OperatingSystem.IsWindows() ? "py" : "python3";
    public string FeedsDir { get; set; } = "feeds";
    public AlertConfig Alerts { get; set; } = new();
}

public sealed class AlertConfig
{
    public string? WebhookUrl { get; set; }
}

public enum Format
{
    Csv,
    Ndjson,
    FhirBundle,
}

/// <summary>Where a feed reads from: a local folder, or an S3 bucket.</summary>
public sealed class SourceConfig
{
    public string Type { get; set; } = "folder";
    public string Pattern { get; set; } = "*";
    public int PollSeconds { get; set; } = 5;
    public int MinFileAgeMs { get; set; } = 1000;

    // folder
    public string? Path { get; set; }
    public string? Archive { get; set; }

    // s3: credentials come from the usual AWS environment (keys, profiles, roles).
    public string? Bucket { get; set; }
    public string Prefix { get; set; } = "";
    public string? Region { get; set; }
    /// <summary>For S3-compatible stores such as MinIO.</summary>
    public string? Endpoint { get; set; }
    public string ArchivePrefix { get; set; } = "processed";

    public TimeSpan PollInterval => TimeSpan.FromSeconds(PollSeconds);
    public TimeSpan MinFileAge => TimeSpan.FromMilliseconds(MinFileAgeMs);

    public string Describe() => Type == "s3" ? $"s3://{Bucket}/{Prefix}" : Path ?? "";
}

public sealed class DeliveryConfig
{
    public int BatchSize { get; set; } = 100;
    public int InitialBackoffMs { get; set; } = 2000;
    public int MaxBackoffMs { get; set; } = 300_000;
    public int AlertAfterAttempts { get; set; } = 5;
}

public sealed class FeedConfig
{
    public string Id { get; set; } = "";
    public string Name { get; set; } = "";
    public SourceConfig Source { get; set; } = new();
    public string Format { get; set; } = "csv";
    public string Transform { get; set; } = "";
    public DeliveryConfig Delivery { get; set; } = new();
    public string? ExpectEvery { get; set; }

    [YamlIgnore]
    public TimeSpan? ExpectEveryDuration { get; set; }

    [YamlIgnore]
    public Format FormatKind => Format switch
    {
        "csv" => Hie.Format.Csv,
        "ndjson" => Hie.Format.Ndjson,
        "fhir_bundle" => Hie.Format.FhirBundle,
        _ => throw new InvalidOperationException($"feed {Id}: unknown format '{Format}' (use csv, ndjson or fhir_bundle)"),
    };

    internal void Resolve(string baseDir)
    {
        if (Source.Type == "folder")
        {
            if (string.IsNullOrEmpty(Source.Path))
                throw new InvalidOperationException($"feed {Id}: a folder source needs a path");
            Source.Path = System.IO.Path.GetFullPath(System.IO.Path.Combine(baseDir, Source.Path));
            if (Source.Archive is not null)
                Source.Archive = System.IO.Path.GetFullPath(System.IO.Path.Combine(baseDir, Source.Archive));
        }
        else if (Source.Type == "s3")
        {
            if (string.IsNullOrEmpty(Source.Bucket))
                throw new InvalidOperationException($"feed {Id}: an s3 source needs a bucket");
        }
        else
        {
            throw new InvalidOperationException($"feed {Id}: unknown source type '{Source.Type}' (use folder or s3)");
        }
        _ = FormatKind;
        Transform = System.IO.Path.GetFullPath(System.IO.Path.Combine(baseDir, Transform));
        if (!File.Exists(Transform))
            throw new InvalidOperationException($"feed {Id}: transform {Transform} not found");
        if (ExpectEvery is not null)
            ExpectEveryDuration = Config.ParseDuration(ExpectEvery);
    }
}

public static class Config
{
    private static readonly IDeserializer Yaml = new DeserializerBuilder()
        .WithNamingConvention(UnderscoredNamingConvention.Instance)
        .IgnoreUnmatchedProperties()
        .Build();

    /// <summary>Loads hie.yaml and every feed in its feeds_dir. Relative paths resolve against the directory holding hie.yaml.</summary>
    public static (HieConfig Config, List<FeedConfig> Feeds) Load(string path)
    {
        var config = Yaml.Deserialize<HieConfig>(File.ReadAllText(path)) ?? new HieConfig();
        var baseDir = Path.GetDirectoryName(Path.GetFullPath(path)) ?? ".";
        config.Database = Path.GetFullPath(Path.Combine(baseDir, config.Database));
        config.FeedsDir = Path.GetFullPath(Path.Combine(baseDir, config.FeedsDir));
        if (string.IsNullOrWhiteSpace(config.FhirBaseUrl))
            throw new InvalidOperationException($"{path}: fhir_base_url is required");

        var feeds = new List<FeedConfig>();
        var ids = new HashSet<string>();
        var files = Directory.GetFiles(config.FeedsDir)
            .Where(f => f.EndsWith(".yaml", StringComparison.OrdinalIgnoreCase) || f.EndsWith(".yml", StringComparison.OrdinalIgnoreCase))
            .OrderBy(f => f, StringComparer.Ordinal);
        foreach (var file in files)
        {
            var feed = Yaml.Deserialize<FeedConfig>(File.ReadAllText(file))
                ?? throw new InvalidOperationException($"{file}: empty feed definition");
            feed.Resolve(baseDir);
            if (!ids.Add(feed.Id))
                throw new InvalidOperationException($"duplicate feed id {feed.Id}");
            feeds.Add(feed);
        }
        return (config, feeds);
    }

    /// <summary>Parses 90s, 30m, 24h or 7d.</summary>
    public static TimeSpan ParseDuration(string text)
    {
        text = text.Trim();
        var digits = text.TakeWhile(char.IsAsciiDigit).Count();
        if (digits == 0 || !long.TryParse(text[..digits], out var n))
            throw new FormatException($"invalid duration '{text}'");
        return text[digits..] switch
        {
            "s" => TimeSpan.FromSeconds(n),
            "m" => TimeSpan.FromMinutes(n),
            "h" => TimeSpan.FromHours(n),
            "d" => TimeSpan.FromDays(n),
            _ => throw new FormatException($"invalid duration unit in '{text}' (use s, m, h or d)"),
        };
    }

    /// <summary>Matches a file name against a pattern with at most one '*', ignoring case.</summary>
    public static bool GlobMatch(string pattern, string name)
    {
        var star = pattern.IndexOf('*');
        if (star < 0)
            return string.Equals(pattern, name, StringComparison.OrdinalIgnoreCase);
        var prefix = pattern[..star];
        var suffix = pattern[(star + 1)..];
        return name.Length >= prefix.Length + suffix.Length
            && name.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)
            && name.EndsWith(suffix, StringComparison.OrdinalIgnoreCase);
    }

    public static string Human(TimeSpan d)
    {
        var s = (long)d.TotalSeconds;
        return s switch
        {
            <= 119 => $"{s}s",
            <= 7199 => $"{s / 60}m",
            <= 172_799 => $"{s / 3600}h",
            _ => $"{s / 86_400}d",
        };
    }
}
