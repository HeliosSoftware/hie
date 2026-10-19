using Hie;
using OpenTelemetry.Logs;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

var configPath = Environment.GetEnvironmentVariable("HIE_CONFIG") ?? "hie.yaml";
var check = false;
for (var i = 0; i < args.Length; i++)
{
    switch (args[i])
    {
        case "--config" or "-c" when i + 1 < args.Length:
            configPath = args[++i];
            break;
        case "check":
            check = true;
            break;
        case "run":
            break;
        case "--help" or "-h":
            Console.WriteLine("usage: hie [run|check] [--config hie.yaml]");
            return 0;
        default:
            Console.Error.WriteLine($"unknown argument {args[i]}");
            return 2;
    }
}

(HieConfig config, List<FeedConfig> feeds) loaded;
try
{
    loaded = Config.Load(configPath);
}
catch (Exception e) when (e is IOException or InvalidOperationException or FormatException or YamlDotNet.Core.YamlException)
{
    Console.Error.WriteLine($"configuration: {e.Message}");
    return 1;
}
var (config, feeds) = loaded;
var runtimeDir = Environment.GetEnvironmentVariable("HIE_RUNTIME_DIR")
    ?? Path.Combine(Path.GetDirectoryName(Path.GetFullPath(configPath)) ?? ".", "hie_runtime");

if (check)
{
    Console.WriteLine($"config ok: {feeds.Count} feed(s), HFS at {config.FhirBaseUrl}");
    foreach (var f in feeds)
        Console.WriteLine($"  {f.Id} ({f.Name}) <- {f.Source.Describe()} via {f.Transform}");
    return 0;
}

var builder = WebApplication.CreateBuilder(new WebApplicationOptions { Args = [] });
builder.WebHost.UseUrls(config.Listen);
builder.Logging.ClearProviders().AddSimpleConsole(o => o.TimestampFormat = "yyyy-MM-ddTHH:mm:ss.fffZ ");
if (Enum.TryParse<LogLevel>(Environment.GetEnvironmentVariable("HIE_LOG"), ignoreCase: true, out var level))
    builder.Logging.SetMinimumLevel(level);
builder.Logging.AddFilter("Microsoft.AspNetCore", LogLevel.Warning);

// Traces, metrics and logs go to an OTLP collector (the Aspire dashboard, or whatever
// watches HFS) when OTEL_EXPORTER_OTLP_ENDPOINT is set; Prometheus text stays on /metrics.
if (!string.IsNullOrEmpty(Environment.GetEnvironmentVariable("OTEL_EXPORTER_OTLP_ENDPOINT")))
{
    builder.Services.AddOpenTelemetry()
        .ConfigureResource(r => r.AddService("hie"))
        .WithTracing(t => t.AddAspNetCoreInstrumentation().AddHttpClientInstrumentation().AddOtlpExporter())
        .WithMetrics(m => m.AddAspNetCoreInstrumentation().AddHttpClientInstrumentation().AddRuntimeInstrumentation().AddOtlpExporter());
    builder.Logging.AddOpenTelemetry(o => o.AddOtlpExporter());
}

var db = Db.Open(config.Database);
var http = new HttpClient { Timeout = TimeSpan.FromSeconds(120) };
var fhir = new FhirClient(http, config.FhirBaseUrl);
builder.Services.AddSingleton(db);
builder.Services.AddSingleton<IReadOnlyList<FeedRunner>>(sp =>
{
    var log = sp.GetRequiredService<ILoggerFactory>();
    var alerts = new Alerter(db, config.Alerts.WebhookUrl, http, log.CreateLogger<Alerter>());
    return feeds.Select(f => new FeedRunner(f, Sources.FromConfig(f), db, fhir, alerts, config.Python, runtimeDir, log.CreateLogger($"hie.feed.{f.Id}"))).ToList();
});
builder.Services.AddHostedService<FeedRunnerService>();

var app = builder.Build();
app.MapHieApi(new AppState(db, feeds));
app.Lifetime.ApplicationStarted.Register(() => app.Logger.LogInformation("API on {Url}", config.Listen));
await app.RunAsync();
return 0;
