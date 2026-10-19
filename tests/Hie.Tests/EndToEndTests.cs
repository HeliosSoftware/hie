using System.Diagnostics;
using System.Text.Json.Nodes;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Hie.Tests;

/// <summary>A stand-in FHIR server: accepts batch Bundles, can be taken down, refuses Patient/999.</summary>
internal sealed class StandInFhir : IAsyncDisposable
{
    private readonly WebApplication _app;
    public string BaseUrl { get; private set; } = "";
    public bool Down { get; set; }
    public Dictionary<string, int> Versions { get; } = new();
    public List<string> Urls { get; } = new();
    public int Requests { get; private set; }

    private StandInFhir(WebApplication app) => _app = app;

    public static async Task<StandInFhir> StartAsync()
    {
        var builder = WebApplication.CreateBuilder(new WebApplicationOptions { Args = [] });
        builder.Logging.ClearProviders();
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        var app = builder.Build();
        var server = new StandInFhir(app);
        app.MapPost("/", async (HttpContext ctx) =>
        {
            server.Requests++;
            if (server.Down)
            {
                ctx.Response.StatusCode = 503;
                await ctx.Response.WriteAsync("""{"resourceType":"OperationOutcome"}""");
                return;
            }
            var bundle = await JsonNode.ParseAsync(ctx.Request.Body);
            var entries = new JsonArray();
            foreach (var e in (JsonArray)bundle!["entry"]!)
            {
                var url = e!["request"]!["url"]!.GetValue<string>();
                server.Urls.Add(url);
                if (e["resource"]?["subject"]?["reference"]?.GetValue<string>() == "Patient/999")
                {
                    entries.Add(new JsonObject
                    {
                        ["response"] = new JsonObject
                        {
                            ["status"] = "400 Bad Request",
                            ["outcome"] = JsonNode.Parse("""{"resourceType":"OperationOutcome","issue":[{"severity":"error","code":"invalid","details":{"text":"subject Patient/999 does not exist"}}]}"""),
                        },
                    });
                    continue;
                }
                var v = server.Versions[url] = server.Versions.GetValueOrDefault(url) + 1;
                entries.Add(new JsonObject { ["response"] = new JsonObject { ["status"] = v == 1 ? "201 Created" : "200 OK", ["etag"] = $"W/\"{v}\"" } });
            }
            ctx.Response.ContentType = "application/fhir+json";
            await ctx.Response.WriteAsync(new JsonObject { ["resourceType"] = "Bundle", ["type"] = "batch-response", ["entry"] = entries }.ToJsonString());
        });
        await app.StartAsync();
        server.BaseUrl = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.First();
        return server;
    }

    public async ValueTask DisposeAsync()
    {
        await _app.StopAsync();
        await _app.DisposeAsync();
    }
}

internal static class TestEnv
{
    public static string Repo => Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", ".."));

    public static string? Python()
    {
        var candidates = Environment.GetEnvironmentVariable("HIE_TEST_PYTHON") is { } p ? [p] : new[] { "py", "python3", "python" };
        foreach (var c in candidates)
        {
            try
            {
                var start = new ProcessStartInfo(c) { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
                start.ArgumentList.Add("-c");
                start.ArgumentList.Add("import sys; sys.exit(0 if sys.version_info >= (3, 8) else 1)");
                using var proc = Process.Start(start);
                if (proc is null)
                    continue;
                proc.WaitForExit(10_000);
                if (proc.ExitCode == 0)
                    return c;
            }
            catch (System.ComponentModel.Win32Exception)
            {
            }
        }
        return null;
    }

    public static FeedConfig Feed(string yaml)
    {
        var dir = Directory.CreateTempSubdirectory("hie-feed");
        Directory.CreateDirectory(Path.Combine(dir.FullName, "feeds"));
        File.WriteAllText(Path.Combine(dir.FullName, "hie.yaml"), "fhir_base_url: http://unused\n");
        File.WriteAllText(Path.Combine(dir.FullName, "feeds", "f.yaml"), yaml);
        return Config.Load(Path.Combine(dir.FullName, "hie.yaml")).Feeds.Single();
    }

    public static long Count(Db db, string feed, string status) => db.FeedCounts(feed)[status]!.GetValue<long>();

    public static List<string> AlertKinds(Db db)
        => db.QueryJson("SELECT kind FROM alerts ORDER BY id").Select(r => r["kind"]!.GetValue<string>()).ToList();
}

public class PipelineTests
{
    [Fact]
    public async Task AFeedSurvivesAnOutageBadRowsAndResends()
    {
        var python = TestEnv.Python();
        if (python is null)
            return; // no Python 3.8+ on this machine; set HIE_TEST_PYTHON

        await using var fhir = await StandInFhir.StartAsync();
        var root = Directory.CreateTempSubdirectory("hie-e2e").FullName;
        var inbox = Path.Combine(root, "inbox");
        var transform = Path.Combine(TestEnv.Repo, "transforms", "lab_results.py").Replace('\\', '/');
        var feed = TestEnv.Feed($"""
            id: lab-results
            name: Lab results
            source:
              type: folder
              path: {inbox.Replace('\\', '/')}
              pattern: "*.csv"
              min_file_age_ms: 0
            transform: {transform}
            delivery:
              initial_backoff_ms: 10
              alert_after_attempts: 2
            expect_every: 1s
            """);
        using var db = Db.InMemory();
        using var http = new HttpClient();
        var alerts = new Alerter(db, null, http, NullLogger.Instance);
        using var runner = new FeedRunner(feed, Sources.FromConfig(feed), db, new FhirClient(http, fhir.BaseUrl), alerts, python, Path.Combine(TestEnv.Repo, "hie_runtime"), NullLogger.Instance);
        var labs = await File.ReadAllTextAsync(Path.Combine(TestEnv.Repo, "examples", "data", "labs.csv"));

        // HFS is down when the file arrives: rows are stored, the file leaves the inbox, nothing is lost.
        fhir.Down = true;
        Drop(inbox, "labs.csv", labs);
        await runner.TickAsync();
        Assert.Equal(3, TestEnv.Count(db, "lab-results", "pending"));
        Assert.False(File.Exists(Path.Combine(inbox, "labs.csv")));
        await Task.Delay(30);
        await runner.TickAsync();
        Assert.Equal(["delivery"], TestEnv.AlertKinds(db));

        // HFS comes back: the same rows are delivered on the next retry.
        fhir.Down = false;
        await Task.Delay(60);
        await runner.TickAsync();
        Assert.Equal(3, TestEnv.Count(db, "lab-results", "sent"));
        Assert.Equal(3, fhir.Versions.Count);
        Assert.Equal(["delivery", "delivery_recovered"], TestEnv.AlertKinds(db));

        // The identical file again: recognized by its hash and not reprocessed.
        var before = fhir.Requests;
        Drop(inbox, "labs.csv", labs);
        await runner.TickAsync();
        Assert.Equal(before, fhir.Requests);
        Assert.Equal(1, db.FeedCounts("lab-results")["duplicate_files"]!.GetValue<long>());

        // The same rows in a different file: delivered again, but as updates, not new resources.
        Drop(inbox, "labs-resent.csv", labs + "\n");
        await runner.TickAsync();
        Assert.Equal(6, TestEnv.Count(db, "lab-results", "sent"));
        Assert.Equal(3, fhir.Versions.Count);
        Assert.All(fhir.Versions.Values, v => Assert.Equal(2, v));

        // A messy file: each bad row is rejected on its own with its reason; good rows go through.
        var messy = await File.ReadAllTextAsync(Path.Combine(TestEnv.Repo, "examples", "data", "messy.csv"));
        Drop(inbox, "messy.csv", messy + "Patient/999,2345-7,Glucose,77,mg/dL,2026-10-16\n");
        await runner.TickAsync();
        var rejected = db.QueryJson("SELECT id, line, error_kind, error, raw FROM records WHERE status = 'rejected' ORDER BY line");
        Assert.Equal(
            [(3L, "data"), (4L, "parse"), (5L, "data"), (6L, "data"), (7L, "data"), (8L, "server")],
            rejected.Select(r => (r["line"]!.GetValue<long>(), r["error_kind"]!.GetValue<string>())).ToList());
        Assert.Contains("'alto' is not a number", rejected[0]["error"]!.GetValue<string>());
        Assert.Contains("Patient/999 does not exist", rejected[5]["error"]!.GetValue<string>());
        Assert.Equal(7, TestEnv.Count(db, "lab-results", "sent")); // the GLU local-code row is mapped to LOINC and sent
        Assert.Equal("rejected_records", TestEnv.AlertKinds(db).Last());

        // Someone fixes the value and reprocesses that one row.
        var alto = rejected[0];
        var raw = (JsonObject)alto["raw"]!.DeepClone();
        raw["value"] = "101";
        Assert.True(db.Reprocess(alto["id"]!.GetValue<long>(), raw));
        await runner.TickAsync();
        Assert.Equal(8, TestEnv.Count(db, "lab-results", "sent"));
        Assert.Equal(5, TestEnv.Count(db, "lab-results", "rejected"));
        var fixedRow = db.QueryJson("SELECT raw, original_raw FROM records WHERE id = @p0", alto["id"]!.GetValue<long>()).Single();
        Assert.Equal("101", fixedRow["raw"]!["value"]!.GetValue<string>());
        Assert.Equal("alto", fixedRow["original_raw"]!["value"]!.GetValue<string>()); // the data as received is kept
        Assert.Equal(1, TestEnv.AlertKinds(db).Count(k => k == "rejected_records")); // reprocessing does not repeat the file's alert

        // Nothing arrives for longer than expect_every: one silence alert, not one per pass.
        await Task.Delay(1100);
        await runner.TickAsync();
        await runner.TickAsync();
        Assert.Equal(1, TestEnv.AlertKinds(db).Count(k => k == "silence"));
    }

    [Fact]
    public async Task AnNdjsonObjectIsDeliveredByIdAndArchived()
    {
        var python = TestEnv.Python();
        if (python is null)
            return;

        await using var fhir = await StandInFhir.StartAsync();
        var transform = Path.Combine(TestEnv.Repo, "transforms", "fhir_passthrough.py").Replace('\\', '/');
        var feed = TestEnv.Feed($"""
            id: export
            name: Export
            source:
              type: s3
              bucket: unused
            format: ndjson
            transform: {transform}
            """);
        var store = new MemoryObjectStore();
        store.Put("exports/Observation.ndjson", System.Text.Encoding.UTF8.GetBytes(
            "{\"resourceType\":\"Patient\",\"id\":\"p1\"}\n" +
            "{\"resourceType\":\"Observation\",\"id\":\"o1\",\"subject\":{\"reference\":\"Patient/p1\"}}\n" +
            "{\"resourceType\":\"Observation\"}\n" +
            "{\"resourceType\":\"Bundle\",\"id\":\"b\"}\n"), DateTimeOffset.UtcNow.AddMinutes(-1));
        var source = new ObjectSource(store, "exports", "exports/processed", "*.ndjson", TimeSpan.Zero);

        using var db = Db.InMemory();
        using var http = new HttpClient();
        var alerts = new Alerter(db, null, http, NullLogger.Instance);
        using var runner = new FeedRunner(feed, source, db, new FhirClient(http, fhir.BaseUrl), alerts, python, Path.Combine(TestEnv.Repo, "hie_runtime"), NullLogger.Instance);
        await runner.TickAsync();

        Assert.Equal((2L, 2L), (TestEnv.Count(db, "export", "sent"), TestEnv.Count(db, "export", "rejected")));
        Assert.Equal(["Patient/p1", "Observation/o1"], fhir.Urls);
        var rejected = db.QueryJson("SELECT line, error FROM records WHERE status = 'rejected' ORDER BY line");
        Assert.Equal(3, rejected[0]["line"]!.GetValue<long>());
        Assert.Contains("neither id nor identifier", rejected[0]["error"]!.GetValue<string>());
        Assert.Equal(4, rejected[1]["line"]!.GetValue<long>());
        Assert.Contains("nested Bundle", rejected[1]["error"]!.GetValue<string>());
        Assert.False(store.Contains("exports/Observation.ndjson"));
        Assert.True(store.Contains("exports/processed/1-Observation.ndjson"));
    }

    private static void Drop(string inbox, string name, string content)
    {
        Directory.CreateDirectory(inbox);
        File.WriteAllText(Path.Combine(inbox, name), content);
    }
}
