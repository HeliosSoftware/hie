using System.Text;
using System.Text.Json.Nodes;

namespace Hie.Tests;

public class ReaderTests
{
    [Fact]
    public void RowsAreKeyedByHeaderWithFileLineNumbers()
    {
        var recs = Reader.ReadCsv("patient,value\nPatient/1,95\n\nPatient/2,180\n");
        Assert.Equal(2, recs.Count);
        Assert.Equal(2, recs[0].Line);
        Assert.Equal("Patient/1", recs[0].Raw["patient"]!.GetValue<string>());
        Assert.Equal("95", recs[0].Raw["value"]!.GetValue<string>());
        Assert.Equal(4, recs[1].Line);
    }

    [Fact]
    public void ARowWithAnExtraColumnIsRejectedAlone()
    {
        var recs = Reader.ReadCsv("a,b\n1,2\n1,2,3\n4,5\n");
        Assert.Equal(3, recs.Count);
        Assert.Null(recs[0].ParseError);
        Assert.Equal("expected 2 columns (a,b), found 3", recs[1].ParseError);
        Assert.Null(recs[2].ParseError);
    }

    [Fact]
    public void QuotedFieldsAndWindowsLineEndingsAreHandled()
    {
        var recs = Reader.ReadCsv("name,note\r\n\"Doe, Jane\",\"said \"\"hi\"\"\"\r\n");
        Assert.Single(recs);
        Assert.Equal("Doe, Jane", recs[0].Raw["name"]!.GetValue<string>());
        Assert.Equal("said \"hi\"", recs[0].Raw["note"]!.GetValue<string>());
    }

    [Fact]
    public void ByteOrderMarksAreUnderstood()
    {
        Assert.Equal("a,b", Reader.DecodeText([0xEF, 0xBB, 0xBF, (byte)'a', (byte)',', (byte)'b']));
        var utf16 = new byte[] { 0xFF, 0xFE }.Concat(Encoding.Unicode.GetBytes("a,b")).ToArray();
        Assert.Equal("a,b", Reader.DecodeText(utf16));
        Assert.Throws<InvalidDataException>(() => Reader.DecodeText([0xC3, 0x28]));
    }

    [Fact]
    public void NdjsonKeepsBadLinesAsRejectedRecords()
    {
        var recs = Reader.ReadNdjson("{\"resourceType\":\"Patient\",\"id\":\"a\"}\n\nnot json\n[1,2]\n");
        Assert.Equal(3, recs.Count);
        Assert.Equal((1L, true), (recs[0].Line, recs[0].ParseError is null));
        Assert.Equal(3, recs[1].Line);
        Assert.StartsWith("invalid JSON", recs[1].ParseError);
        Assert.Equal((4L, "line is not a JSON object"), (recs[2].Line, recs[2].ParseError));
    }

    [Fact]
    public void BundleEntriesBecomeRecordsWithAbsoluteReferences()
    {
        var bundle = """
            {"resourceType":"Bundle","type":"transaction","entry":[
              {"fullUrl":"urn:uuid:p1","resource":{"resourceType":"Patient","id":"p1"}},
              {"fullUrl":"urn:uuid:o1","resource":{"resourceType":"Observation","id":"o1",
                 "subject":{"reference":"urn:uuid:p1"},"performer":[{"reference":"urn:uuid:unknown"}]}},
              {"fullUrl":"urn:uuid:x"}]}
            """;
        var recs = Reader.ReadFhirBundle(bundle);
        Assert.Equal(3, recs.Count);
        Assert.Equal(2, recs[1].Line);
        Assert.Equal("Patient/p1", recs[1].Raw["subject"]!["reference"]!.GetValue<string>());
        Assert.Equal("urn:uuid:unknown", recs[1].Raw["performer"]![0]!["reference"]!.GetValue<string>());
        Assert.Equal("entry has no resource", recs[2].ParseError);
    }

    [Fact]
    public void ASingleResourceFileIsOneRecord()
    {
        Assert.Single(Reader.ReadFhirBundle("""{"resourceType":"Patient","id":"x"}"""));
        Assert.Throws<InvalidDataException>(() => Reader.ReadFhirBundle("""{"a":1}"""));
    }
}

public class ConfigTests
{
    [Fact]
    public void DurationsParseWithUnits()
    {
        Assert.Equal(TimeSpan.FromSeconds(90), Config.ParseDuration("90s"));
        Assert.Equal(TimeSpan.FromDays(1), Config.ParseDuration("24h"));
        Assert.Throws<FormatException>(() => Config.ParseDuration("5 weeks"));
    }

    [Fact]
    public void PatternsMatchByPrefixAndSuffix()
    {
        Assert.True(Config.GlobMatch("*.csv", "labs.CSV"));
        Assert.True(Config.GlobMatch("lab_*.csv", "lab_2026-10-18.csv"));
        Assert.False(Config.GlobMatch("*.csv", "labs.csv.tmp"));
        Assert.True(Config.GlobMatch("*", "anything"));
    }

    [Fact]
    public void AFeedFileLoadsWithDefaults()
    {
        var dir = Directory.CreateTempSubdirectory("hie-config");
        try
        {
            Directory.CreateDirectory(Path.Combine(dir.FullName, "feeds"));
            File.WriteAllText(Path.Combine(dir.FullName, "t.py"), "def transform(r, c): return r\n");
            File.WriteAllText(Path.Combine(dir.FullName, "hie.yaml"), "fhir_base_url: http://localhost:8080\n");
            File.WriteAllText(Path.Combine(dir.FullName, "feeds", "x.yaml"),
                "id: x\nname: X\nsource:\n  type: s3\n  bucket: lab\n  prefix: results/\nformat: ndjson\ntransform: t.py\nexpect_every: 24h\n");
            var (config, feeds) = Config.Load(Path.Combine(dir.FullName, "hie.yaml"));
            Assert.Equal("http://localhost:8080", config.FhirBaseUrl);
            var feed = Assert.Single(feeds);
            Assert.Equal(Format.Ndjson, feed.FormatKind);
            Assert.Equal("s3://lab/results/", feed.Source.Describe());
            Assert.Equal(("*", "processed"), (feed.Source.Pattern, feed.Source.ArchivePrefix));
            Assert.Equal(TimeSpan.FromHours(24), feed.ExpectEveryDuration);
        }
        finally
        {
            dir.Delete(recursive: true);
        }
    }
}

public class FhirTests
{
    [Fact]
    public void ConditionalUrlsEncodeValuesButNotTheSeparator()
    {
        var key = new DeliveryKey("Observation", null, "urn:helios:hie:lab-results", "Patient/1 2345-7&x");
        Assert.Equal("Observation?identifier=urn:helios:hie:lab-results|Patient/1%202345-7%26x", key.RequestUrl());
        Assert.Equal("Patient/p1", new DeliveryKey("Patient", "p1", null, null).RequestUrl());
    }

    [Fact]
    public void ResourcesNeedAnIdOrAnIdentifier()
    {
        var byIdentifier = JsonNode.Parse("""{"resourceType":"Observation","identifier":[{"system":"urn:x","value":"1"}]}""")!;
        Assert.Equal(new DeliveryKey("Observation", null, "urn:x", "1"), Fhir.CheckResource(byIdentifier).Key);
        var byId = JsonNode.Parse("""{"resourceType":"Patient","id":"p1","identifier":[{"value":"mrn"}]}""")!;
        Assert.Equal(new DeliveryKey("Patient", "p1", null, null), Fhir.CheckResource(byId).Key);
        Assert.Contains("neither id nor identifier", Fhir.CheckResource(JsonNode.Parse("""{"resourceType":"Observation"}""")!).Error);
        Assert.NotNull(Fhir.CheckResource(JsonNode.Parse("""{"identifier":[]}""")!).Error);
    }

    [Fact]
    public void BatchEntriesAreClassifiedByStatus()
    {
        var resp = JsonNode.Parse("""
            {"entry":[
              {"response":{"status":"201 Created"}},
              {"response":{"status":"400 Bad Request","outcome":{"issue":[{"details":{"text":"bad status"}}]}}},
              {"response":{"status":"503 Service Unavailable"}}]}
            """)!;
        var r = Fhir.EntryResults(resp, 4);
        Assert.Equal(EntryKind.Ok, r[0].Kind);
        Assert.Equal((EntryKind.Rejected, 400), (r[1].Kind, r[1].Status));
        Assert.Equal((EntryKind.Transient, 503), (r[2].Kind, r[2].Status));
        Assert.Equal((EntryKind.Transient, 0), (r[3].Kind, r[3].Status));
        Assert.Equal("bad status", Fhir.OutcomeText(r[1].Response));
    }

    [Fact]
    public void RepliesMapToOutcomes()
    {
        var ok = PythonTransformer.ParseReply(JsonNode.Parse("""{"id":1,"resources":[{"resourceType":"Observation"}]}""")!);
        Assert.Single(Assert.IsType<Outcome.Resources>(ok).Items);
        var bad = PythonTransformer.ParseReply(JsonNode.Parse("""{"id":1,"error":{"kind":"data","message":"bad value"}}""")!);
        Assert.Equal(new Outcome.Rejected("data", "bad value"), bad);
    }
}

public class ObjectSourceTests
{
    [Fact]
    public async Task ObjectsAreListedFetchedAndArchived()
    {
        var store = new MemoryObjectStore();
        foreach (var key in new[] { "in/b.csv", "in/a.csv", "in/notes.txt", "in/processed/old.csv", "elsewhere/c.csv" })
            store.Put(key, "x"u8.ToArray(), DateTimeOffset.UtcNow.AddMinutes(-1));
        var source = new ObjectSource(store, "in", "in/processed", "*.csv", TimeSpan.Zero);
        Assert.Equal(["in/a.csv", "in/b.csv"], await source.ListAsync());

        var got = await source.FetchAsync("in/a.csv");
        Assert.Equal(("a.csv", "x"), (got.Name, Encoding.UTF8.GetString(got.Bytes)));

        await source.AcknowledgeAsync("in/a.csv", "7-a.csv");
        Assert.Equal(["in/b.csv"], await source.ListAsync());
        Assert.True(store.Contains("in/processed/7-a.csv"));
    }

    [Fact]
    public async Task AFreshObjectWaitsForTheAgeGuard()
    {
        var store = new MemoryObjectStore();
        store.Put("x.csv", "x"u8.ToArray());
        Assert.Empty(await new ObjectSource(store, "", "processed", "*", TimeSpan.FromMinutes(1)).ListAsync());
        Assert.Equal(["x.csv"], await new ObjectSource(store, "", "processed", "*", TimeSpan.Zero).ListAsync());
    }
}

public class ApiTests
{
    [Fact]
    public void MetricsRenderForAFeedWithAndWithoutData()
    {
        using var db = Db.InMemory();
        var feed = new FeedConfig { Id = "lab", Name = "Lab" };
        var text = Api.Metrics(new AppState(db, [feed]));
        Assert.Contains("hie_records{feed=\"lab\",status=\"sent\"} 0", text);

        var fileId = db.InsertFile("lab", "a.csv", "sha", 1, "processing", null);
        db.InsertRecords(fileId, "lab", [new SourceRecord(2, new JsonObject(), null), new SourceRecord(3, new JsonObject(), "bad")]);
        db.InsertAlert("lab", "rejected_records", "x");
        text = Api.Metrics(new AppState(db, [feed]));
        Assert.Contains("hie_records{feed=\"lab\",status=\"received\"} 1", text);
        Assert.Contains("hie_records{feed=\"lab\",status=\"rejected\"} 1", text);
        Assert.Contains("hie_feed_last_received_timestamp_seconds{feed=\"lab\"}", text);
        Assert.Contains("hie_alerts_total{feed=\"lab\",kind=\"rejected_records\"} 1", text);
    }
}
