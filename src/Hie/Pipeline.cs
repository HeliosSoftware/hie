using System.Text.Json.Nodes;

namespace Hie;

/// <summary>
/// Moves one feed's data through: source -> stored rows -> transform -> HFS. Every row is in
/// the database before its object leaves the source, so a crash or an HFS outage never loses
/// data, and every row keeps its object name and line number.
/// </summary>
public sealed class FeedRunner : IDisposable
{
    private const int TransformChunk = 200;

    public FeedConfig Feed { get; }
    private readonly ISource _source;
    private readonly Db _db;
    private readonly FhirClient _fhir;
    private readonly PythonTransformer _transformer;
    private readonly Alerter _alerts;
    private readonly ILogger _log;
    private readonly long _startedAt = Db.NowMs();
    private bool _outageAlerted;

    public FeedRunner(FeedConfig feed, ISource source, Db db, FhirClient fhir, Alerter alerts, string python, string runtimeDir, ILogger log)
    {
        Feed = feed;
        _source = source;
        _db = db;
        _fhir = fhir;
        _alerts = alerts;
        _log = log;
        _transformer = new PythonTransformer(python, runtimeDir, feed.Transform, log);
    }

    public async Task RunAsync(CancellationToken ct)
    {
        _log.LogInformation("watching feed={Feed} source={Source}", Feed.Id, Feed.Source.Describe());
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await TickAsync(ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception e)
            {
                _log.LogError("feed={Feed}: {Error}", Feed.Id, e.Message);
            }
            try
            {
                await Task.Delay(Feed.Source.PollInterval, ct);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    /// <summary>
    /// One pass of the whole pipeline. Each step picks up whatever the previous passes left,
    /// so a step that fails is simply retried on the next pass.
    /// </summary>
    public async Task TickAsync(CancellationToken ct = default)
    {
        await ScanAsync(ct);
        await TransformReceivedAsync(ct);
        await DeliverDueAsync(ct);
        Finish();
        CheckSilence();
    }

    private async Task ScanAsync(CancellationToken ct)
    {
        foreach (var key in await _source.ListAsync(ct))
            await IngestAsync(await _source.FetchAsync(key, ct), ct);
    }

    private async Task IngestAsync(Incoming incoming, CancellationToken ct)
    {
        var feed = Feed.Id;
        var (key, name, bytes) = incoming;
        var sha = Reader.Sha256Hex(bytes);

        if (_db.FindFileByHash(feed, sha) is { } original)
        {
            var dupId = _db.InsertFile(feed, name, sha, bytes.Length, "duplicate", original);
            _log.LogInformation("feed={Feed} file={File} duplicate_of={Original}: same content already received; not reprocessed", feed, name, original);
            await _source.AcknowledgeAsync(key, $"{dupId}-{name}", ct);
            return;
        }

        var fileId = _db.InsertFile(feed, name, sha, bytes.Length, "processing", null);
        try
        {
            var records = Reader.Read(Feed.FormatKind, bytes);
            _db.InsertRecords(fileId, feed, records);
            _log.LogInformation("feed={Feed} file={File} records={Count}: received", feed, name, records.Count);
        }
        catch (Exception e) when (e is InvalidDataException or FormatException)
        {
            _db.FailFile(fileId, e.Message);
            _alerts.Raise(feed, "unreadable_file", $"{Feed.Name}: cannot read {name}: {e.Message}");
        }
        await _source.AcknowledgeAsync(key, $"{fileId}-{name}", ct);
    }

    private async Task TransformReceivedAsync(CancellationToken ct)
    {
        while (true)
        {
            var items = _db.ReceivedRecords(Feed.Id, TransformChunk);
            if (items.Count == 0)
                return;
            foreach (var item in items)
            {
                var context = new JsonObject { ["feed"] = Feed.Id, ["file"] = item.FileName, ["line"] = item.Line };
                Outcome outcome;
                try
                {
                    outcome = await _transformer.TransformAsync(item.Raw, context, ct);
                }
                catch (Exception e) when (e is IOException or InvalidDataException or InvalidOperationException)
                {
                    _log.LogError("feed={Feed}: transform process unavailable, will retry: {Error}", Feed.Id, e.Message);
                    return;
                }
                switch (outcome)
                {
                    case Outcome.Rejected rejected:
                        _db.MarkRejected(item.Id, rejected.Kind, rejected.Message, null);
                        break;
                    case Outcome.Resources { Items.Count: 0 }:
                        _db.MarkRejected(item.Id, "filtered", "transform returned no resources", null);
                        break;
                    case Outcome.Resources resources:
                        var error = resources.Items.Select(r => Fhir.CheckResource(r).Error).FirstOrDefault(e => e is not null);
                        if (error is not null)
                            _db.MarkRejected(item.Id, "validation", error, null);
                        else
                            _db.MarkTransformed(item.Id, new JsonArray(resources.Items.Select(r => r.DeepClone()).ToArray()));
                        break;
                }
            }
        }
    }

    private async Task DeliverDueAsync(CancellationToken ct)
    {
        while (true)
        {
            var items = _db.DueRecords(Feed.Id, Feed.Delivery.BatchSize);
            if (items.Count == 0 || !await DeliverAsync(items, ct))
                return;
        }
    }

    /// <summary>
    /// Sends items in one batch Bundle. Returns false when HFS is unavailable, so the caller
    /// stops until the next pass instead of hammering it.
    /// </summary>
    private async Task<bool> DeliverAsync(List<WorkItem> items, CancellationToken ct)
    {
        var entries = new List<(string Url, JsonNode Resource)>();
        var owner = new List<int>();
        for (var i = 0; i < items.Count; i++)
        {
            foreach (var resource in items[i].Resources ?? [])
            {
                if (resource is null)
                    continue;
                var (key, error) = Fhir.CheckResource(resource);
                if (key is null)
                    throw new InvalidOperationException(error);
                entries.Add((key.RequestUrl(), resource));
                owner.Add(i);
            }
        }
        switch (await _fhir.PostBundleAsync(Fhir.BatchBundle(entries), ct))
        {
            case SendResult.Transient transient:
                foreach (var item in items)
                    Defer(item, transient.Reason);
                var worst = items.Max(i => i.Attempts + 1);
                if (worst >= Feed.Delivery.AlertAfterAttempts && !_outageAlerted)
                {
                    _outageAlerted = true;
                    _alerts.Raise(Feed.Id, "delivery", $"{Feed.Name}: cannot deliver to HFS after {worst} attempts ({transient.Reason}); data is kept and retried");
                }
                return false;

            case SendResult.Refused refused when items.Count > 1:
                _log.LogWarning("feed={Feed} status={Status}: batch refused as a whole; sending records one by one", Feed.Id, refused.Status);
                foreach (var item in items)
                    if (!await DeliverAsync([item], ct))
                        return false;
                return true;

            case SendResult.Refused refused:
                _db.MarkRejected(items[0].Id, "server", $"HTTP {refused.Status}: {Fhir.OutcomeText(refused.Body)}", refused.Body);
                return true;

            case SendResult.Ok ok:
                if (_outageAlerted)
                {
                    _outageAlerted = false;
                    _alerts.Raise(Feed.Id, "delivery_recovered", $"{Feed.Name}: delivery to HFS recovered");
                }
                var results = Fhir.EntryResults(ok.Body, entries.Count);
                for (var i = 0; i < items.Count; i++)
                {
                    var mine = results.Where((_, idx) => owner[idx] == i).ToList();
                    var responses = new JsonArray(mine.Select(r => r.Response.DeepClone()).ToArray());
                    var rejected = mine.FirstOrDefault(r => r.Kind == EntryKind.Rejected);
                    var transientEntry = mine.FirstOrDefault(r => r.Kind == EntryKind.Transient);
                    if (rejected is not null)
                        _db.MarkRejected(items[i].Id, "server", $"HTTP {rejected.Status}: {Fhir.OutcomeText(rejected.Response)}", responses);
                    else if (transientEntry is not null)
                        Defer(items[i], $"HTTP {transientEntry.Status} for this entry");
                    else
                        _db.MarkSent(items[i].Id, responses);
                }
                return true;

            default:
                throw new InvalidOperationException("unreachable");
        }
    }

    private void Defer(WorkItem item, string reason)
    {
        var attempts = item.Attempts + 1;
        var d = Feed.Delivery;
        var backoff = Math.Min((long)d.InitialBackoffMs << Math.Min(attempts - 1, 20), d.MaxBackoffMs);
        _db.Defer(item.Id, attempts, Db.NowMs() + backoff, reason);
    }

    private void Finish()
    {
        foreach (var f in _db.FinishFiles(Feed.Id))
        {
            _log.LogInformation("feed={Feed} file={File} total={Total} sent={Sent} rejected={Rejected}: file done", Feed.Id, f.Name, f.Total, f.Sent, f.Rejected);
            if (f.NewlyRejected <= 0)
                continue;
            var first = f.FirstRejection is { } fr ? $" First: line {fr.Line}: {fr.Error.Split('\n')[0]}" : "";
            _alerts.Raise(Feed.Id, "rejected_records", $"{Feed.Name}: {f.Rejected} of {f.Total} records rejected in {f.Name}.{first}");
        }
    }

    private void CheckSilence()
    {
        if (Feed.ExpectEveryDuration is not { } window)
            return;
        var last = _db.LastReceivedAt(Feed.Id) ?? _startedAt;
        var silentFor = Db.NowMs() - last;
        if (silentFor <= (long)window.TotalMilliseconds)
            return;
        if (_db.LastAlertAt(Feed.Id, "silence") is { } t && t >= last)
            return;
        _alerts.Raise(Feed.Id, "silence",
            $"{Feed.Name}: nothing received for {Config.Human(TimeSpan.FromMilliseconds(silentFor))} (expected at least every {Config.Human(window)})");
    }

    public void Dispose() => _transformer.Dispose();
}

/// <summary>Runs every feed's loop for the lifetime of the host.</summary>
public sealed class FeedRunnerService(IReadOnlyList<FeedRunner> runners) : BackgroundService
{
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
        => Task.WhenAll(runners.Select(r => r.RunAsync(stoppingToken)));
}
