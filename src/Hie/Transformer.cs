using System.Diagnostics;
using System.Text.Json.Nodes;

namespace Hie;

/// <summary>What a transform made of one record.</summary>
public abstract record Outcome
{
    public sealed record Resources(List<JsonNode> Items) : Outcome;
    public sealed record Rejected(string Kind, string Message) : Outcome;
}

/// <summary>
/// A long-lived Python process running one feed's transform script. One JSON line per
/// request and per reply. A failure of the process (not of the data) kills it, and the
/// next call starts a fresh one.
/// </summary>
public sealed class PythonTransformer(string python, string runtimeDir, string transformScript, ILogger logger) : IDisposable
{
    private readonly SemaphoreSlim _lock = new(1, 1);
    private Process? _process;
    private StreamWriter? _stdin;
    private StreamReader? _stdout;
    private long _nextId = 1;

    public async Task<Outcome> TransformAsync(JsonNode record, JsonObject context, CancellationToken ct = default)
    {
        await _lock.WaitAsync(ct);
        try
        {
            for (var attempt = 0; ; attempt++)
            {
                if (_process is null)
                    await SpawnAsync(ct);
                try
                {
                    return ParseReply(await CallAsync(record, context, ct));
                }
                catch (Exception e) when (e is IOException or InvalidDataException or ObjectDisposedException)
                {
                    Kill();
                    if (attempt == 1)
                        throw;
                    logger.LogWarning("transform process failed ({Error}); restarting it", e.Message);
                }
            }
        }
        finally
        {
            _lock.Release();
        }
    }

    private async Task SpawnAsync(CancellationToken ct)
    {
        var start = new ProcessStartInfo(python)
        {
            RedirectStandardInput = true,
            RedirectStandardOutput = true,
            UseShellExecute = false,
        };
        start.ArgumentList.Add("-u");
        start.ArgumentList.Add(Path.Combine(runtimeDir, "worker.py"));
        start.ArgumentList.Add(transformScript);
        var process = Process.Start(start) ?? throw new InvalidOperationException($"cannot start {python}");
        var stdout = process.StandardOutput;
        var ready = await stdout.ReadLineAsync(ct);
        if (ready is null || JsonNode.Parse(ready)?["ready"]?.GetValue<bool>() != true)
        {
            process.Kill();
            throw new InvalidOperationException($"transform {transformScript} failed to load");
        }
        _process = process;
        _stdin = process.StandardInput;
        _stdout = stdout;
        _nextId = 1;
    }

    private async Task<JsonNode> CallAsync(JsonNode record, JsonObject context, CancellationToken ct)
    {
        var id = _nextId++;
        var request = new JsonObject { ["id"] = id, ["record"] = record.DeepClone(), ["context"] = context.DeepClone() };
        await _stdin!.WriteLineAsync(request.ToJsonString());
        await _stdin.FlushAsync(ct);
        var line = await _stdout!.ReadLineAsync(ct) ?? throw new IOException("transform process exited");
        var reply = JsonNode.Parse(line) ?? throw new InvalidDataException("invalid reply from transform process");
        if (reply["id"]?.GetValue<long>() != id)
            throw new InvalidDataException("transform reply out of order");
        return reply;
    }

    public static Outcome ParseReply(JsonNode reply)
    {
        if (reply["error"] is JsonObject err)
        {
            var kind = err["kind"]?.GetValue<string>() ?? "transform";
            var message = err["message"]?.GetValue<string>() ?? "transform failed";
            if (err["trace"]?.GetValue<string>() is { } trace)
                message = $"{message}\n{trace}";
            return new Outcome.Rejected(kind, message);
        }
        var resources = (reply["resources"] as JsonArray)?.Where(r => r is not null).Select(r => r!.DeepClone()).ToList() ?? [];
        return new Outcome.Resources(resources);
    }

    private void Kill()
    {
        try
        {
            _process?.Kill();
        }
        catch (InvalidOperationException)
        {
        }
        _process?.Dispose();
        _process = null;
        _stdin = null;
        _stdout = null;
    }

    public void Dispose()
    {
        Kill();
        _lock.Dispose();
    }
}
