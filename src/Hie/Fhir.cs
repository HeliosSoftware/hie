using System.Net.Http.Headers;
using System.Text;
using System.Text.Json.Nodes;

namespace Hie;

/// <summary>What makes a resource's delivery idempotent: its own id, or a business identifier.</summary>
public sealed record DeliveryKey(string ResourceType, string? Id, string? System, string? Value)
{
    public string RequestUrl() => Id is not null
        ? $"{ResourceType}/{Fhir.Encode(Id)}"
        : $"{ResourceType}?identifier={Fhir.Encode(System!)}|{Fhir.Encode(Value!)}";
}

public abstract record SendResult
{
    public sealed record Ok(JsonNode Body) : SendResult;
    /// <summary>Connection failure, timeout, 429 or 5xx: try again later.</summary>
    public sealed record Transient(string Reason) : SendResult;
    /// <summary>The server refused the Bundle itself (4xx).</summary>
    public sealed record Refused(int Status, JsonNode Body) : SendResult;
}

public enum EntryKind { Ok, Rejected, Transient }

public sealed record EntryResult(EntryKind Kind, int Status, JsonNode Response);

public sealed class FhirClient(HttpClient http, string baseUrl)
{
    private readonly string _base = baseUrl.TrimEnd('/');

    public async Task<SendResult> PostBundleAsync(JsonNode bundle, CancellationToken ct = default)
    {
        HttpResponseMessage response;
        try
        {
            using var content = new StringContent(bundle.ToJsonString(), Encoding.UTF8);
            content.Headers.ContentType = new MediaTypeHeaderValue("application/fhir+json");
            using var request = new HttpRequestMessage(HttpMethod.Post, _base + "/") { Content = content };
            request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/fhir+json"));
            response = await http.SendAsync(request, ct);
        }
        catch (Exception e) when (e is HttpRequestException or TaskCanceledException)
        {
            return new SendResult.Transient($"cannot reach {_base}: {e.Message}");
        }
        using (response)
        {
            var status = (int)response.StatusCode;
            JsonNode body;
            try
            {
                body = JsonNode.Parse(await response.Content.ReadAsStringAsync(ct)) ?? new JsonObject();
            }
            catch (System.Text.Json.JsonException)
            {
                body = new JsonObject();
            }
            return status switch
            {
                >= 200 and <= 299 => new SendResult.Ok(body),
                429 or >= 500 => new SendResult.Transient($"HTTP {status} from {_base}"),
                _ => new SendResult.Refused(status, body),
            };
        }
    }
}

public static class Fhir
{
    /// <summary>Encodes a URL component, keeping the characters a FHIR search value may carry literally.</summary>
    public static string Encode(string s)
    {
        var sb = new StringBuilder(s.Length);
        foreach (var b in Encoding.UTF8.GetBytes(s))
        {
            var c = (char)b;
            if (char.IsAsciiLetterOrDigit(c) || "-._~:/".Contains(c))
                sb.Append(c);
            else
                sb.Append('%').Append(b.ToString("X2"));
        }
        return sb.ToString();
    }

    /// <summary>
    /// Checks what HIE itself relies on: a resource type, and a key that makes delivery
    /// idempotent. A resource with an id is written to Type/id; one without needs a
    /// business identifier and is written as a conditional update on it.
    /// </summary>
    public static (DeliveryKey? Key, string? Error) CheckResource(JsonNode resource)
    {
        var rt = Str(resource["resourceType"]);
        if (rt is null)
            return (null, "resource has no resourceType");
        var id = Str(resource["id"]);
        if (id is not null)
            return (new DeliveryKey(rt, id, null, null), null);
        var ident = (resource["identifier"] as JsonArray)?.FirstOrDefault();
        if (ident is null)
            return (null, $"{rt} has neither id nor identifier; HIE needs one to avoid duplicates on re-send");
        var system = Str(ident["system"]);
        var value = Str(ident["value"]);
        if (system is null || value is null)
            return (null, $"{rt}.identifier[0] needs both system and value");
        return (new DeliveryKey(rt, null, system, value), null);
    }

    private static string? Str(JsonNode? node)
        => node is JsonValue v && v.TryGetValue<string>(out var s) && s.Length > 0 ? s : null;

    /// <summary>A batch Bundle of PUT entries: an entry fails alone, and re-sending updates instead of creating.</summary>
    public static JsonObject BatchBundle(IEnumerable<(string Url, JsonNode Resource)> entries)
    {
        var entry = new JsonArray();
        foreach (var (url, resource) in entries)
            entry.Add(new JsonObject
            {
                ["resource"] = resource.DeepClone(),
                ["request"] = new JsonObject { ["method"] = "PUT", ["url"] = url },
            });
        return new JsonObject { ["resourceType"] = "Bundle", ["type"] = "batch", ["entry"] = entry };
    }

    public static List<EntryResult> EntryResults(JsonNode response, int expected)
    {
        var entries = response["entry"] as JsonArray;
        var results = new List<EntryResult>(expected);
        for (var i = 0; i < expected; i++)
        {
            var entry = entries is not null && i < entries.Count ? entries[i] : null;
            if (entry is null)
            {
                results.Add(new EntryResult(EntryKind.Transient, 0, new JsonObject { ["error"] = "missing entry in batch-response" }));
                continue;
            }
            var resp = entry["response"]?.DeepClone() ?? new JsonObject();
            var statusText = Str(resp["status"]) ?? "";
            var status = int.TryParse(statusText.Split(' ')[0], out var n) ? n : 0;
            var kind = status switch
            {
                >= 200 and <= 299 => EntryKind.Ok,
                429 or >= 500 or 0 => EntryKind.Transient,
                _ => EntryKind.Rejected,
            };
            results.Add(new EntryResult(kind, status, resp));
        }
        return results;
    }

    /// <summary>A short, human-readable reason from an OperationOutcome (or anything else).</summary>
    public static string OutcomeText(JsonNode v)
    {
        var outcome = v["outcome"] ?? v;
        var texts = (outcome["issue"] as JsonArray)?
            .Select(i => Str(i?["details"]?["text"]) ?? Str(i?["diagnostics"]))
            .Where(t => t is not null)
            .ToList() ?? [];
        return texts.Count == 0 ? v.ToJsonString() : string.Join("; ", texts);
    }
}
