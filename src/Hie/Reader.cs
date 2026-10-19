using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Nodes;

namespace Hie;

/// <summary>
/// One record cut from a source object. Line is the 1-based line in the file for CSV and
/// NDJSON, and the 1-based entry index for a FHIR Bundle, so a rejection can be reported
/// as "line 3 of labs.csv" or "entry 12 of patient.json".
/// </summary>
public sealed record SourceRecord(long Line, JsonNode Raw, string? ParseError);

public static class Reader
{
    public static string Sha256Hex(byte[] bytes) => Convert.ToHexStringLower(SHA256.HashData(bytes));

    /// <summary>Decodes UTF-8 (with or without a byte-order mark) and UTF-16 with a byte-order mark.</summary>
    public static string DecodeText(byte[] bytes)
    {
        if (bytes.Length >= 3 && bytes[0] == 0xEF && bytes[1] == 0xBB && bytes[2] == 0xBF)
            return Strict(Encoding.UTF8).GetString(bytes, 3, bytes.Length - 3);
        if (bytes.Length >= 2 && bytes[0] == 0xFF && bytes[1] == 0xFE)
            return Strict(Encoding.Unicode).GetString(bytes, 2, bytes.Length - 2);
        if (bytes.Length >= 2 && bytes[0] == 0xFE && bytes[1] == 0xFF)
            return Strict(Encoding.BigEndianUnicode).GetString(bytes, 2, bytes.Length - 2);
        try
        {
            return Strict(Encoding.UTF8).GetString(bytes);
        }
        catch (DecoderFallbackException)
        {
            throw new InvalidDataException("file is not UTF-8 or UTF-16 with a byte-order mark");
        }
    }

    private static Encoding Strict(Encoding e)
    {
        var strict = (Encoding)e.Clone();
        strict.DecoderFallback = DecoderFallback.ExceptionFallback;
        return strict;
    }

    /// <summary>Cuts a source object into records according to the feed's format.</summary>
    public static List<SourceRecord> Read(Format format, byte[] bytes)
    {
        var text = DecodeText(bytes);
        return format switch
        {
            Format.Csv => ReadCsv(text),
            Format.Ndjson => ReadNdjson(text),
            Format.FhirBundle => ReadFhirBundle(text),
            _ => throw new ArgumentOutOfRangeException(nameof(format)),
        };
    }

    /// <summary>
    /// Reads a CSV with a header row into one JSON object per data row, keyed by column name.
    /// A row whose column count differs from the header is kept, marked with a parse error,
    /// instead of failing the whole file.
    /// </summary>
    public static List<SourceRecord> ReadCsv(string text)
    {
        var rows = Csv.Parse(text);
        if (rows.Count == 0 || rows[0].Fields.All(string.IsNullOrEmpty))
            throw new InvalidDataException("CSV has no header row");
        var headers = rows[0].Fields;
        var output = new List<SourceRecord>();
        foreach (var row in rows.Skip(1))
        {
            if (row.Fields.All(string.IsNullOrEmpty))
                continue;
            if (row.Fields.Count != headers.Count)
            {
                var fields = new JsonArray(row.Fields.Select(f => (JsonNode)f).ToArray());
                output.Add(new SourceRecord(row.Line, new JsonObject { ["_fields"] = fields },
                    $"expected {headers.Count} columns ({string.Join(",", headers)}), found {row.Fields.Count}"));
                continue;
            }
            var obj = new JsonObject();
            for (var i = 0; i < headers.Count; i++)
                obj[headers[i]] = row.Fields[i];
            output.Add(new SourceRecord(row.Line, obj, null));
        }
        return output;
    }

    /// <summary>Reads NDJSON: one JSON object per line. A bad line is kept as a rejected record.</summary>
    public static List<SourceRecord> ReadNdjson(string text)
    {
        var output = new List<SourceRecord>();
        var lines = text.Split('\n');
        for (var i = 0; i < lines.Length; i++)
        {
            var line = lines[i].TrimEnd('\r');
            if (string.IsNullOrWhiteSpace(line))
                continue;
            long lineNo = i + 1;
            JsonNode? node;
            try
            {
                node = JsonNode.Parse(line);
            }
            catch (System.Text.Json.JsonException e)
            {
                output.Add(new SourceRecord(lineNo, new JsonObject { ["_text"] = line }, $"invalid JSON: {e.Message}"));
                continue;
            }
            if (node is JsonObject obj)
                output.Add(new SourceRecord(lineNo, obj, null));
            else
                output.Add(new SourceRecord(lineNo, new JsonObject { ["_text"] = line }, "line is not a JSON object"));
        }
        return output;
    }

    /// <summary>
    /// Reads a FHIR Bundle as one record per entry resource; a file holding a single resource
    /// is one record. Entries are delivered independently, so references between them that
    /// use the entry's fullUrl (Synthea writes urn:uuid:...) are rewritten to Type/id.
    /// </summary>
    public static List<SourceRecord> ReadFhirBundle(string text)
    {
        JsonNode doc;
        try
        {
            doc = JsonNode.Parse(text) ?? throw new InvalidDataException("invalid JSON");
        }
        catch (System.Text.Json.JsonException e)
        {
            throw new InvalidDataException($"invalid JSON: {e.Message}");
        }
        var rt = doc["resourceType"]?.GetValue<string>();
        if (rt is null)
            throw new InvalidDataException("JSON has no resourceType");
        if (rt != "Bundle")
            return [new SourceRecord(1, doc, null)];

        var entries = doc["entry"] as JsonArray ?? [];
        var urls = new Dictionary<string, string>();
        foreach (var e in entries)
        {
            var full = e?["fullUrl"]?.GetValue<string>();
            var res = e?["resource"];
            var type = res?["resourceType"]?.GetValue<string>();
            var id = res?["id"]?.GetValue<string>();
            if (full is not null && type is not null && id is not null)
                urls[full] = $"{type}/{id}";
        }
        var output = new List<SourceRecord>(entries.Count);
        for (var i = 0; i < entries.Count; i++)
        {
            long line = i + 1;
            var entry = entries[i];
            if (entry?["resource"] is JsonObject res)
            {
                var raw = (JsonObject)res.DeepClone();
                if (urls.Count > 0)
                    RewriteReferences(raw, urls);
                output.Add(new SourceRecord(line, raw, null));
            }
            else
            {
                output.Add(new SourceRecord(line, entry?.DeepClone() ?? new JsonObject(), "entry has no resource"));
            }
        }
        return output;
    }

    private static void RewriteReferences(JsonNode node, Dictionary<string, string> urls)
    {
        switch (node)
        {
            case JsonObject obj:
                foreach (var key in obj.Select(p => p.Key).ToList())
                {
                    var value = obj[key];
                    if (key == "reference" && value is JsonValue v && v.TryGetValue<string>(out var s) && urls.TryGetValue(s, out var target))
                    {
                        obj[key] = target;
                        continue;
                    }
                    if (value is not null)
                        RewriteReferences(value, urls);
                }
                break;
            case JsonArray arr:
                foreach (var item in arr)
                    if (item is not null)
                        RewriteReferences(item, urls);
                break;
        }
    }
}

/// <summary>A small RFC 4180 parser that keeps the file line each record started on.</summary>
public static class Csv
{
    public sealed record Row(long Line, List<string> Fields);

    public static List<Row> Parse(string text)
    {
        var rows = new List<Row>();
        var fields = new List<string>();
        var field = new StringBuilder();
        long line = 1, rowLine = 1;
        var inQuotes = false;
        var anyField = false;
        for (var i = 0; i < text.Length; i++)
        {
            var c = text[i];
            if (inQuotes)
            {
                if (c == '"')
                {
                    if (i + 1 < text.Length && text[i + 1] == '"')
                    {
                        field.Append('"');
                        i++;
                    }
                    else
                    {
                        inQuotes = false;
                    }
                }
                else
                {
                    if (c == '\n')
                        line++;
                    field.Append(c);
                }
                continue;
            }
            switch (c)
            {
                case '"':
                    inQuotes = true;
                    anyField = true;
                    break;
                case ',':
                    fields.Add(field.ToString().Trim());
                    field.Clear();
                    anyField = true;
                    break;
                case '\r':
                    break;
                case '\n':
                    if (anyField || field.Length > 0)
                    {
                        fields.Add(field.ToString().Trim());
                        rows.Add(new Row(rowLine, fields));
                    }
                    fields = [];
                    field.Clear();
                    anyField = false;
                    line++;
                    rowLine = line;
                    break;
                default:
                    field.Append(c);
                    anyField = true;
                    break;
            }
        }
        if (anyField || field.Length > 0)
        {
            fields.Add(field.ToString().Trim());
            rows.Add(new Row(rowLine, fields));
        }
        return rows;
    }
}
