using System.Text;
using System.Text.Json.Nodes;

namespace Hie;

public sealed record AppState(Db Db, IReadOnlyList<FeedConfig> Feeds);

/// <summary>The HTTP API a client UI calls, and the Prometheus metrics endpoint.</summary>
public static class Api
{
    private static string Iso(string column) => $"strftime('%Y-%m-%dT%H:%M:%fZ', {column}/1000.0, 'unixepoch')";

    public static void MapHieApi(this WebApplication app, AppState state)
    {
        app.MapGet("/health", () => Results.Json(new JsonObject { ["status"] = "ok" }));

        app.MapGet("/api/feeds", () =>
        {
            var feeds = new JsonArray(state.Feeds.Select(f => (JsonNode)FeedStatus(state.Db, f)).ToArray());
            return Results.Json(new JsonObject { ["feeds"] = feeds });
        });

        app.MapGet("/api/feeds/{id}/files", (string id, long? limit) =>
        {
            var rows = state.Db.QueryJson(
                $"""
                SELECT f.id, f.name, f.status, f.duplicate_of, f.size, f.error,
                       {Iso("f.received_at")} AS received_at, {Iso("f.finished_at")} AS finished_at,
                       COUNT(r.id) AS records,
                       COALESCE(SUM(r.status = 'sent'), 0) AS sent,
                       COALESCE(SUM(r.status = 'rejected'), 0) AS rejected,
                       COALESCE(SUM(r.status IN ('received', 'pending')), 0) AS in_progress
                FROM files f LEFT JOIN records r ON r.file_id = f.id
                WHERE f.feed_id = @p0 GROUP BY f.id ORDER BY f.id DESC LIMIT @p1
                """, id, limit ?? 50);
            return Results.Json(new JsonObject { ["files"] = ToArray(rows) });
        });

        app.MapGet("/api/records", (string? feed, string? status, long? file_id, long? limit) =>
        {
            var rows = state.Db.QueryJson(
                $"""
                SELECT r.id, r.feed_id, r.file_id, f.name AS file, r.line, r.status, r.error_kind, r.error,
                       r.attempts, {Iso("r.updated_at")} AS updated_at
                FROM records r JOIN files f ON f.id = r.file_id
                WHERE (@p0 IS NULL OR r.feed_id = @p0) AND (@p1 IS NULL OR r.status = @p1) AND (@p2 IS NULL OR r.file_id = @p2)
                ORDER BY r.id DESC LIMIT @p3
                """, feed, status, file_id, limit ?? 100);
            return Results.Json(new JsonObject { ["records"] = ToArray(rows) });
        });

        app.MapGet("/api/records/{id:long}", (long id) =>
        {
            var row = state.Db.QueryJson(
                $"""
                SELECT r.id, r.feed_id, r.file_id, f.name AS file, f.sha256 AS file_sha256, r.line, r.status,
                       r.raw, r.original_raw, r.resources, r.response, r.error_kind, r.error, r.attempts,
                       {Iso("f.received_at")} AS received_at, {Iso("r.updated_at")} AS updated_at
                FROM records r JOIN files f ON f.id = r.file_id WHERE r.id = @p0
                """, id).FirstOrDefault();
            return row is null ? NotFound($"record {id} not found") : Results.Json(row);
        });

        app.MapPost("/api/records/{id:long}/reprocess", async (long id, HttpRequest request) =>
        {
            JsonNode? raw = null;
            if (request.ContentLength is > 0)
            {
                var body = JsonNode.Parse(await request.ReadFromJsonAsync<JsonNode>() is { } n ? n.ToJsonString() : "{}");
                raw = body?["raw"]?.DeepClone();
            }
            if (!state.Db.Reprocess(id, raw))
                return NotFound($"record {id} not found");
            return Results.Json(new JsonObject { ["id"] = id, ["status"] = "received", ["corrected"] = raw is not null }, statusCode: 202);
        });

        app.MapGet("/api/alerts", (string? feed, long? limit) =>
        {
            var rows = state.Db.QueryJson(
                $"""
                SELECT id, feed_id, kind, message, {Iso("created_at")} AS created_at FROM alerts
                WHERE (@p0 IS NULL OR feed_id = @p0) ORDER BY id DESC LIMIT @p1
                """, feed, limit ?? 100);
            return Results.Json(new JsonObject { ["alerts"] = ToArray(rows) });
        });

        app.MapGet("/metrics", () => Results.Text(Metrics(state), "text/plain; version=0.0.4"));
    }

    private static IResult NotFound(string error) => Results.Json(new JsonObject { ["error"] = error }, statusCode: 404);

    private static JsonArray ToArray(List<JsonObject> rows) => new(rows.Select(r => (JsonNode)r).ToArray());

    private static JsonObject FeedStatus(Db db, FeedConfig feed)
    {
        var last = db.LastReceivedAt(feed.Id);
        var freshness = (feed.ExpectEveryDuration, last) switch
        {
            (null, _) => "not_monitored",
            (_, null) => "no_data_yet",
            ({ } w, { } t) when Db.NowMs() - t > (long)w.TotalMilliseconds => "silent",
            _ => "ok",
        };
        var lastAlert = db.QueryJson(
            $"SELECT kind, message, {Iso("created_at")} AS created_at FROM alerts WHERE feed_id = @p0 ORDER BY id DESC LIMIT 1", feed.Id).FirstOrDefault();
        return new JsonObject
        {
            ["id"] = feed.Id,
            ["name"] = feed.Name,
            ["source"] = feed.Source.Describe(),
            ["expect_every"] = feed.ExpectEveryDuration is { } d ? Config.Human(d) : null,
            ["freshness"] = freshness,
            ["last_received_at"] = last is { } ms ? DateTimeOffset.FromUnixTimeMilliseconds(ms).ToString("O") : null,
            ["records"] = db.FeedCounts(feed.Id),
            ["last_alert"] = lastAlert,
        };
    }

    /// <summary>Prometheus text format, so the numbers can live in the same monitoring stack as HFS.</summary>
    public static string Metrics(AppState state)
    {
        var sb = new StringBuilder();
        sb.Append("# TYPE hie_records gauge\n");
        foreach (var f in state.Feeds)
        {
            var counts = state.Db.FeedCounts(f.Id);
            foreach (var status in new[] { "received", "pending", "sent", "rejected" })
                sb.Append($"hie_records{{feed=\"{f.Id}\",status=\"{status}\"}} {counts[status]!.GetValue<long>()}\n");
        }
        sb.Append("# TYPE hie_feed_last_received_timestamp_seconds gauge\n");
        foreach (var f in state.Feeds)
            if (state.Db.LastReceivedAt(f.Id) is { } t)
                sb.Append($"hie_feed_last_received_timestamp_seconds{{feed=\"{f.Id}\"}} {t / 1000}\n");
        sb.Append("# TYPE hie_alerts_total counter\n");
        foreach (var row in state.Db.QueryJson("SELECT COALESCE(feed_id, '') AS feed, kind, COUNT(*) AS n FROM alerts GROUP BY feed_id, kind"))
            sb.Append($"hie_alerts_total{{feed=\"{row["feed"]}\",kind=\"{row["kind"]}\"}} {row["n"]}\n");
        return sb.ToString();
    }
}
