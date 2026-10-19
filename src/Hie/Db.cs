using System.Text.Json.Nodes;
using Microsoft.Data.Sqlite;

namespace Hie;

public sealed record WorkItem(long Id, long FileId, long Line, string FileName, JsonNode Raw, JsonArray? Resources, int Attempts);

public sealed record FinishedFile(long Id, string Name, long Total, long Sent, long Rejected, long NewlyRejected, (long Line, string Error)? FirstRejection);

/// <summary>
/// The engine's store: every object and record with its state, and the alerts.
/// One SQLite connection, serialized; the schema is plain SQL.
/// </summary>
public sealed class Db : IDisposable
{
    private const string Schema = """
        CREATE TABLE IF NOT EXISTS files (
          id INTEGER PRIMARY KEY,
          feed_id TEXT NOT NULL,
          name TEXT NOT NULL,
          sha256 TEXT NOT NULL,
          size INTEGER NOT NULL,
          status TEXT NOT NULL,
          duplicate_of INTEGER,
          received_at INTEGER NOT NULL,
          finished_at INTEGER,
          error TEXT,
          rejected_reported INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS files_feed_sha ON files(feed_id, sha256);
        CREATE INDEX IF NOT EXISTS files_feed_received ON files(feed_id, received_at);
        CREATE TABLE IF NOT EXISTS records (
          id INTEGER PRIMARY KEY,
          file_id INTEGER NOT NULL REFERENCES files(id),
          feed_id TEXT NOT NULL,
          line INTEGER NOT NULL,
          raw TEXT NOT NULL,
          original_raw TEXT,
          status TEXT NOT NULL,
          resources TEXT,
          error_kind TEXT,
          error TEXT,
          response TEXT,
          attempts INTEGER NOT NULL DEFAULT 0,
          next_attempt_at INTEGER NOT NULL DEFAULT 0,
          updated_at INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS records_feed_status ON records(feed_id, status, next_attempt_at);
        CREATE INDEX IF NOT EXISTS records_file ON records(file_id);
        CREATE TABLE IF NOT EXISTS alerts (
          id INTEGER PRIMARY KEY,
          feed_id TEXT,
          kind TEXT NOT NULL,
          message TEXT NOT NULL,
          created_at INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS alerts_feed_kind ON alerts(feed_id, kind, created_at);
        """;

    private readonly SqliteConnection _conn;
    private readonly Lock _lock = new();

    public static long NowMs() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    private Db(SqliteConnection conn)
    {
        _conn = conn;
        Exec("PRAGMA journal_mode = WAL; PRAGMA foreign_keys = ON; PRAGMA busy_timeout = 5000;");
        Exec(Schema);
    }

    public static Db Open(string path)
    {
        var dir = Path.GetDirectoryName(Path.GetFullPath(path));
        if (dir is not null)
            Directory.CreateDirectory(dir);
        var conn = new SqliteConnection($"Data Source={path}");
        conn.Open();
        return new Db(conn);
    }

    public static Db InMemory()
    {
        var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        return new Db(conn);
    }

    private SqliteCommand Cmd(string sql, params object?[] args)
    {
        var cmd = _conn.CreateCommand();
        cmd.CommandText = sql;
        for (var i = 0; i < args.Length; i++)
            cmd.Parameters.AddWithValue($"@p{i}", args[i] ?? DBNull.Value);
        return cmd;
    }

    private void Exec(string sql, params object?[] args)
    {
        lock (_lock)
        {
            using var cmd = Cmd(sql, args);
            cmd.ExecuteNonQuery();
        }
    }

    private int ExecCount(string sql, params object?[] args)
    {
        lock (_lock)
        {
            using var cmd = Cmd(sql, args);
            return cmd.ExecuteNonQuery();
        }
    }

    private T? Scalar<T>(string sql, params object?[] args)
    {
        lock (_lock)
        {
            using var cmd = Cmd(sql, args);
            var v = cmd.ExecuteScalar();
            return v is null or DBNull ? default : (T)Convert.ChangeType(v, Nullable.GetUnderlyingType(typeof(T)) ?? typeof(T));
        }
    }

    public long? FindFileByHash(string feed, string sha)
        => Scalar<long?>("SELECT id FROM files WHERE feed_id = @p0 AND sha256 = @p1 AND status <> 'duplicate' ORDER BY id LIMIT 1", feed, sha);

    public long InsertFile(string feed, string name, string sha, long size, string status, long? duplicateOf)
    {
        lock (_lock)
        {
            using var cmd = Cmd(
                """
                INSERT INTO files (feed_id, name, sha256, size, status, duplicate_of, received_at, finished_at)
                VALUES (@p0, @p1, @p2, @p3, @p4, @p5, @p6, CASE WHEN @p4 = 'processing' THEN NULL ELSE @p6 END);
                SELECT last_insert_rowid();
                """, feed, name, sha, size, status, duplicateOf, NowMs());
            return (long)cmd.ExecuteScalar()!;
        }
    }

    public void FailFile(long fileId, string error)
        => Exec("UPDATE files SET status = 'unreadable', error = @p1, finished_at = @p2 WHERE id = @p0", fileId, error, NowMs());

    public void InsertRecords(long fileId, string feed, IEnumerable<SourceRecord> rows)
    {
        lock (_lock)
        {
            using var tx = _conn.BeginTransaction();
            var now = NowMs();
            foreach (var r in rows)
            {
                using var cmd = Cmd(
                    """
                    INSERT INTO records (file_id, feed_id, line, raw, status, error_kind, error, updated_at)
                    VALUES (@p0, @p1, @p2, @p3, @p4, @p5, @p6, @p7)
                    """,
                    fileId, feed, r.Line, r.Raw.ToJsonString(),
                    r.ParseError is null ? "received" : "rejected",
                    r.ParseError is null ? null : "parse", r.ParseError, now);
                cmd.Transaction = tx;
                cmd.ExecuteNonQuery();
            }
            tx.Commit();
        }
    }

    private List<WorkItem> WorkItems(string sql, params object?[] args)
    {
        lock (_lock)
        {
            using var cmd = Cmd(sql, args);
            using var r = cmd.ExecuteReader();
            var items = new List<WorkItem>();
            while (r.Read())
            {
                var raw = JsonNode.Parse(r.GetString(4)) ?? new JsonObject();
                var resources = r.IsDBNull(5) ? null : JsonNode.Parse(r.GetString(5)) as JsonArray;
                items.Add(new WorkItem(r.GetInt64(0), r.GetInt64(1), r.GetInt64(2), r.GetString(3), raw, resources, r.GetInt32(6)));
            }
            return items;
        }
    }

    public List<WorkItem> ReceivedRecords(string feed, int limit) => WorkItems(
        """
        SELECT r.id, r.file_id, r.line, f.name, r.raw, r.resources, r.attempts
        FROM records r JOIN files f ON f.id = r.file_id
        WHERE r.feed_id = @p0 AND r.status = 'received' ORDER BY r.id LIMIT @p1
        """, feed, limit);

    public List<WorkItem> DueRecords(string feed, int limit) => WorkItems(
        """
        SELECT r.id, r.file_id, r.line, f.name, r.raw, r.resources, r.attempts
        FROM records r JOIN files f ON f.id = r.file_id
        WHERE r.feed_id = @p0 AND r.status = 'pending' AND r.next_attempt_at <= @p1
        ORDER BY r.id LIMIT @p2
        """, feed, NowMs(), limit);

    public void MarkTransformed(long id, JsonArray resources) => Exec(
        """
        UPDATE records SET status = 'pending', resources = @p1, error_kind = NULL, error = NULL,
               attempts = 0, next_attempt_at = 0, updated_at = @p2 WHERE id = @p0
        """, id, resources.ToJsonString(), NowMs());

    public void MarkRejected(long id, string kind, string error, JsonNode? response) => Exec(
        "UPDATE records SET status = 'rejected', error_kind = @p1, error = @p2, response = @p3, updated_at = @p4 WHERE id = @p0",
        id, kind, error, response?.ToJsonString(), NowMs());

    public void MarkSent(long id, JsonNode response) => Exec(
        "UPDATE records SET status = 'sent', response = @p1, error_kind = NULL, error = NULL, updated_at = @p2 WHERE id = @p0",
        id, response.ToJsonString(), NowMs());

    public void Defer(long id, int attempts, long nextAttemptAt, string error) => Exec(
        "UPDATE records SET attempts = @p1, next_attempt_at = @p2, error_kind = 'transient', error = @p3, updated_at = @p4 WHERE id = @p0",
        id, attempts, nextAttemptAt, error, NowMs());

    /// <summary>Puts a record back at the start of the pipeline, optionally with corrected source data; the original row is kept.</summary>
    public bool Reprocess(long id, JsonNode? raw)
    {
        lock (_lock)
        {
            var n = raw is null
                ? ExecCountUnlocked(
                    """
                    UPDATE records SET status = 'received', resources = NULL, error_kind = NULL, error = NULL,
                           response = NULL, attempts = 0, next_attempt_at = 0, updated_at = @p1 WHERE id = @p0
                    """, id, NowMs())
                : ExecCountUnlocked(
                    """
                    UPDATE records SET status = 'received', original_raw = COALESCE(original_raw, raw), raw = @p1, resources = NULL,
                           error_kind = NULL, error = NULL, response = NULL, attempts = 0, next_attempt_at = 0, updated_at = @p2 WHERE id = @p0
                    """, id, raw.ToJsonString(), NowMs());
            if (n > 0)
                ExecCountUnlocked("UPDATE files SET status = 'processing', finished_at = NULL WHERE id = (SELECT file_id FROM records WHERE id = @p0)", id);
            return n > 0;
        }
    }

    private int ExecCountUnlocked(string sql, params object?[] args)
    {
        using var cmd = Cmd(sql, args);
        return cmd.ExecuteNonQuery();
    }

    /// <summary>Marks files whose records have all reached sent or rejected as done and returns them.</summary>
    public List<FinishedFile> FinishFiles(string feed)
    {
        lock (_lock)
        {
            var done = new List<FinishedFile>();
            using (var cmd = Cmd(
                """
                SELECT f.id, f.name, COUNT(r.id),
                       COALESCE(SUM(r.status = 'sent'), 0),
                       COALESCE(SUM(r.status = 'rejected'), 0),
                       f.rejected_reported
                FROM files f LEFT JOIN records r ON r.file_id = f.id
                WHERE f.feed_id = @p0 AND f.status = 'processing'
                GROUP BY f.id
                HAVING COALESCE(SUM(r.status IN ('received', 'pending')), 0) = 0
                """, feed))
            using (var r = cmd.ExecuteReader())
            {
                while (r.Read())
                {
                    var rejected = r.GetInt64(4);
                    done.Add(new FinishedFile(r.GetInt64(0), r.GetString(1), r.GetInt64(2), r.GetInt64(3), rejected, rejected - r.GetInt64(5), null));
                }
            }
            for (var i = 0; i < done.Count; i++)
            {
                var f = done[i];
                ExecCountUnlocked("UPDATE files SET status = 'done', finished_at = @p1, rejected_reported = MAX(rejected_reported, @p2) WHERE id = @p0", f.Id, NowMs(), f.Rejected);
                using var cmd = Cmd("SELECT line, error FROM records WHERE file_id = @p0 AND status = 'rejected' ORDER BY line LIMIT 1", f.Id);
                using var r = cmd.ExecuteReader();
                if (r.Read())
                    done[i] = f with { FirstRejection = (r.GetInt64(0), r.IsDBNull(1) ? "" : r.GetString(1)) };
            }
            return done;
        }
    }

    public long? LastReceivedAt(string feed)
        => Scalar<long?>("SELECT MAX(received_at) FROM files WHERE feed_id = @p0 AND status <> 'duplicate'", feed);

    public long? LastAlertAt(string feed, string kind)
        => Scalar<long?>("SELECT MAX(created_at) FROM alerts WHERE feed_id = @p0 AND kind = @p1", feed, kind);

    public long InsertAlert(string? feed, string kind, string message)
    {
        lock (_lock)
        {
            using var cmd = Cmd("INSERT INTO alerts (feed_id, kind, message, created_at) VALUES (@p0, @p1, @p2, @p3); SELECT last_insert_rowid();", feed, kind, message, NowMs());
            return (long)cmd.ExecuteScalar()!;
        }
    }

    public JsonObject FeedCounts(string feed)
    {
        var counts = new JsonObject { ["received"] = 0L, ["pending"] = 0L, ["sent"] = 0L, ["rejected"] = 0L };
        foreach (var row in QueryJson("SELECT status, COUNT(*) AS n FROM records WHERE feed_id = @p0 GROUP BY status", feed))
            counts[row["status"]!.GetValue<string>()] = row["n"]!.GetValue<long>();
        counts["files"] = Scalar<long>("SELECT COUNT(*) FROM files WHERE feed_id = @p0 AND status <> 'duplicate'", feed);
        counts["duplicate_files"] = Scalar<long>("SELECT COUNT(*) FROM files WHERE feed_id = @p0 AND status = 'duplicate'", feed);
        return counts;
    }

    /// <summary>Runs a read-only query and returns each row as a JSON object keyed by column name.</summary>
    public List<JsonObject> QueryJson(string sql, params object?[] args)
    {
        lock (_lock)
        {
            using var cmd = Cmd(sql, args);
            using var r = cmd.ExecuteReader();
            var rows = new List<JsonObject>();
            while (r.Read())
            {
                var obj = new JsonObject();
                for (var i = 0; i < r.FieldCount; i++)
                {
                    var name = r.GetName(i);
                    if (r.IsDBNull(i))
                    {
                        obj[name] = null;
                        continue;
                    }
                    obj[name] = r.GetFieldType(i) == typeof(string)
                        ? ParseIfJsonColumn(name, r.GetString(i))
                        : r.GetFieldType(i) == typeof(double)
                            ? JsonValue.Create(r.GetDouble(i))
                            : JsonValue.Create(r.GetInt64(i));
                }
                rows.Add(obj);
            }
            return rows;
        }
    }

    private static JsonNode? ParseIfJsonColumn(string name, string text)
    {
        if (name is "raw" or "original_raw" or "resources" or "response")
        {
            try
            {
                return JsonNode.Parse(text);
            }
            catch (System.Text.Json.JsonException)
            {
            }
        }
        return JsonValue.Create(text);
    }

    public void Dispose() => _conn.Dispose();
}
