using Amazon;
using Amazon.S3;
using Amazon.S3.Model;

namespace Hie;

/// <summary>One object fetched from a source: a file, an S3 object.</summary>
public sealed record Incoming(string Key, string Name, byte[] Bytes);

/// <summary>
/// Where a feed's objects come from. List names what is waiting, Fetch reads one object,
/// and Acknowledge moves it out of the way once its records are stored.
/// </summary>
public interface ISource
{
    Task<List<string>> ListAsync(CancellationToken ct = default);
    Task<Incoming> FetchAsync(string key, CancellationToken ct = default);
    Task AcknowledgeAsync(string key, string archivedName, CancellationToken ct = default);
}

public static class Sources
{
    public static ISource FromConfig(FeedConfig feed)
    {
        var s = feed.Source;
        if (s.Type == "folder")
            return new FolderSource(s.Path!, s.Pattern, s.MinFileAge, s.Archive ?? Path.Combine(s.Path!, "processed"));

        var config = new AmazonS3Config();
        if (s.Region is not null)
            config.RegionEndpoint = RegionEndpoint.GetBySystemName(s.Region);
        if (s.Endpoint is not null)
        {
            config.ServiceURL = s.Endpoint;
            config.ForcePathStyle = true;
        }
        var store = new S3ObjectStore(new AmazonS3Client(config), s.Bucket!);
        return new ObjectSource(store, s.Prefix, s.ArchivePrefix, s.Pattern, s.MinFileAge);
    }
}

/// <summary>A local folder, polled. A file is taken once it has not changed for MinAge.</summary>
public sealed class FolderSource(string dir, string pattern, TimeSpan minAge, string archive) : ISource
{
    public Task<List<string>> ListAsync(CancellationToken ct = default)
    {
        Directory.CreateDirectory(dir);
        var keys = new List<string>();
        foreach (var path in Directory.GetFiles(dir))
        {
            var name = Path.GetFileName(path);
            if (!Config.GlobMatch(pattern, name))
                continue;
            if (DateTime.UtcNow - File.GetLastWriteTimeUtc(path) < minAge)
                continue;
            keys.Add(path);
        }
        keys.Sort(StringComparer.Ordinal);
        return Task.FromResult(keys);
    }

    public async Task<Incoming> FetchAsync(string key, CancellationToken ct = default)
        => new(key, Path.GetFileName(key), await File.ReadAllBytesAsync(key, ct));

    public Task AcknowledgeAsync(string key, string archivedName, CancellationToken ct = default)
    {
        Directory.CreateDirectory(archive);
        File.Move(key, Path.Combine(archive, archivedName), overwrite: true);
        return Task.CompletedTask;
    }
}

/// <summary>The subset of an object store the engine needs; S3 in production, memory in tests.</summary>
public interface IObjectStore
{
    Task<List<(string Key, DateTimeOffset LastModified)>> ListAsync(string? prefix, CancellationToken ct = default);
    Task<byte[]> GetAsync(string key, CancellationToken ct = default);
    Task RenameAsync(string from, string to, CancellationToken ct = default);
}

public sealed class S3ObjectStore(IAmazonS3 s3, string bucket) : IObjectStore
{
    public async Task<List<(string Key, DateTimeOffset LastModified)>> ListAsync(string? prefix, CancellationToken ct = default)
    {
        var result = new List<(string, DateTimeOffset)>();
        var request = new ListObjectsV2Request { BucketName = bucket, Prefix = prefix };
        ListObjectsV2Response response;
        do
        {
            response = await s3.ListObjectsV2Async(request, ct);
            foreach (var o in response.S3Objects ?? [])
                result.Add((o.Key, o.LastModified ?? DateTimeOffset.MinValue));
            request.ContinuationToken = response.NextContinuationToken;
        } while (response.IsTruncated == true);
        return result;
    }

    public async Task<byte[]> GetAsync(string key, CancellationToken ct = default)
    {
        using var response = await s3.GetObjectAsync(bucket, key, ct);
        using var buffer = new MemoryStream();
        await response.ResponseStream.CopyToAsync(buffer, ct);
        return buffer.ToArray();
    }

    public async Task RenameAsync(string from, string to, CancellationToken ct = default)
    {
        await s3.CopyObjectAsync(bucket, from, bucket, to, ct);
        await s3.DeleteObjectAsync(bucket, from, ct);
    }
}

public sealed class MemoryObjectStore : IObjectStore
{
    private readonly Dictionary<string, (byte[] Bytes, DateTimeOffset LastModified)> _objects = new();

    public void Put(string key, byte[] bytes, DateTimeOffset? lastModified = null)
        => _objects[key] = (bytes, lastModified ?? DateTimeOffset.UtcNow);

    public bool Contains(string key) => _objects.ContainsKey(key);

    public Task<List<(string Key, DateTimeOffset LastModified)>> ListAsync(string? prefix, CancellationToken ct = default)
        => Task.FromResult(_objects
            .Where(p => prefix is null || p.Key.StartsWith(prefix, StringComparison.Ordinal))
            .Select(p => (p.Key, p.Value.LastModified))
            .ToList());

    public Task<byte[]> GetAsync(string key, CancellationToken ct = default)
        => _objects.TryGetValue(key, out var o) ? Task.FromResult(o.Bytes) : throw new FileNotFoundException(key);

    public Task RenameAsync(string from, string to, CancellationToken ct = default)
    {
        _objects[to] = _objects[from];
        _objects.Remove(from);
        return Task.CompletedTask;
    }
}

/// <summary>
/// An object store polled by listing a prefix. Acknowledged objects are renamed under
/// the archive prefix, which the listing skips.
/// </summary>
public sealed class ObjectSource(IObjectStore store, string prefix, string archivePrefix, string pattern, TimeSpan minAge) : ISource
{
    private readonly string? _prefix = string.IsNullOrEmpty(prefix.Trim('/')) ? null : prefix.Trim('/') + "/";
    private readonly string _archive = archivePrefix.Trim('/');

    public async Task<List<string>> ListAsync(CancellationToken ct = default)
    {
        var keys = new List<string>();
        foreach (var (key, lastModified) in await store.ListAsync(_prefix, ct))
        {
            if (key.StartsWith(_archive + "/", StringComparison.Ordinal))
                continue;
            var name = key[(key.LastIndexOf('/') + 1)..];
            if (name.Length == 0 || !Config.GlobMatch(pattern, name))
                continue;
            if (DateTimeOffset.UtcNow - lastModified < minAge)
                continue;
            keys.Add(key);
        }
        keys.Sort(StringComparer.Ordinal);
        return keys;
    }

    public async Task<Incoming> FetchAsync(string key, CancellationToken ct = default)
        => new(key, key[(key.LastIndexOf('/') + 1)..], await store.GetAsync(key, ct));

    public Task AcknowledgeAsync(string key, string archivedName, CancellationToken ct = default)
        => store.RenameAsync(key, $"{_archive}/{archivedName}", ct);
}
