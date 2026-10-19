using System.Text.Json.Nodes;

namespace Hie;

/// <summary>
/// Records every alert and, when configured, posts it to a webhook. The payload's
/// text field is what Slack and Teams incoming webhooks display.
/// </summary>
public sealed class Alerter(Db db, string? webhookUrl, HttpClient http, ILogger logger)
{
    public void Raise(string? feed, string kind, string message)
    {
        var id = db.InsertAlert(feed, kind, message);
        logger.LogWarning("alert {AlertId} {Kind} feed={Feed}: {Message}", id, kind, feed, message);
        if (webhookUrl is null)
            return;
        var payload = new JsonObject { ["text"] = $"[HIE] {message}", ["feed"] = feed, ["kind"] = kind, ["alert_id"] = id };
        _ = Task.Run(async () =>
        {
            try
            {
                using var content = new StringContent(payload.ToJsonString(), System.Text.Encoding.UTF8, "application/json");
                using var response = await http.PostAsync(webhookUrl, content);
                response.EnsureSuccessStatusCode();
            }
            catch (Exception e) when (e is HttpRequestException or TaskCanceledException)
            {
                logger.LogError("alert webhook failed: {Error}", e.Message);
            }
        });
    }
}
