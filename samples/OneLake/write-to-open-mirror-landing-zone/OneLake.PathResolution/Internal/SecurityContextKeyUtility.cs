using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace PathResolution.Internal;

internal static class SecurityContextKeyUtility
{
    // Derive a stable key from a JWT bearer token by hashing payload without lifetime claims.
    // Returns null if token cannot be parsed.
    public static string? TryCompute(string? bearerToken)
    {
        if (string.IsNullOrWhiteSpace(bearerToken)) return null;
        var parts = bearerToken.Split('.');
        if (parts.Length < 2) return null;
        try
        {
            var payloadJson = Base64UrlDecodeToUtf8(parts[1]);
            using var doc = JsonDocument.Parse(payloadJson);
            var root = doc.RootElement;

            var filtered = new SortedDictionary<string, JsonElement>(StringComparer.Ordinal);
            foreach (var prop in root.EnumerateObject())
            {
                if (prop.NameEquals("exp") || prop.NameEquals("iat") || prop.NameEquals("nbf")) continue;
                filtered[prop.Name] = prop.Value;
            }

            // Serialize deterministically
            var buffer = new System.Buffers.ArrayBufferWriter<byte>();
            using (var writer = new Utf8JsonWriter(buffer))
            {
                writer.WriteStartObject();
                foreach (var kvp in filtered)
                {
                    writer.WritePropertyName(kvp.Key);
                    kvp.Value.WriteTo(writer);
                }
                writer.WriteEndObject();
                writer.Flush();
            }
            var canonical = Encoding.UTF8.GetString(buffer.WrittenSpan);
            using var sha = SHA256.Create();
            var hash = sha.ComputeHash(Encoding.UTF8.GetBytes(canonical));
            return Convert.ToHexString(hash); // upper-case hex
        }
        catch
        {
            return null;
        }
    }

    private static string Base64UrlDecodeToUtf8(string base64Url)
    {
        string padded = base64Url.Replace('-', '+').Replace('_', '/');
        switch (padded.Length % 4)
        {
            case 2: padded += "=="; break;
            case 3: padded += "="; break;
        }
        var bytes = Convert.FromBase64String(padded);
        return Encoding.UTF8.GetString(bytes);
    }
}
