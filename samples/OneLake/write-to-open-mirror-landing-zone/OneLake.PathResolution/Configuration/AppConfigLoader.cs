using System.Text.Json;

namespace PathResolution.Configuration;

public static class AppConfigLoader
{
    public static AppConfig Load()
    {
        return LoadAndValidate();
    }

    private static AppConfig LoadAndValidate()
    {
        var path = Path.Combine(AppContext.BaseDirectory, "appsettings.json");
        if (!File.Exists(path)) throw new InvalidOperationException("Configuration missing (appsettings.json).");
        var json = File.ReadAllText(path);
        using var doc = JsonDocument.Parse(json);
        if (!doc.RootElement.TryGetProperty("OneLake", out var oneLake)) throw new InvalidOperationException("Configuration missing (onelake)."); ;
        var cfg = new AppConfig
        {
            TenantId = oneLake.GetPropertyOrNull("TenantId"),
            ClientId = oneLake.GetPropertyOrNull("ClientId"),
            ClientSecret = oneLake.GetPropertyOrNull("ClientSecret"),
            OneLakeUri = oneLake.GetPropertyOrNull("OneLakeUri"),
            LoginAuthority = oneLake.GetPropertyOrNull("LoginAuthority"),
            StorageScope = oneLake.GetPropertyOrNull("StorageScope")
        };
        var issues = new List<string>();
        Validate(cfg, issues);
        if (issues.Count > 0)
            throw new InvalidOperationException("Invalid configuration: " + string.Join(", ", issues));
        return cfg;
    }

    private static void Validate(AppConfig cfg, List<string> list)
    {
        if (cfg is null) { list.Add("Config missing"); return; }
        if (string.IsNullOrWhiteSpace(cfg.TenantId)) list.Add("TenantId missing");
        if (string.IsNullOrWhiteSpace(cfg.ClientId)) list.Add("ClientId missing");
        if (string.IsNullOrWhiteSpace(cfg.ClientSecret)) list.Add("ClientSecret missing");
        if (string.IsNullOrWhiteSpace(cfg.OneLakeUri)) list.Add("OneLakeUri missing");
        if (string.IsNullOrWhiteSpace(cfg.LoginAuthority)) list.Add("LoginAuthority missing");
        if (string.IsNullOrWhiteSpace(cfg.StorageScope)) list.Add("StorageScope missing");
    }
}

internal static class AppConfigJsonExtensions
{
    public static string? GetPropertyOrNull(this JsonElement element, string name)
    {
        if (element.TryGetProperty(name, out var prop))
        {
            return prop.ValueKind == JsonValueKind.String ? prop.GetString() : prop.ToString();
        }
        return null;
    }
}
