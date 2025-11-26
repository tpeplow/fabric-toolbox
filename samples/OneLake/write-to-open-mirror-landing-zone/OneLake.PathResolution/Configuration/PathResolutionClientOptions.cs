namespace PathResolution.Configuration;

public sealed class PathResolutionClientOptions
{
    public Uri BaseAddress { get; set; } = new("https://onelakecst110.dfs.pbidedicated.windows-int.net/");
    public string? ApiVersion { get; set; }
    public TimeSpan RefreshSkew { get; set; } = TimeSpan.FromMinutes(1);
    public TimeSpan BackgroundRefreshTimeout { get; set; } = TimeSpan.FromSeconds(10);
    public int MaxConcurrentBackgroundRefreshes { get; set; } = 4;
    public string? SasToken { get; set; }
}
