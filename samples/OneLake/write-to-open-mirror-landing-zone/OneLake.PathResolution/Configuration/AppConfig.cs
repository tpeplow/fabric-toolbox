namespace PathResolution.Configuration
{
    public sealed class AppConfig
    {
        public string? TenantId { get; set; }
        public string? ClientId { get; set; }
        public string? ClientSecret { get; set; }
        public string? OneLakeUri { get; set; }
        public string? LoginAuthority { get; set; }
        public string? StorageScope { get; set; }
    }
}