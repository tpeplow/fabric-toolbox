using PathResolution.Abstractions;

namespace PathResolution.Internal;

internal sealed class SecurityContextProvider : ISecurityContextProvider
{
    private readonly HttpClient _http;
    public SecurityContextProvider(HttpClient http) => _http = http;

    public string GetContextKey()
    {
        var authHeader = _http.DefaultRequestHeaders.Authorization;
        if (authHeader is not null && authHeader.Scheme.Equals("Bearer", StringComparison.OrdinalIgnoreCase))
        {
            var token = authHeader.Parameter;
            var key = SecurityContextKeyUtility.TryCompute(token);
            if (!string.IsNullOrEmpty(key)) return key;
        }
        return "_shared";
    }
}
