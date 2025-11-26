using System.Text.Json.Serialization;

namespace PathResolution.Models;

[JsonConverter(typeof(JsonStringEnumConverter))]
public enum ResolutionResult
{
    AllPathsResolved,
    ResolveChildPath,
    NotSupported
}
