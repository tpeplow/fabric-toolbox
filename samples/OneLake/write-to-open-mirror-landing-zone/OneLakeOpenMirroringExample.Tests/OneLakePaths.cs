namespace OneLakeOpenMirroringExample.Tests;

public static class OneLakePaths
{
    public static string WorkspaceName => "TomTestWorkspace";
    public static string LakehouseName => "Demo.Lakehouse";

    public static OpenMirroredTableId CreateFabricPricePaidTableId()
    {
        return new OpenMirroredTableId(
            WorkspaceName: WorkspaceName,
            MirroredDatabaseName: "HousePriceOpenMirror.MountedRelationalDatabase",
            TableName: "PricePaid");
    }

    public static OpenMirroredTableId CreateEmulatorPricePaidTableId()
    {
        return new OpenMirroredTableId(
            WorkspaceName: $"workspace-{Guid.NewGuid()}",
            MirroredDatabaseName: "database",
            TableName: "table");
    }

    internal static string? GetLakehouseFilesPath()
    {
        return LakehouseName + "/Files/";
    }
}
