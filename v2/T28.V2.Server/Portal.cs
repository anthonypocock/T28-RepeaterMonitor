using System.Reflection;

namespace T28.V2.Server;

public static class Portal
{
    public static readonly string Html = ReadAsset("channel.html");
    public static readonly string ListenerIndexHtml = ReadAsset("listener.html");
    public static readonly string Worklet = ReadAsset("audio-worklet.js");
    public static readonly string FleetHtml = ReadAsset("fleet.html");

    public static string Page(string page, bool fleet, string instrumentation) =>
        Html.Replace("__T28_PAGE__", page, StringComparison.Ordinal)
            .Replace("__T28_TITLE__", page == "admin" ? "Admin" : "Listener", StringComparison.Ordinal)
            .Replace("__T28_TEST_INSTRUMENTATION__", instrumentation, StringComparison.Ordinal)
        + (fleet ? FleetHtml : "");

    private static string ReadAsset(string fileName)
    {
        Assembly assembly = typeof(Portal).Assembly;
        string suffix = ".PortalAssets." + fileName;
        string resourceName = assembly.GetManifestResourceNames().SingleOrDefault(name => name.EndsWith(suffix, StringComparison.Ordinal))
            ?? throw new InvalidOperationException($"Missing embedded portal asset {fileName}.");
        using Stream stream = assembly.GetManifestResourceStream(resourceName)
            ?? throw new InvalidOperationException($"Unable to open embedded portal asset {fileName}.");
        using StreamReader reader = new(stream);
        return reader.ReadToEnd();
    }
}
