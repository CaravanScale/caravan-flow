using Microsoft.AspNetCore.Components.Web;
using Microsoft.AspNetCore.Components.WebAssembly.Hosting;
using CaravanFlow.UI;
using CaravanFlow.UI.Services;

var builder = WebAssemblyHostBuilder.CreateDefault(args);
builder.RootComponents.Add<App>("#app");
builder.RootComponents.Add<HeadOutlet>("head::after");

// FlowApiClient — typed wrapper over the worker's management API.
// Base address resolution:
//   1. ?worker=<url> query string (override for visiting bundles on
//      any host) — kept first so demos don't need appsettings.
//   2. appsettings.json key "worker:url" (dev/prod convention)
//   3. Same origin as the bundle — the target when the worker ships
//      the published bundle as its own static asset.
var workerUrl = builder.Configuration["worker:url"];
var qs = new Uri(builder.HostEnvironment.BaseAddress + "unused").Query;
if (!string.IsNullOrEmpty(qs))
{
    var match = System.Text.RegularExpressions.Regex.Match(qs, @"[?&]worker=([^&]+)");
    if (match.Success) workerUrl = Uri.UnescapeDataString(match.Groups[1].Value);
}
var baseAddr = !string.IsNullOrWhiteSpace(workerUrl)
    ? new Uri(workerUrl.EndsWith("/") ? workerUrl : workerUrl + "/")
    : new Uri(builder.HostEnvironment.BaseAddress);
builder.Services.AddScoped(sp => new HttpClient { BaseAddress = baseAddr });
builder.Services.AddScoped<FlowApiClient>();

await builder.Build().RunAsync();
