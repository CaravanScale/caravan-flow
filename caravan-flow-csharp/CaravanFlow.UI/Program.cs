using Microsoft.AspNetCore.Components.Web;
using Microsoft.AspNetCore.Components.WebAssembly.Hosting;
using CaravanFlow.UI;
using CaravanFlow.UI.Services;

var builder = WebAssemblyHostBuilder.CreateDefault(args);
builder.RootComponents.Add<App>("#app");
builder.RootComponents.Add<HeadOutlet>("head::after");

// FlowApiClient — typed wrapper over the worker's management API.
// Base address: wherever this bundle is served from. In production the
// worker serves the bundle as static assets from its own port, so
// HostEnvironment.BaseAddress == the worker URL. In dev, the WebAssembly
// DevServer sets it to the dev server URL; set CARAVAN_FLOW_WORKER_URL
// via appsettings.Development.json or a proxy in that case.
builder.Services.AddScoped(sp => new HttpClient
{
    BaseAddress = new Uri(builder.HostEnvironment.BaseAddress),
});
builder.Services.AddScoped<FlowApiClient>();

await builder.Build().RunAsync();
