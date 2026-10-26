using System.Windows;
using AgentClient.App.ViewModels;
using AgentClient.Core;
using AgentClient.Core.Http;
using AgentClient.Core.WebSockets;
using Microsoft.Extensions.DependencyInjection;
namespace AgentClient.App;

/// <summary>
/// Interaction logic for App.xaml
/// </summary>
public partial class App : Application
{
    public static IServiceProvider Services { get; private set; } = null!;


    protected override void OnStartup(StartupEventArgs e)
    {
        base.OnStartup(e);

        try
        {
            var services = new ServiceCollection();
            ConfigureServices(services);
            Services = services.BuildServiceProvider();

            var window = Services.GetRequiredService<MainWindow>();
            window.Show();
        }
        catch (Exception ex)
        {
            MessageBox.Show(ex.ToString(), "Startup failed",
                MessageBoxButton.OK, MessageBoxImage.Error);
            Shutdown(1);
        }
    }


    private static void ConfigureServices(IServiceCollection services)
    {
        const string baseUrl = "http://127.0.0.1:8787";
        services.AddSingleton(new AgentClientOptions { BaseUrl = baseUrl });
        services.AddHttpClient<AgentApiClient>(c => c.BaseAddress = new Uri(baseUrl));
        services.AddHttpClient<UploadService>(c => c.BaseAddress = new Uri(baseUrl));
        services.AddHttpClient<ProgressStream>(c => c.BaseAddress = new Uri(baseUrl));
        services.AddTransient<MainViewModel>();
        services.AddTransient<MainWindow>();
    }
}

