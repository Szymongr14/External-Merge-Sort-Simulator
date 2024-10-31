using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace ExternalMergeSortSimulator;

public abstract class Program
{
    public static void Main(string[] args)
    {
        // Set up configuration
        var configuration = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
            .Build();
        
        // Validate configuration file
        // TODO: create configuration validation service
        
        // Set up DI container
        var services = new ServiceCollection();
        ConfigureServices(services, configuration);
        var serviceProvider = services.BuildServiceProvider();
    }

    private static void ConfigureServices(ServiceCollection services, IConfiguration configuration)
    {
        services.AddSingleton(configuration);
    }
}