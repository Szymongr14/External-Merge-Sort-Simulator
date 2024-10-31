using ExternalMergeSortSimulator.Models;
using ExternalMergeSortSimulator.Validators;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace ExternalMergeSortSimulator;

public abstract class Program
{
    public static void Main()
    {
        // Set up configuration
        var configuration = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
            .Build();
        
        // Set up DI container
        var services = new ServiceCollection();
        ConfigureServices(services);
        var serviceProvider = services.BuildServiceProvider();
        
        // Get logger instance from DI
        var logger = serviceProvider.GetRequiredService<ILogger<Program>>();
        
        // Validate configuration file
        var configurationModel = new AppSettings();
        configuration.GetSection("Settings").Bind(configurationModel);
        ValidateAppSettings(configurationModel, logger);
        services.AddSingleton(configuration);
        
        logger.LogInformation("Application started successfully.");
    }

    private static void ConfigureServices(ServiceCollection services)
    {
        services.AddLogging(configure => configure.AddConsole());
    }

    private static void ValidateAppSettings(AppSettings settings, ILogger logger)
    {
        var validator = new AppSettingsValidator();
        var validationResult = validator.Validate(settings);

        if (validationResult.IsValid) return;

        foreach (var error in validationResult.Errors)
        {
            logger.LogError("Configuration error in {PropertyName}: {ErrorMessage}", error.PropertyName, error.ErrorMessage);
        }
        throw new ArgumentException("Invalid configuration in appsettings.json.");
    }
}