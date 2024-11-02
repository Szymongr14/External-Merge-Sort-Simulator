using ExternalMergeSortSimulator.Factories;
using ExternalMergeSortSimulator.Interfaces;
using ExternalMergeSortSimulator.Validators;
using MemoryPageAccessSimulator.Models;
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
        
        // Bind and validate configuration file
        var configurationModel = new AppSettings();
        configuration.GetSection("Settings").Bind(configurationModel);

        // Set up DI container
        var services = new ServiceCollection();
        ConfigureServices(services, configurationModel);
        var serviceProvider = services.BuildServiceProvider();
        
        // Get logger instance from DI
        var logger = serviceProvider.GetRequiredService<ILogger<Program>>();
        ValidateAppSettings(configurationModel, logger);
        
        logger.LogInformation("Application started successfully.");
        
        // Start the application
        var externalMergeSortService = serviceProvider.GetRequiredService<ExternalMergeSortUsingLargeBuffersService>();
        externalMergeSortService.Start();
    }
    
    private static void ConfigureServices(ServiceCollection services, AppSettings configurationModel)
    {
        services.AddSingleton(configurationModel);
        
        services.AddLogging(configure => configure.AddConsole());
        services.AddSingleton<IDatasetInputStrategyFactory, DatasetInputStrategyFactory>();
        
        services.AddTransient(serviceProvider =>
        {
            var factory = serviceProvider.GetRequiredService<IDatasetInputStrategyFactory>();
            return factory.Create();
        });
        
        services.AddTransient<ExternalMergeSortUsingLargeBuffersService>();
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