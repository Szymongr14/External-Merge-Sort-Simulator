using MemoryPageAccessSimulator.Interfaces;
using MemoryPageAccessSimulator.Services;
using Microsoft.Extensions.DependencyInjection;

namespace MemoryPageAccessSimulator;

public static class DependencyInjection
{
    public static void AddMemoryPageAccessSimulatorDependencies(this IServiceCollection services)
    {
        services.AddSingleton<IMemoryManagerService, MemoryManagerService>();
    }
}