using ExternalMergeSortSimulator.Interfaces;
using MemoryPageAccessSimulator.Models;

namespace ExternalMergeSortSimulator;

public class ExternalMergeSortUsingLargeBuffersService
{
    private readonly IDatasetInputStrategy _datasetInputStrategy;
    private readonly AppSettings _appSettings;

    public ExternalMergeSortUsingLargeBuffersService(AppSettings appSettings, IDatasetInputStrategy datasetInputStrategy)
    {
        _appSettings = appSettings;
        _datasetInputStrategy = datasetInputStrategy;
    }

    public void Start()
    {
        Console.WriteLine("hello");
        var records = _datasetInputStrategy.GetRecords();
        foreach (var record in records)
        {
            Console.WriteLine($"X: {record.X}, Y: {record.Y}, KEY: {record.Key}");
        }
    }
}