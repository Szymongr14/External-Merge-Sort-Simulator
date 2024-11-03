using ExternalMergeSortSimulator.Interfaces;
using MemoryPageAccessSimulator.Interfaces;
using MemoryPageAccessSimulator.Models;

namespace ExternalMergeSortSimulator;

public class ExternalMergeSortUsingLargeBuffersService
{
    private readonly IDatasetInputStrategy _datasetInputStrategy;
    private readonly AppSettings _appSettings;
    private readonly IDiskStorageService _diskStorageService;

    public ExternalMergeSortUsingLargeBuffersService(AppSettings appSettings, IDatasetInputStrategy datasetInputStrategy, IDiskStorageService diskStorageService)
    {
        _appSettings = appSettings;
        _datasetInputStrategy = datasetInputStrategy;
        _diskStorageService = diskStorageService;
    }

    public void Start()
    {
        var records = _datasetInputStrategy.GetRecords();
        _diskStorageService.WriteInitialRecordsToBinaryFile(records, "Disk/initial_records.bin");
        Console.WriteLine("Saved to binary file.");
        _diskStorageService.ReadInitialRecordsFromBinaryFile("Disk/initial_records.bin");
    }
}
    