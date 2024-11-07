using ExternalMergeSortSimulator.Interfaces;
using MemoryPageAccessSimulator.Interfaces;
using MemoryPageAccessSimulator.Models;
using Microsoft.Extensions.Logging;

namespace ExternalMergeSortSimulator;

public class ExternalMergeSortUsingLargeBuffersService
{
    private readonly IDatasetInputStrategy _datasetInputStrategy;
    private readonly AppSettings _appSettings;
    private readonly IMemoryManagerService _memoryManagerService;
    private readonly ILogger<ExternalMergeSortUsingLargeBuffersService> _logger;
    private const string InitialRecordsFile = "initial_records.bin";

    public ExternalMergeSortUsingLargeBuffersService(AppSettings appSettings, IDatasetInputStrategy datasetInputStrategy, IMemoryManagerService memoryManagerService, ILogger<ExternalMergeSortUsingLargeBuffersService> logger)
    {
        _appSettings = appSettings;
        _datasetInputStrategy = datasetInputStrategy;
        _memoryManagerService = memoryManagerService;
        _logger = logger;
    }

    public void Start()
    {
        var records = _datasetInputStrategy.GetRecords();
        _memoryManagerService.WriteInitialRecordsToBinaryFile(records, $"Disk/{InitialRecordsFile}");
        CreateRuns($"Disk/{InitialRecordsFile}");
    }
    
    private void CreateRuns(string filePath)
    {
        var offset = 0;
        var runCounter = 0;
        var pageHasEmptySlotsFlag = false;
        var maxOffsetForInitialFile = _memoryManagerService.GetMaxPageOffsetForFile(filePath);
        var readPages = 0;
        
        _logger.LogInformation("Initial Run Generation phase has started...");

        while (offset < maxOffsetForInitialFile)
        {
            while (!_memoryManagerService.RAMIsFull() && !pageHasEmptySlotsFlag)
            {
                var page = _memoryManagerService.ReadPageFromTape(filePath, offset++);
                _memoryManagerService.InsertPageIntoRAM(page!);
                if (page!.GetNumberOfRecords() < _appSettings.PageSizeInNumberOfRecords) pageHasEmptySlotsFlag = true;
                readPages++;
            }
            SortRecordsInRAM();
            if(_appSettings.LogLevel == "Detailed") PrintRun(runCounter);
            var pageIndex = 0;
            while (pageIndex < readPages)
            {
                var page = _memoryManagerService.GetLastPageFromRAM();
                _memoryManagerService.RemoveLastPageFromRAM();
                _memoryManagerService.WritePageToTape(page, $"Disk/run_{runCounter}.bin");
                pageIndex++;
            }

            runCounter++;
            readPages = 0;
        }

        var (currentTotalWrites, currentTotalReads) = _memoryManagerService.GetTotalReadsAndWrites();
        _logger.LogInformation("Initial Run Generation phase has ended... {} runs was created and sorted! Total writes: {}, Total reads: {}", runCounter, currentTotalWrites, currentTotalReads);
    }

    private void PrintRun(int runNumber)
    {
        var records = _memoryManagerService.GetRecordsFromRAM();
        Console.WriteLine($"Run {runNumber}:");
        for (var i = 0; i < records.Count; i++)
        {
            Console.WriteLine($"X: {records[i].X}, Y: {records[i].Y}, KEY: {records[i].Key}");
            if (i == 10)
            {
                Console.WriteLine("                            ...");
                break;
            }
        }
    }

    private void SortRecordsInRAM()
    {
        var records = _memoryManagerService.GetRecordsFromRAM();
        records.Sort((x, y) => x.Key.CompareTo(y.Key));
        _memoryManagerService.WriteRecordsToRAM(records);
    }
}
    