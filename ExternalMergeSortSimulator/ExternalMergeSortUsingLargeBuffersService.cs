using ExternalMergeSortSimulator.Interfaces;
using ExternalMergeSortSimulator.Models;
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
        var numberOfCreatedRuns = CreateRuns($"Disk/{InitialRecordsFile}");
        Merge("Disk", numberOfCreatedRuns);
    }

    private void Merge(string diskDir, int numberOfInitialRuns)
    {
        var mergeMinHeap = new PriorityQueue<HeapElement, double>();
        var outputCounter = 0;
        
        var currentOffsets = new int[_appSettings.RAMSizeInNumberOfPages];
        var maxOffsets = new int[_appSettings.RAMSizeInNumberOfPages];
        for (var i = 0; i < _appSettings.RAMSizeInNumberOfPages - 1; i++)
        {
            maxOffsets[i] = _memoryManagerService.GetMaxPageOffsetForFile($"{diskDir}/run_{i}.bin");
            var page = _memoryManagerService.ReadPageFromTape($"{diskDir}/run_{i}.bin", 0);
            _memoryManagerService.InsertPageIntoRAMAtGivenIndex(page!, i);
            var firstRecord = _memoryManagerService.GetFirstRecordFromGivenPage(i);
            _memoryManagerService.RemoveFirstRecordFromGivenPage(i);
            mergeMinHeap.Enqueue(new HeapElement(firstRecord, i), firstRecord.Key);
        }

        var minValueOnHeap = mergeMinHeap.Dequeue();
        _memoryManagerService.MoveRecordToPage(outputCounter, minValueOnHeap.Record);
        var pageWithOutputRun = _memoryManagerService.GetPageFromRAM(_appSettings.RAMSizeInNumberOfPages - 1);
        if(pageWithOutputRun.PageIsFull())
        {
            _memoryManagerService.WritePageToTape(pageWithOutputRun, $"{diskDir}/output_{outputCounter}.bin");
        }
        
        // TODO: check if page is empty, if yes fetch new page from tape
    }
    
    private int CreateRuns(string filePath)
    {
        var initialRecordsFileOffset = 0;
        var runCounter = 0;
        var maxOffsetForInitialFile = _memoryManagerService.GetMaxPageOffsetForFile(filePath);

        _logger.LogInformation("Initial Run Generation phase has started...");

        while (initialRecordsFileOffset < maxOffsetForInitialFile)
        {
            var (readPages, newOffset) = LoadPagesIntoRAM(filePath, initialRecordsFileOffset);
            initialRecordsFileOffset = newOffset;
            SortAndSaveRunToDisk(runCounter, readPages);
            runCounter++;
        }

        LogSummary(runCounter);
        _memoryManagerService.ClearRAMPages();
        return runCounter;
    }

    private (int readPages, int newOffset) LoadPagesIntoRAM(string filePath, int offset)
    {
        var readPages = 0;
        var pageHasEmptySlotsFlag = false;

        while (!_memoryManagerService.RAMIsFull() && !pageHasEmptySlotsFlag && offset < _memoryManagerService.GetMaxPageOffsetForFile(filePath))
        {
            var page = _memoryManagerService.ReadPageFromTape(filePath, offset++);
            _memoryManagerService.InsertPageIntoRAM(page!);

            if (page!.GetNumberOfRecords() < _appSettings.PageSizeInNumberOfRecords)
            {
                pageHasEmptySlotsFlag = true;
            }

            readPages++;
        }

        return (readPages, offset);
    }

    private void SortAndSaveRunToDisk(int runCounter, int readPages)
    {
        SortRecordsInRAM();
        var pageIndex = 0;
        
        if (_appSettings.LogLevel == "Detailed")
        {
            PrintRun(runCounter);
        }

        while (pageIndex < readPages)
        {
            var page = _memoryManagerService.GetLastPageFromRAM();
            _memoryManagerService.RemoveLastPageFromRAM();
            _memoryManagerService.WritePageToTape(page, $"Disk/run_{runCounter}.bin");

            pageIndex++;
        }
    }

    private void LogSummary(int runCounter)
    {
        var (currentTotalWrites, currentTotalReads) = _memoryManagerService.GetTotalReadsAndWrites();
        _logger.LogInformation("Initial Run Generation phase has ended... {runCounter} runs were created and sorted! Total writes: {totalWrites}, Total reads: {totalReads}", runCounter, currentTotalWrites, currentTotalReads);
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
                Console.WriteLine($"                     ({records.Count - i} more records)");
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
    