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

    private void Merge(string diskDir, int numberOfRunsToSortInFirstPhase)
    {
        var phaseCounter = 0;
        var numberOfRunsToMergeInCurrentPhase = numberOfRunsToSortInFirstPhase;

        while (numberOfRunsToMergeInCurrentPhase > 1)
        {
            var outputCounter = 0;
            var mergeMinHeap = new PriorityQueue<HeapElement, double>();

            var numberOfRunsInRAM = Math.Min(numberOfRunsToMergeInCurrentPhase, _appSettings.RAMSizeInNumberOfPages - 1);

            var currentOffsets = new int[numberOfRunsInRAM];
            var maxOffsets = new int[numberOfRunsInRAM];

            for (var runStartIndex = 0; runStartIndex < numberOfRunsToMergeInCurrentPhase; runStartIndex += numberOfRunsInRAM)
            {
                var outputFilePath = $"{diskDir}/phase_{phaseCounter + 1}_run_{outputCounter++}.bin";

                for (var i = 0; i < numberOfRunsInRAM && (runStartIndex + i) < numberOfRunsToMergeInCurrentPhase; i++)
                {
                    var runIndex = runStartIndex + i;
                    LoadInitialPageIntoHeap($"{diskDir}/phase_{phaseCounter}_run_{runIndex}.bin", i, mergeMinHeap);
                    currentOffsets[i] = 0;
                    maxOffsets[i] = _memoryManagerService.GetMaxPageOffsetForFile($"{diskDir}/phase_{phaseCounter}_run_{runIndex}.bin");
                }

                _memoryManagerService.InsertPageIntoRAMAtGivenIndex(new Page(_appSettings.PageSizeInNumberOfRecords), _appSettings.RAMSizeInNumberOfPages - 1);

                while (mergeMinHeap.Count > 0)
                {
                    var (record, pageNumberOfMinRecord) = mergeMinHeap.Dequeue();

                    _memoryManagerService.MoveRecordToPage(_appSettings.RAMSizeInNumberOfPages - 1, record);
                    var outputPage = _memoryManagerService.GetPageFromRAM(_appSettings.RAMSizeInNumberOfPages - 1);

                    if (outputPage.PageIsFull())
                    {
                        _memoryManagerService.WritePageToTape(outputPage, outputFilePath);
                        _memoryManagerService.ClearPage(_appSettings.RAMSizeInNumberOfPages - 1);
                    }

                    if (_memoryManagerService.PageIsEmpty(pageNumberOfMinRecord))
                    {
                        if (currentOffsets[pageNumberOfMinRecord] < maxOffsets[pageNumberOfMinRecord] - 1)
                        {
                            var nextPage = _memoryManagerService.ReadPageFromTape($"{diskDir}/phase_{phaseCounter}_run_{pageNumberOfMinRecord}.bin", ++currentOffsets[pageNumberOfMinRecord]);
                            _memoryManagerService.InsertPageIntoRAMAtGivenIndex(nextPage!, pageNumberOfMinRecord);
                        }
                        else
                        {
                            continue;
                        }
                    }

                    var nextRecordFromPage = _memoryManagerService.GetFirstRecordFromGivenPage(pageNumberOfMinRecord);
                    _memoryManagerService.RemoveFirstRecordFromGivenPage(pageNumberOfMinRecord);
                    mergeMinHeap.Enqueue(new HeapElement(nextRecordFromPage, pageNumberOfMinRecord), nextRecordFromPage.Key);
                }

                var finalOutputPage = _memoryManagerService.GetPageFromRAM(_appSettings.RAMSizeInNumberOfPages - 1);
                if (!finalOutputPage.IsEmpty())
                {
                    _memoryManagerService.WritePageToTape(finalOutputPage, outputFilePath);
                }
            }

            for (var i = 0; i < numberOfRunsToMergeInCurrentPhase; i++)
            {
                var inputFilePath = $"{diskDir}/phase_{phaseCounter}_run_{i}.bin";
                File.Delete(inputFilePath);
                _logger.LogInformation($"Deleted temporary file: {inputFilePath}");
            }

            phaseCounter++;
            numberOfRunsToMergeInCurrentPhase = outputCounter;
        }

        _logger.LogInformation("Merge completed successfully. Final sorted run is in the last output file.");
    }

    
    private void LoadInitialPageIntoHeap(string filePath, int runIndex, PriorityQueue<HeapElement, double> minHeap)
    {
        var page = _memoryManagerService.ReadPageFromTape(filePath, 0);
        _memoryManagerService.InsertPageIntoRAMAtGivenIndex(page!, runIndex);
        var firstRecord = _memoryManagerService.GetFirstRecordFromGivenPage(runIndex);
        _memoryManagerService.RemoveFirstRecordFromGivenPage(runIndex);
        minHeap.Enqueue(new HeapElement(firstRecord, runIndex), firstRecord.Key);
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
        _memoryManagerService.InitializeEmptyPagesInRAM();
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
            _memoryManagerService.WritePageToTape(page, $"Disk/phase_0_run_{runCounter}.bin");

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
    