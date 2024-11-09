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

    // private void Merge(string diskDir, int numberOfInitialRuns)
    // {
    //     var mergeMinHeap = new PriorityQueue<HeapElement, double>();
    //     var outputCounter = 0;
    //     
    //     var currentOffsets = new int[_appSettings.RAMSizeInNumberOfPages];
    //     var maxOffsets = new int[_appSettings.RAMSizeInNumberOfPages];
    //     for (var i = 0; i < _appSettings.RAMSizeInNumberOfPages - 1; i++)
    //     {
    //         maxOffsets[i] = _memoryManagerService.GetMaxPageOffsetForFile($"{diskDir}/run_{i}.bin");
    //         var page = _memoryManagerService.ReadPageFromTape($"{diskDir}/run_{i}.bin", 0);
    //         _memoryManagerService.InsertPageIntoRAMAtGivenIndex(page!, i);
    //         var firstRecord = _memoryManagerService.GetFirstRecordFromGivenPage(i);
    //         _memoryManagerService.RemoveFirstRecordFromGivenPage(i);
    //         mergeMinHeap.Enqueue(new HeapElement(firstRecord, i), firstRecord.Key);
    //     }
    //
    //     var minValueOnHeap = mergeMinHeap.Dequeue();
    //     _memoryManagerService.MoveRecordToPage(outputCounter, minValueOnHeap.Record);
    //     var pageWithOutputRun = _memoryManagerService.GetPageFromRAM(_appSettings.RAMSizeInNumberOfPages - 1);
    //     if(pageWithOutputRun.PageIsFull())
    //     {
    //         _memoryManagerService.WritePageToTape(pageWithOutputRun, $"{diskDir}/output_{outputCounter}.bin");
    //     }
    //     
    //     // TODO: check if page is empty, if yes fetch new page from tape
    // }
    //
    
    private void Merge(string diskDir, int numberOfInitialRuns)
    {
        int phaseCounter = 0;

        while (numberOfInitialRuns > 1)
        {
            var mergeMinHeap = new PriorityQueue<HeapElement, double>();
            int activeRunCount = Math.Min(numberOfInitialRuns, _appSettings.RAMSizeInNumberOfPages - 1);
            var currentOffsets = new int[activeRunCount];
            var maxOffsets = new int[activeRunCount];
            
            // Current phase's output file path (only one file per phase)
            var outputFilePath = $"{diskDir}/merged_output_phase_{phaseCounter}.bin";
            
            // Load first pages from the initial `n-1` runs into RAM and initialize the heap
            for (int i = 0; i < activeRunCount; i++)
            {
                LoadInitialPageIntoHeap($"{diskDir}/run_{i}.bin", i, mergeMinHeap);
                currentOffsets[i] = 0;
                maxOffsets[i] = _memoryManagerService.GetMaxPageOffsetForFile($"{diskDir}/run_{i}.bin");
            }
            _memoryManagerService.InsertPageIntoRAMAtGivenIndex(new Page(_appSettings.PageSizeInNumberOfRecords), _appSettings.RAMSizeInNumberOfPages - 1);

            _logger.LogInformation($"Starting merge phase {phaseCounter}...");

            while (mergeMinHeap.Count > 0)
            {
                // 1. Dequeue the smallest element from the heap
                var minElement = mergeMinHeap.Dequeue();
                int sourceRunIndex = minElement.PageNumber;

                // 2. Add the smallest element to the output page in RAM
                _memoryManagerService.MoveRecordToPage(_appSettings.RAMSizeInNumberOfPages - 1, minElement.Record);
                var outputPage = _memoryManagerService.GetPageFromRAM(_appSettings.RAMSizeInNumberOfPages - 1);

                // 3. If output page is full, write it to the output file and update output offset
                if (outputPage.PageIsFull())
                {
                    _memoryManagerService.WritePageToTape(outputPage, outputFilePath);
                    _memoryManagerService.ClearPage(_appSettings.RAMSizeInNumberOfPages - 1);  // Clear the output page for reuse
                }

                // 4. Check if there are more records on the current page in RAM (sourceRunIndex)
                if (_memoryManagerService.PageIsEmpty(sourceRunIndex))
                {
                    if (currentOffsets[sourceRunIndex] == maxOffsets[sourceRunIndex] - 1)
                    {
                        Console.WriteLine();
                        continue;
                    }
                    var nextPage = _memoryManagerService.ReadPageFromTape($"{diskDir}/run_{sourceRunIndex}.bin", ++currentOffsets[sourceRunIndex]);
                    _memoryManagerService.InsertPageIntoRAMAtGivenIndex(nextPage!, sourceRunIndex);
                }

                var nextRecordFromPage = _memoryManagerService.GetFirstRecordFromGivenPage(sourceRunIndex);
                _memoryManagerService.RemoveFirstRecordFromGivenPage(sourceRunIndex);
                mergeMinHeap.Enqueue(new HeapElement(nextRecordFromPage, sourceRunIndex), nextRecordFromPage.Key);
            }

            // Finalize the output file by writing any remaining data in the output page
            var finalOutputPage = _memoryManagerService.GetPageFromRAM(_appSettings.RAMSizeInNumberOfPages - 1);
            if (!finalOutputPage.IsEmpty())
            {
                _memoryManagerService.WritePageToTape(finalOutputPage, outputFilePath);
            }

            // Prepare for the next phase, treating this phase's output file as a new single run
            phaseCounter++;
            numberOfInitialRuns = 1; // Now we have a single output file, which we use in the next phase as input
        }

        _logger.LogInformation("Merge completed successfully.");
    }
    
    private void LoadInitialPageIntoHeap(string filePath, int runIndex, PriorityQueue<HeapElement, double> minHeap)
    {
        // 1. Read the first page from the specified run file
        var page = _memoryManagerService.ReadPageFromTape(filePath, 0);
    
        // 2. Insert the page into RAM at the specified index (runIndex)
        _memoryManagerService.InsertPageIntoRAMAtGivenIndex(page!, runIndex);

        // 3. Retrieve the first record from the page
        var firstRecord = _memoryManagerService.GetFirstRecordFromGivenPage(runIndex);
    
        // 4. Remove the first record from the page (to keep track of the next record in this page)
        _memoryManagerService.RemoveFirstRecordFromGivenPage(runIndex);

        // 5. Enqueue the first record into the min-heap with its key as the priority
        if (firstRecord != null)
        {
            minHeap.Enqueue(new HeapElement(firstRecord, runIndex), firstRecord.Key);
        }
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
    