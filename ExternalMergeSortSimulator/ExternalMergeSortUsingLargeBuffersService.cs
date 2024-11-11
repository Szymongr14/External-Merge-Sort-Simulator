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
    private const string DiskDirPath = "Disk";

    public ExternalMergeSortUsingLargeBuffersService(AppSettings appSettings,
        IDatasetInputStrategy datasetInputStrategy, IMemoryManagerService memoryManagerService,
        ILogger<ExternalMergeSortUsingLargeBuffersService> logger)
    {
        _appSettings = appSettings;
        _datasetInputStrategy = datasetInputStrategy;
        _memoryManagerService = memoryManagerService;
        _logger = logger;
    }

    public void Start()
    {
        var records = _datasetInputStrategy.GetRecords();
        _memoryManagerService.WriteInitialRecordsToBinaryFile(records, $"{DiskDirPath}/{InitialRecordsFile}");
        var numberOfCreatedRuns = CreateRuns($"{DiskDirPath}/{InitialRecordsFile}");
        ExecuteMergeStage(numberOfCreatedRuns);
    }

    private void ExecuteMergeStage(int numberOfRunsToMergeInFirstPhase)
    {
        _logger.LogInformation("Merge stage has started... {numberOfRunsToMerge} runs need to be merged", numberOfRunsToMergeInFirstPhase);
        var phaseCounter = 0;
        var numberOfRunsToMergeInCurrentPhase = numberOfRunsToMergeInFirstPhase;

        while (numberOfRunsToMergeInCurrentPhase > 1)
        {
            var outputCounterInCurrentPhase = 0;
            var numberOfRunsInRAM = Math.Min(numberOfRunsToMergeInCurrentPhase, _appSettings.RAMSizeInNumberOfPages - 1);
            var currentOffsets = new int[numberOfRunsInRAM];
            var maxOffsets = new int[numberOfRunsInRAM];

            for (var runStartIndex = 0; runStartIndex < numberOfRunsToMergeInCurrentPhase; runStartIndex += numberOfRunsInRAM)
            {
                if (numberOfRunsToMergeInCurrentPhase - runStartIndex == 1)
                {
                    var leftoverRunPath = $"{DiskDirPath}/phase_{phaseCounter}_run_{runStartIndex}.bin";
                    var renamedOutputPath = $"{DiskDirPath}/phase_{phaseCounter + 1}_run_{outputCounterInCurrentPhase++}.bin";
                    
                    File.Move(leftoverRunPath, renamedOutputPath, true);
                    _logger.LogInformation("Only one run remained unmerged in phase {phaseCounter}. Renaming {leftoverRunPath} to {renamedOutputPath}.", phaseCounter, leftoverRunPath, renamedOutputPath);
                    break;
                }

                var outputFilePath = $"{DiskDirPath}/phase_{phaseCounter + 1}_run_{outputCounterInCurrentPhase++}.bin";
                var mergeMinHeap = InitializeHeapAndSetUpBatch(phaseCounter, runStartIndex, numberOfRunsInRAM,
                    currentOffsets, maxOffsets, numberOfRunsToMergeInCurrentPhase);

                PrepareOutputPageInRAM();
                MergeBatch(phaseCounter, mergeMinHeap, outputFilePath, currentOffsets, maxOffsets);
                Console.WriteLine($"Output file {outputFilePath} has been created.");
                var (totalReads, totalWrites) = _memoryManagerService.GetTotalReadsAndWrites();
                Console.WriteLine($"total reads: {totalReads}, total writes: {totalWrites}, total I/O: {totalReads + totalWrites}");
            }

            PrintOutputs(phaseCounter, outputCounterInCurrentPhase);
            DeletePreviousPhaseFiles(phaseCounter, numberOfRunsToMergeInCurrentPhase);

            phaseCounter++;
            numberOfRunsToMergeInCurrentPhase = outputCounterInCurrentPhase;
        }

        LogMergeStageSummary(phaseCounter);
    }

    private PriorityQueue<HeapElement, double> InitializeHeapAndSetUpBatch(int phaseCounter, int runStartIndex,
        int numberOfRunsInRAM, int[] currentOffsets, int[] maxOffsets, int numberOfRunsToMergeInCurrentPhase)
    {
        var mergeMinHeap = new PriorityQueue<HeapElement, double>();

        for (var i = 0; i < numberOfRunsInRAM && (runStartIndex + i) < numberOfRunsToMergeInCurrentPhase; i++)
        {
            var runIndex = runStartIndex + i;
            LoadInitialPageIntoRAM($"{DiskDirPath}/phase_{phaseCounter}_run_{runIndex}.bin", i);
            GetFirstRecordFromPageAndInsertIntoHeap(i, mergeMinHeap);
            currentOffsets[i] = 0;
            maxOffsets[i] =
                _memoryManagerService.GetMaxPageOffsetForFile($"{DiskDirPath}/phase_{phaseCounter}_run_{runIndex}.bin");
        }

        return mergeMinHeap;
    }

    private void MergeBatch(int phaseCounter, PriorityQueue<HeapElement, double> mergeMinHeap, string outputFilePath,
        int[] currentOffsets, int[] maxOffsets)
    {
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
                    var nextPage = _memoryManagerService.ReadPageFromTape( $"{DiskDirPath}/phase_{phaseCounter}_run_{pageNumberOfMinRecord}.bin", ++currentOffsets[pageNumberOfMinRecord]);
                    _memoryManagerService.InsertPageIntoRAMAtGivenIndex(nextPage!, pageNumberOfMinRecord);
                }
                else
                {
                    continue;
                }
            }

            GetFirstRecordFromPageAndInsertIntoHeap(pageNumberOfMinRecord, mergeMinHeap);
        }

        var finalOutputPage = _memoryManagerService.GetPageFromRAM(_appSettings.RAMSizeInNumberOfPages - 1);
        if (!finalOutputPage.IsEmpty())
        {
            _memoryManagerService.WritePageToTape(finalOutputPage, outputFilePath);
        }
    }


    private void LoadInitialPageIntoRAM(string filePath, int runIndex)
    {
        var page = _memoryManagerService.ReadPageFromTape(filePath, 0);
        _memoryManagerService.InsertPageIntoRAMAtGivenIndex(page!, runIndex);
    }

    private void GetFirstRecordFromPageAndInsertIntoHeap(int pageNumber, PriorityQueue<HeapElement, double> minHeap)
    {
        var firstRecord = _memoryManagerService.GetFirstRecordFromGivenPage(pageNumber);
        _memoryManagerService.RemoveFirstRecordFromGivenPage(pageNumber);
        minHeap.Enqueue(new HeapElement(firstRecord, pageNumber), firstRecord.Key);
    }

    private void PrepareOutputPageInRAM()
    {
        _memoryManagerService.InsertPageIntoRAMAtGivenIndex(new Page(_appSettings.PageSizeInNumberOfRecords),
            _appSettings.RAMSizeInNumberOfPages - 1);
    }
    
    private void DeletePreviousPhaseFiles(int phaseCounter, int numberOfRunsToMergeInCurrentPhase)
    {
        for (var i = 0; i < numberOfRunsToMergeInCurrentPhase; i++)
        {
            var inputFilePath = $"{DiskDirPath}/phase_{phaseCounter}_run_{i}.bin";
            File.Delete(inputFilePath);
        }
    }
    
    private void PrintOutputs(int phaseCounter, int outputCounter)
    {
        if (_appSettings.LogLevel != "Detailed") return;
       Console.WriteLine($"Phase {phaseCounter} outputs:");
        for (var i = 0; i < outputCounter; i++)
        {
            Console.WriteLine($"Output {i}:");
            var filePath = $"{DiskDirPath}/phase_{phaseCounter + 1}_run_{i}.bin";
            using var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read);
            using var reader = new BinaryReader(fileStream);
            var records = new List<Record>();
            while (fileStream.Position < fileStream.Length)
            {
                var x = reader.ReadDouble();
                var y = reader.ReadDouble();
                var key = reader.ReadDouble();
                records.Add(new Record(x, y, key));
            }

            for (var j = 0; j < records.Count; j++)
            {
                Console.WriteLine($"X: {records[j].X}, Y: {records[j].Y}, KEY: {records[j].Key}");
                if (j == 10)
                {
                    Console.WriteLine("                            ...");
                    Console.WriteLine($"                     ({records.Count - j} more records)");
                    break;
                }
            }
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

        LogInitialDistributionSummary(runCounter);
        _memoryManagerService.ClearRAMPages();
        _memoryManagerService.InitializeEmptyPagesInRAM();
        return runCounter;
    }

    private (int readPages, int newOffset) LoadPagesIntoRAM(string filePath, int offset)
    {
        var readPages = 0;
        var pageHasEmptySlotsFlag = false;

        while (!_memoryManagerService.RAMIsFull() && !pageHasEmptySlotsFlag &&
               offset < _memoryManagerService.GetMaxPageOffsetForFile(filePath))
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

    private void LogInitialDistributionSummary(int runCounter)
    {
        var (currentTotalWrites, currentTotalReads) = _memoryManagerService.GetTotalReadsAndWrites();
        _logger.LogInformation(
            "Initial Run Generation phase has ended... {runCounter} runs were created and sorted! Total writes: {totalWrites}, Total reads: {totalReads}",
            runCounter, currentTotalWrites, currentTotalReads);
    }
    
    private void LogMergeStageSummary(int phaseCounter)
    {
        var (currentTotalWrites, currentTotalReads) = _memoryManagerService.GetTotalReadsAndWrites();
        _logger.LogInformation(
            "Merge stage has ended with {phaseCounter} phases. Total writes: {totalWrites}, Total reads: {totalReads}. Total I/O operations: {totalIO}",
            phaseCounter, currentTotalWrites, currentTotalReads, currentTotalWrites + currentTotalReads);
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