namespace MemoryPageAccessSimulator.Models;

public class AppSettings
{
    public int PageSizeInNumberOfRecords { get; init; }
    public int RAMSizeInNumberOfPages { get; init; }
    public string InternalSortingMethod { get; init; } = null!;
    public string DataSource { get; init; } = null!;
    public int? NumberOfRecordsToGenerate { get; init; }
    public string LogLevel { get; init; } = null!;
    public string FilePath { get; init; } = null!;
}