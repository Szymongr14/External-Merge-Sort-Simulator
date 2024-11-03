namespace MemoryPageAccessSimulator.Models;

public class Page
{
    private readonly AppSettings _appSettings;
    public int PageNumber { get; init; }
    public List<Record> Records { get; set; } = [];
    public int MaxNumberOfRecords { get; init; }
    public Page(AppSettings appSettings, int pageNumber)
    {
        _appSettings = appSettings;
        PageNumber = pageNumber;
        MaxNumberOfRecords = appSettings.PageSizeInNumberOfRecords;
    }
}