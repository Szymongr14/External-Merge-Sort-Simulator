namespace MemoryPageAccessSimulator.Models;

public class RAM
{
    public int NumberOfPages { get; init; }
    public List<Page> Pages { get; set; } = [];

    public RAM(AppSettings appSettings)
    {
        NumberOfPages = appSettings.RAMSizeInNumberOfPages;
    }

    
    
}