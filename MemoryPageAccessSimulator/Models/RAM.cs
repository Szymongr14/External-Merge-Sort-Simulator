namespace MemoryPageAccessSimulator.Models;

public class RAM
{
    public int MaxNumberOfPages { get; init; }
    public List<Page> Pages { get; set; }

    public RAM(AppSettings appSettings)
    {
        MaxNumberOfPages = appSettings.RAMSizeInNumberOfPages;
        Pages = new List<Page>(MaxNumberOfPages);
    }
}