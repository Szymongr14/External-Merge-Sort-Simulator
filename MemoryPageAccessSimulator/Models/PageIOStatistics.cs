namespace MemoryPageAccessSimulator.Models;

public class PageIOStatistics
{
    public int TotalReads { get; private set; }
    public int TotalWrites { get; private set; }

    public void IncrementRead()
    {
        TotalReads++;
    }

    public void IncrementWrite()
    {
        TotalWrites++;
    }
    
}