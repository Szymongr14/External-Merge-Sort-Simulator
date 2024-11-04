namespace MemoryPageAccessSimulator.Models;

public class Page
{
    public int PageSizeInBytes { get; init; }
    internal LinkedList<Record> Records { get; set; } = [];
    private int MaxNumberOfRecords { get; init; }
    
    public Page(int maxNumberOfRecords)
    {
        MaxNumberOfRecords = maxNumberOfRecords;
    }
    
    public void AddRecord(Record record)
    {
        if (Records.Count < MaxNumberOfRecords)
        {
            Records.AddLast(record);
        }
        else
        {
            throw new Exception("Page is full.");
        }
    }
    
    public void ClearPage()
    {
        Records.Clear();
    }
    
    public bool PageIsFull()
    {
        return Records.Count == MaxNumberOfRecords;
    }
    
    public int GetNumberOfRecords()
    {
        return Records.Count;
    }
}