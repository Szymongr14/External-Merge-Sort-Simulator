using MemoryPageAccessSimulator.Interfaces;
using MemoryPageAccessSimulator.Models;

namespace MemoryPageAccessSimulator.Services;

public class MemoryManagerService : IMemoryManagerService
{
    private readonly AppSettings _appSettings;
    private RAM _ram;
    private int PageSizeInBytes { get; init; }
    public int PageSizeInNumberOfRecords { get; init; }
    public PageIOStatistics PageIOStatistics { get; init; } = new();

    
    public MemoryManagerService(AppSettings appSettings)
    {
        _appSettings = appSettings;
        _ram = new RAM(appSettings);
        PageSizeInBytes = appSettings.PageSizeInNumberOfRecords * appSettings.RecordSizeInBytes;
        PageSizeInNumberOfRecords = appSettings.PageSizeInNumberOfRecords;
    }
    
    public void WriteInitialRecordsToBinaryFile(IEnumerable<Record> records, string filePath)
    {
        using var fileStream = new FileStream(filePath, FileMode.Create, FileAccess.Write);
        using var writer = new BinaryWriter(fileStream);
        foreach (var record in records)
        {
            writer.Write(record.X);
            writer.Write(record.Y);
            writer.Write(record.Key);
        }
        writer.Close();
    }

    public IEnumerable<Record> ReadInitialRecordsFromBinaryFile(string filePath)
    {
        var records = new List<Record>();
        using var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read);
        using var reader = new BinaryReader(fileStream);
        while (fileStream.Position < fileStream.Length)
        {
            var x = reader.ReadDouble();
            var y = reader.ReadDouble();
            var key = reader.ReadDouble();
            records.Add(new Record(x, y, key));
        }

        foreach (var record in records)
        {
            Console.WriteLine($"X: {record.X}, Y: {record.Y}, KEY: {record.Key}");
        }

        return records;
    }
    
    public void WritePageToTape(Page page, string filePath)
    {
        using var fileStream = new FileStream(filePath, FileMode.OpenOrCreate, FileAccess.Write);
    
        fileStream.Seek(0, SeekOrigin.End);

        using var writer = new BinaryWriter(fileStream);
        foreach (var record in page.Records)
        {
            writer.Write(record.X);
            writer.Write(record.Y);
            writer.Write(record.Key);
        }
        PageIOStatistics.IncrementWrite();
    }

    public Page? ReadPageFromTape(string filePath, int offset)
    {
        var page = new Page(PageSizeInNumberOfRecords);
        using var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read);
        using var reader = new BinaryReader(fileStream);
        fileStream.Seek(offset * PageSizeInBytes, SeekOrigin.Begin);
        while (!page.PageIsFull() && fileStream.Position < fileStream.Length)
        {
            var x = reader.ReadDouble();
            var y = reader.ReadDouble();
            var key = reader.ReadDouble();
            page.AddRecord(new Record(x, y, key));
        }

        PageIOStatistics.IncrementRead();
        return page;
    }
    
    public void InsertPageIntoRAM(Page page)
    {
        _ram.Pages.Add(page);
    }
    
    public void RemovePageFromRAM(int pageNumber)
    {
        _ram.Pages.RemoveAt(pageNumber);
    }

    public Page GetLastPageFromRAM()
    {
        return _ram.Pages[^1];
    }
    
    public void RemoveLastPageFromRAM()
    {
        _ram.Pages.RemoveAt(_ram.Pages.Count - 1);
    }

    public Page GetPageFromRAM(int pageNumber)
    {
        var page = _ram.Pages[pageNumber];
        return page;
    }
    
    public bool RAMIsFull()
    {
        return _ram.Pages.Count == _ram.MaxNumberOfPages;
    }

    public List<Record> GetRecordsFromRAM()
    {
        var records = new List<Record>();
        foreach (var page in _ram.Pages)
        {
            records.AddRange(page.Records);
        }

        return records;
    }
    
    public void WriteRecordsToRAM(List<Record> records)
    {
        var pageNumber = 0;
        var recordIndex = 0;
        ClearRAMPages();
        InitializeEmptyPagesInRAM();
        foreach (var record in records)
        {
            if (recordIndex % PageSizeInNumberOfRecords == 0)
            {
                pageNumber++;
            }
            _ram.Pages[pageNumber - 1].AddRecord(record);
            recordIndex++;
        }
    }

    public bool RAMIsEmpty()
    {
        return _ram.Pages.Count == 0;
    }

    private void InitializeEmptyPagesInRAM()
    {
        for (var i = 0; i < _ram.MaxNumberOfPages; i++)
        {
            _ram.Pages.Add(new Page(PageSizeInNumberOfRecords));
        }
    }
    
    private void ClearRAMPages()
    {
        _ram.Pages.Clear();
    }
    
    public int GetMaxPageOffsetForFile(string filePath)
    {
        using var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read);
        return (int)Math.Ceiling((double)fileStream.Length / PageSizeInBytes);
    }
    
    public (int totalReads, int totalWrites) GetTotalReadsAndWrites()
    {
        return (PageIOStatistics.TotalReads, PageIOStatistics.TotalWrites);
    }
}