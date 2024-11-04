using MemoryPageAccessSimulator.Models;

namespace MemoryPageAccessSimulator.Interfaces;

public interface IMemoryManagerService
{
    public void WriteInitialRecordsToBinaryFile(IEnumerable<Record> records, string filePath);
    public IEnumerable<Record> ReadInitialRecordsFromBinaryFile(string filePath);
    public void WritePageToTape(Page page, string filePath);
    public Page? ReadPageFromTape(string filePath, int offset);
    public void InsertPageIntoRAM(Page page);
    public Page GetPageFromRAM(int pageNumber);
    public bool RAMIsFull();
    public List<Record> GetRecordsFromRAM();
    public void WriteRecordsToRAM(List<Record> records);
    public bool RAMIsEmpty();
    public void RemovePageFromRAM(int pageNumber);
    public Page GetLastPageFromRAM();
    public void RemoveLastPageFromRAM();
    public int GetMaxPageOffsetForFile(string filePath);
}