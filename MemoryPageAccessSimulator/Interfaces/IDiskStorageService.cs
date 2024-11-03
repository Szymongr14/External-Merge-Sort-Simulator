using MemoryPageAccessSimulator.Models;

namespace MemoryPageAccessSimulator.Interfaces;

public interface IDiskStorageService
{
    public void WriteInitialRecordsToBinaryFile(IEnumerable<Record> records, string filePath);
    public IEnumerable<Record> ReadInitialRecordsFromBinaryFile(string filePath);

}