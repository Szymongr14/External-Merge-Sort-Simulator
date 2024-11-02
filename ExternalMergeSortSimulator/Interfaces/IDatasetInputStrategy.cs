using MemoryPageAccessSimulator.Models;

namespace ExternalMergeSortSimulator.Interfaces;

public interface IDatasetInputStrategy
{
    IEnumerable<Record> GetRecords();
}