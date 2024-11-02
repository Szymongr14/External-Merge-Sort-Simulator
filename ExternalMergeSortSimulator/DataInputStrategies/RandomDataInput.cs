using ExternalMergeSortSimulator.Interfaces;
using MemoryPageAccessSimulator.Models;

namespace ExternalMergeSortSimulator.DataInputStrategies;

public class RandomDataInput : IDatasetInputStrategy
{
    private readonly AppSettings _appSettings;
    
    public RandomDataInput(AppSettings appSettings)
    {
        _appSettings = appSettings;
    }

    public IEnumerable<Record> GetRecords()
    {
        var records = new List<Record>();
        var random = new Random();
        for (var i = 0; i < _appSettings.NumberOfRecordsToGenerate; i++)
        {
            var x = random.NextDouble() * 100;
            var y = random.NextDouble() * 100;
            records.Add(new Record(x, y));
        }

        return records;
    }
}