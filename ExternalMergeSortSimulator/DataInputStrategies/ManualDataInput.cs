using ExternalMergeSortSimulator.Interfaces;
using MemoryPageAccessSimulator.Models;

namespace ExternalMergeSortSimulator.DataInputStrategies;

public class ManualDataInput : IDatasetInputStrategy
{
    private readonly AppSettings _appSettings;

    public ManualDataInput(AppSettings appSettings)
    {
        _appSettings = appSettings;
    }

    public IEnumerable<Record> GetRecords()
    {
        var records = new List<Record>();
        for (var i = 0; i < _appSettings.NumberOfRecordsToGenerate; i++)
        {
            Console.WriteLine("Enter X:");
            var x = double.Parse(Console.ReadLine() ?? string.Empty);
            Console.WriteLine("Enter Y:");
            var y = double.Parse(Console.ReadLine() ?? string.Empty);
            records.Add(new Record(x, y));
        }

        return records;
    }
}