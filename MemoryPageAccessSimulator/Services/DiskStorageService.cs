using MemoryPageAccessSimulator.Interfaces;
using MemoryPageAccessSimulator.Models;

namespace MemoryPageAccessSimulator.Services;

public class DiskStorageService : IDiskStorageService
{
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
        File.Copy(filePath, "Disk/initial_records_copy.bin", true);
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
}