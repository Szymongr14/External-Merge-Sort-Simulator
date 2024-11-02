using MemoryPageAccessSimulator.Models;

namespace MemoryPageAccessSimulator;

public class MemoryManagerService
{
    private readonly AppSettings _appSettings;

    public MemoryManagerService(AppSettings appSettings)
    {
        _appSettings = appSettings;
    }

    public Record ReadRecordFromTape()
    {
        throw new NotImplementedException();
    }
    
    public void WriteRecordToTape(Record record)
    {
        throw new NotImplementedException();
    }
    
}