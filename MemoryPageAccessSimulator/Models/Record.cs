namespace MemoryPageAccessSimulator.Models;

public record Record
{
    public double X { get; init; }
    public double Y { get; init; }
    public double Key { get; private set; }
    
    public Record(double x, double y)
    {
        X = x;
        Y = y;
        Key = CountDistanceToOrigin();
    }

    private float CountDistanceToOrigin() => (float)Math.Sqrt(X * X + Y * Y);
}