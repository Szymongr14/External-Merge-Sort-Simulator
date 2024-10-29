namespace ExternalMergeSortSimulator;

public record Record(double X, double Y)
{
    public float CountDistanceToOrigin() => (float)Math.Sqrt(X * X + Y * Y);
}