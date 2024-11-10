using MemoryPageAccessSimulator.Models;

namespace ExternalMergeSortSimulator.Validators;

using FluentValidation;

public class AppSettingsValidator : AbstractValidator<AppSettings>
{
    public AppSettingsValidator()
    {
        RuleFor(x => x.PageSizeInNumberOfRecords)
            .NotEmpty().GreaterThan(0).WithMessage("PageSizeInNumberOfRecords must be greater than 0.");

        RuleFor(x => x.RAMSizeInNumberOfPages)
            .NotEmpty().GreaterThan(0).WithMessage("RAMSizeInNumberOfPages must be greater than 0.");

        RuleFor(x => x.DataSource)
            .NotEmpty().WithMessage("DataSource is required.")
            .Must(value => new[] { "GenerateRandomly", "ProvideManually", "LoadFromFile" }.Contains(value))
            .WithMessage("DataSource must be one of: 'GenerateRandomly', 'ProvideManually', or 'LoadFromFile'.");

        RuleFor(x => x.NumberOfRecordsToGenerate)
            .NotEmpty().GreaterThan(0).When(x => x.DataSource == "GenerateRandomly")
            .WithMessage("NumberOfRecordsToGenerate must be specified and greater than 0 when DataSource is 'GenerateRandomly'.");

        RuleFor(x => x.FilePath)
            .NotEmpty().When(x => x.DataSource == "LoadFromFile")
            .WithMessage("FilePath is required when DataSource is 'LoadFromFile'.");
    }
}