# External Merge Sort with Page Access Simulation

This project implements an **external merge sort** algorithm with a **page access simulation** feature, designed to simulate memory page access and track I/O operations. The application sorts large datasets that cannot fit into memory by dividing them into sorted runs and iteratively merging them. This project is part of the **Database Structures** course at **Gdańsk University of Technology**.

___

## Configuration (`appsettings.json`)

Below is a list of available configuration options that you can specify in the `appsettings.json` file:

```json
{
  "Settings": {
    "PageSizeInNumberOfRecords": 4096,
    "RAMSizeInNumberOfPages": 10000,
    "InternalSortingMethod": "QuickSort",
    "DataSource": "LoadFromFile",
    "FilePath": "data/input.txt",
    "NumberOfRecordsToGenerate" : null,
    "LogLevel": "Detailed"
  }
}
```

### Configuration Options Explained

- **PageSizeInNumberOfRecords**: Specifies the size of each memory page in terms of the number of records that fit into one page, influencing the page access simulation.

- **RAMSizeInNumberOfPages**: Defines the amount of RAM available for the simulation in terms of pages, affecting how many records can be held in memory at a time.

- **InternalSortingMethod**: Specifies the sorting method to use for internal sorting. Options include:
    - `"MergeSort"`
    - `"QuickSort"`

- **DataSource**: Determines the source of the dataset. Available options:
    - `"GenerateRandomly"`: Generates a random dataset, **NumberOfRecordsToGenerate** must be provided.
    - `"ProvideManually"`: Allows the user to input data manually.
    - `"LoadFromFile"`: Loads data from an external file. If this option is chosen, **FilePath** must be provided.

- **FilePath**: Specifies the relative path for loading data when `"DataSource"` is set to `"LoadFromFile"`. The path is relative to the **build directory** (e.g., `bin/Debug/net8.0/`). For example, setting `"FilePath": "data/input.txt"` expects the file to be located in `bin/Debug/net8.0/data/input.txt` when running in debug mode.

- **NumberOfRecordsToGenerate**: Specifies the number of records to generate when "DataSource" is set to "GenerateRandomly". This option is ignored if DataSource is set to another value.

- **LogLevel**: Sets the level of detail for logging information about the sorting and simulation process. Options:
    - ``Basic``:
        - Logs completion of each main phase.
        - Provides a summary of sorting phases and I/O operations at the end.
    - ``Detailed``:
        - Includes all logs from the Basic level.
        - Logs completion of each iteration within the merging and distribution phases.

---