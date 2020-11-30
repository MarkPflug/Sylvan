# Sylvan.BuildTools.Data

This package provides the *easiest* way to work with a csv data in .NET.

Create a new console application.

Add Sylvan.BuildTools.Data nuget package.

Add a .csv file to the project.

`State.csv:`
```
Code,Name,FIPS,Population
OR,Oregon,41,4190000
WA,Washington,53,7540000
CA,California,06,39560000
```

You can then easily access the data via strongly typed objects.

```
void Main() {
    foreach(var state in State.Read()) {
        Console.WriteLine(state.Name + " " + state.Population);
    }
}
```

### Features

- CSV data analysis: the types and nullability of the columns will be automatically detected.

- A strongly typed class representing the data will be generated and included in your project.

- Data is *not* embedded in compilation, this allows the data to be modified without recompiling the application.

- Supports numeric or date serial columns, meaning the file can have a variable number of columns which will be exposed as a single property which allows.

- Uses Sylvan.Data.Csv library for parsing, which is the fastest csv parser available for .NET.
