# <img src="../Sylvan.png" height="48" alt="Sylvan Logo"/> Sylvan.Data

Sylvan.Data provides types for working with ADO.NET data sources. 
The library is provider-agnostic, and the APIs typically target `DbDataReader`.

#### Installation

[Sylvan.Data Nuget Package](https://www.nuget.org/packages/Sylvan.Data/)

`Install-Package Sylvan.Data`


## Sylvan.Data.DataBinder

This type provides a general purpose data binder that binds a `DbDataReader` to
strongly-typed objects. The data binder uses the schema (`GetColumnSchema`/`GetSchemaTable`) of the
data reader to produce a near-optimally efficient binder; the performance is essentially equivalent
to a hand-written binder.

primitives
string
byte/char[]
Enum
custom types via constructor
	ie Version(string s);
