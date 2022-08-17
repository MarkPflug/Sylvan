# CsvDataReaderOptions

The CsvDataReader accepts several options that control it's behavior. If no options are provided defaults will be used that are suitable for many common scenarios.

__HasHeaders__

Indicates if the first row of the file contains header names. Defaults to true. In the absence of a header row, and without providing a schema that includes column names, columns can only be accessed ordinally. In the even that the header row defines duplicate column names, the duplicate columns cannot be accessed by name, GetName(int ordinal) will throw an exception.

__Delimiter__ (`char?`)

The delimiter character used between records. This is `null`, by default, which will result in the delimiter being automatically detected. The detection algorithm will look at the first row and count the number of candidate delimiters, which are one of ',', '\t', ';' or '|'.

__Quote__

The character used to quote fields. Defaults to `'"'`.

__Escape__

The character used to escape quotes in quoted fields. Defaults to `'"'`.

__TrueString/FalseString__

A string representing the `true` and/or `false` value when reading boolean fields. 

These defaults to `null`, which attempt to parse the values as the default "true"/"false" string, then fallback to parsing the field as an integer where `0` is interpreted as `false`, and all other integer values as `true`.

If either `TrueString` or `FalseString` are non-null, then that value is the singular, case-insensitive string that will be interpreted as the associated boolean value. If only one of the two is assigned it causes all other values to be interpreted as the negation. If both are assigned any value that is not one or the other will result in a `FormatException` being thrown.

__DateFormat__

The format string used to parse `DateTime` values. This defaults to null, which will result in values being parsed using the provide `CultureInfo`.

Some CSV data sources use a compact date format like `"yyyyMMdd"` which cannot be parsed by default date parsing behavior, in which case this option allows parsing such values.

__BinaryEncoding__

The encoding format used to interpret binary data, either Base64 or Hexadecimal. Hexadecimal values can optionally be prefixed with "0x".

__BufferSize__

The size of the internal buffer to allocate for parsing records.
This size is fixed and will not grow. This is the primary constraint on the data reader as all records *must* fit in the buffer or a parse exception will be thrown.

The default buffer size is 64kb, which provides a good balance between memory and performance. A smaller buffer requires more chatty IO operations, and test to be slower.

The fact that the buffer is not allowed to grow can be seen as a security feature which prevents malicious input from consuming unbounded memory. But, it also means that a larger buffer may be needed provided for some valid inputs.

__Buffer__

It is possible to provide an external buffer for parsing. It is expected that the `CsvDataReader` has exclusive use of this buffer.
This allows using a buffer from a pool if desired. Defaults to null, such that the data reader will allocate its own.


__HeaderComparer__

A string comparer used to match header values in calls to `GetOrdinal`. Defaults to `Ordinal` comparisons. This is intended to allow case-insensitive header comparisons if need.


__Culture__

The `CultureInfo` used when parsing primitive values. Defaults to
`InvariantCulture`.

__OwnsReader__

Indicates if the `CsvDataReader` owns the TextReader and should dispose it when complete. Defaults to true.

__Schema__

Allows providing a schema for the csv data. This is the most complicated of the options, and is documented [here](Schema.md)

__StringFactory__

The StringFactory option allows providing a custom mechanism for string construction. The default of `null`, will result in strings being constructed normally, each call to GetString will produce a new value. The intent of this option is allowing deduplication of strings during parsing. CSV files often contain very repetitive strings; city or state names, for example, and by providing a custom `StringFactory` implementation these strings can be de-duped as they are read. This can be significantly more efficient than de-duping after the fact with a `HashSet<string>` for example.

Providing a string factory can affect performance both positively, and adversely depending on how much duplication there is in the file, and of course depends on the implementation of the factory method itself.

The `Sylvan.Data.Csv` library does not provide a `StringFactory` implementation, but the `Sylvan.Common` package does via the `StringPool` type. The length parameter to `StringPool` controls the limit beyond which de-dupe will not be attempted. Identifying dupes takes some time and longer strings are less likely to be duplicated, so this value allows tuning speed/memory. The `StringPool` implementation doesn't evict strings, and is only intended to be short lived, for the duration of processing a small number of files.

```C#

// all strings shorter than 20 characters will be de-duplicated.
var pool = new StringPool(20);

var opts = new CsvDataReaderOptions {
    StringFactory = pool.GetString,
};

var csv = CsvDataReader.Create("data.csv", opts);
```

The `[Ben.StringIntern](https://github.com/benaadams/Ben.StringIntern)` package provides a robust implementation with configurable options and exposes telemetry data. This should be preferred if the string pool will be used beyond processing a single file.

```
using Ben.Collections.Specialized;
...

var pool = new InternPool();

var opts = new CsvDataReaderOptions {
    StringFactory = (char[] b, int o, int l) => pool.Intern(b.AsSpan().Slice(o, l))
};

var csv = CsvDataReader.Create("data.csv", opts);
```

Another example might be -- if a file is known to contain a lot of single-character strings -- an implementation like the following that only caches single character strings:

```C#
static readonly string[] pool = new string[128];

static string Pool(char[] buf, int offset, int length)
{
    if(length == 1)
    {
        var c = buf[offset];
        if (c < 128) 
        {
            return pool[c] ?? (pool[c] = ((char)c).ToString());	
        }			
    }
    // anything else just construct normally (or call a nested factory)
    return new string(buf, offset, length);
}
```