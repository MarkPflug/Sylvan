# <img src="../../Sylvan.png" height="48" alt="Sylvan Logo"/> Sylvan.Common

The `Sylvan.Common` library contains a collection of types and extensions that I've developed over time.
Most of these will not be of much interest to most people, but you're free to use it if you do.


### IsoDate

Exposes methods to convert to and from ISO 8601 formatted dates. 
These are faster than the default DateTime implementation since they don't need to account for a variety of formats and cultures.
`DateTime.TryParse` performance is best when using the "O" (round-trip) format specifier, which requires a very specific ISO 8601 format.
`IsoDate` attempts to be a bit more lenient in what it parses, while maintaining high performance.
This code was largely taken from an implementation in `System.Text.Json`, which I then modified in the following way:

- Handles chars instead of bytes, as the S.T.Json impl was utf-8 specific.
- Allows ' ' in place of 'T' as time separator. Not ISO compliant, but common enough that I wanted to support it.
- Allows ',' in place of '.' as fractional time separator, which ISO8601 apparently allows, but isn't supported by S.T.Json.
- Rounds appropriately when more than 7 fractional second digits are parsed. Nanoseconds are important, right.

### StringPool

This type provides a means of de-duping strings upon construction. 
It is conceptually similar to the `System.Xml.NameTable` type, which is intended to de-dupe the repetitive names in XML.
This implements a dictionary that only allows adding by calling `string GetString(char[] buffer, int offset, int length)`, which
will return an existing string if the same sequence has already been seen. This was created to de-dupe strings in the `Sylvan.Data.Csv.CsvDataReader` implementation. This type has no eviction policy, so it is essentially a giant memory leak until it is GCed.
If you are intending to use this along with the Sylvan CSV reader, you might investigate using [Ben.StringPool](https://www.nuget.org/packages/Ben.StringIntern/) instead, which offers a more robust implementation that includes LRU cache eviction, making it suitable for use in longer-lived scopes.

### ... and more

Lots of other types, most not worth discussing. :)