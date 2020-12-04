## CsvDataReader

The strategy used to parse the CSV data is to read a block of input into a buffer, 
then scan each record out of that buffer. A record must be able to
fit in a single buffer. 
Indeed, this can also be considered a safety measure to prevent malicious input 
files from consuming unbounded resources.

As each record is scanned, it tracks whether it was quoted, how many escaped 
characters it contains, and the end position. No string is constructed during the 
initial scan. Only when the field is consumed is it (potentially) 
turn it into a string. By tracking quotes/escapes it is possible to calculate the 
exact space required by the output string. This means that if only a subset of 
the fields are consumed that the performance is even higher.

When reading fields as values other than strings, it is possible 
(in netstandard2.1) to parse the value directly out of the input buffer to avoid
allocating an extra, scratch string. This functionality depends on new parsing APIs that
support parsing from spans, and so isn't supported in the netstandard2.0 version.

## CsvDataWriter

The strategy used to write CSV data is similar to it is read. A single
buffer is maintained that is flushed when it is full. When writing 
fields, it first attempts to write directly to the output buffer (optimistic).
If, during the optimistic write, it is detected that the value needed to be quoted
or escaped, the position is reset and a slower "pessimistic" write is
performed, which requires quoting/escaping the value in a scratch buffer. Typical
CSV data doesn't require a significant amount of quoting, so this optimization
shows some performance benefit in real world scenarios.

Similarly, when writing primitive values, the value can be written directly into
the output buffer when it is known that it can't possibly contain a delimiter. This
is also only supported on netstandard2.1.