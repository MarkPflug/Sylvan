## CsvDataReader

The strategy used to parse the CSV data is to read a block of input into a buffer, 
then scan each record out of that buffer. A record must be able to
fit in a single buffer. 
Indeed, this can also be considered a safety measure to prevent malicious input 
files from consuming unbounded resources.

As each record is scanned, it tracks whether it was quoted, how many escaped 
characters it contains, and the end position. We do not construct a string for
each field at this point. Only when the field is consumed do we (potentially) 
turn it into a string. By tracking quotes/escapes it is possible to calculate the 
exact space required by the output string. This means that if only a subset of 
the fields are consumed that the performance is even higher.

We allow reading fields as values other than strings, in which case we can
(in netstandard2.1) parse the value directly out of the input buffer to avoid
allocating an extra, scratch string.

## CsvDataWriter

The strategy used to write CSV data is similar to it is read. A single
buffer is maintained that is flushed when it is full. When writing 
fields, it first attempts to write directly to the output buffer (optimistic).
If, during the optimistic write, it is detected that the value needed to be quoted
or escaped, the optimistic write is reset and a slower "pessimistic" write is
performed, which requires quoting/escaping the value in a scratch buffer. Typical
CSV data doesn't require a significant amount of quoting, so this optimization
shows some performance benefit in real world scenarios.

Similarly, when writing primitive values, we can format the value directly into
the output buffer when we know that it can't possibly contain a delimiter. This
is only supported on netstandard2.1.