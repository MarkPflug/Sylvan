## CsvDataReader

The strategy used to parse the CSV data is to read a block of input into a buffer, then scan each record out of that buffer. A record must be able to
fit in a single buffer. Indeed, this can also be considered a safety measure to prevent malicious input files from consuming unbounded resources.

As each record is scanned, we track whether it was quoted, how many escaped characters it contains, and the end offset. We do not construct a string for
each field at this point. Only when the field is consumed do we (potentially) 
turn it into a string. By tracking quotes/escapes we can calculate the exact 
space required by the output string.

We allow reading fields as values other than strings, in which case we can
(in netstandard2.1) parse the value directly out of the input buffer to avoid
creating an extra string.

## CsvDataWriter

The strategy used to write CSV data is similar to the how we read it. We
maintain a single buffer that will be flushed when it is full. When we write
fields, we first attempt to write directly to the output buffer (optimistic).
If as we are writing the value we detect that the value needed to be quoted
or escaped, we abort the write, reset our position, and try a "pessimistic"
write that requires quoting/escaping the value in a scratch buffer. Typical
CSV data doesn't require a significant amount of quoting, so this optimization
shows some performance benefit in real world scenarios.

Similarly, when writing primitive values, we can format the value directly into
the output buffer when we know that it can't possibly contain a delimiter. This
is only supported on netstandard2.1, though.