# Sylvan.Data Release Notes

_0.2.15_
- Adds async support to a few DbDataReader implementations that were previously missing it. This lead to incorrect behavior when used asynchronously.

_0.2.14_
- DataValidationContext now exposes `RowNumber` and `IsValid(int ordinal)` to assist in custom validations.
- Calling DataValidationContext.SetValue will clear any error associated with that field.
- Fixes an issue with data nullability handling in `DbDataReader.Validate/ValidateSchema` methods.
- The `DbDataReader.Validate/ValidateSchema` methods remain in "preview".

_0.2.13_
- Data binder now throws an exception if *no* properties get bound in `BindMode.Any`, now at least one must get bound.
- Data binder now honors `DataMemberAttribute.IsRequired` property. If unbound, binder creation will throw an `UnboundMemberException` exception.
- `AsDataReader` extension method now uses DataMemberAttribute.Name as the header string if present, otherwise the property name is used.

_0.2.12_
- `GetRecords<T>` and `GetRecordsAsync<T>` methods now accept optional `DataBinderOptions` parameter to configure binding behavior.
- DataBinder better handles some scenarios with enum types.
- Improvements to `DbDataReader.ValidateSchema` extension method (still experimental).

_0.2.11_
- Fixes DbDataReader.WriteJson/Async methods to correctly handle local DateTime values.
- DbDataReader.WriteJson/Async methods now write binary values as base64 encoded strings.

_0.2.10_
- Fixes a bug with `WithColumns` extension method handling of nullable columns.

_0.2.9_
- Fix DataBinder issue when binding DateOnly/TimeOnly properties.

_0.2.8_
- Fix DbDataWriter.WriteJsonAsync to properly async dispose the writer.

_0.2.7_
- Adds `IAsyncEnumerable<T>.AsDataReader()` extension.
- Adds `DbDataReader.WriteJson(Stream)` extension.

_0.2.6_
- Adds support for reading `decimal` properties with ObjectDataReader.
- Adds support for binding DateOnly and TimeOnly values.

_0.2.5_
- Expose GetRecordsAsync to net6.0.

_0.2.4_
- Add experimental DbDataReader.ValidateSchema.
- Add support for binding to DateOnly and TimeOnly.

_0.2.3_
- Fix an issue binding to `record class` types.
- Add `class` constraint to binder methods, as structs are not currently supported.
- Restore ObjectDataReader methods that were inadvertently removed in the previous release.

_0.2.2_
- Expose DbDataReaderAdapter base class.
- Add DbDataReader.AsVariableField extension method.
- Add `TakeWhile`.
- Add API documentation.

_0.2.1_
- Adds `DbDataReader.Skip`/`Take` extension methods.
- Adds GetRecordsAsync extension method.
