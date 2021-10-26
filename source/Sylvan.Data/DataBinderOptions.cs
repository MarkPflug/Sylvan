using System.Globalization;

namespace Sylvan.Data
{
	/// <summary>
	/// Options for configuring a data binder.
	/// </summary>
	public sealed class DataBinderOptions
	{
		internal static readonly DataBinderOptions Default = new DataBinderOptions();
	
		/// <summary>
		/// The culture to use when converting string values during binding.
		/// </summary>
		public CultureInfo Culture { get; set; }

		/// <summary>
		/// Indicates how the data source will bind to the target type.
		/// Defaults to <see cref="DataBindingMode.AllProperties"/> which requires that
		/// the datasource have column that binds to each property, but would allow unbound columns.
		/// </summary>
		public DataBindingMode BindingMode { get; set; }

		/// <summary>
		/// Indicates that the target member type is used to indicate
		/// how to access the data source. This can be used when
		/// the data reader might not have a schema, and allows
		/// accessing fields using multiple accessors.
		/// </summary>
		/// <remarks>
		/// This is primarily to support the Sylvan CSV, which allows
		/// accesssing CSV (string) fields using any DbDataReader accessor.
		/// </remarks>
		public bool InferColumnTypeFromMember { get; set; }

		/// <summary>
		/// Creates a new DataBinderOptions instance.
		/// </summary>
		public DataBinderOptions()
		{
			this.Culture = CultureInfo.InvariantCulture;
			this.BindingMode = DataBindingMode.AllProperties;
		}
	}
}
