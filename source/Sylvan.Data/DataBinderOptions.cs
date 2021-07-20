using System.Globalization;

namespace Sylvan.Data
{
	public sealed class DataBinderOptions
	{
		internal static readonly DataBinderOptions Default = new DataBinderOptions();
	
		//public bool ReaderAllowsDynamicAccess { get; set; }
		
		public CultureInfo Culture { get; set; }

		/// <summary>
		/// Indicates how the data source will bind to the target type.
		/// Defaults to <see cref="DataBindingMode.AllProperties"/> which requires that
		/// the datasource have column that binds to each property, but would allow unbound columns.
		/// </summary>
		public DataBindingMode BindingMode { get; set; }

		public bool InferColumnTypeFromProperty { get; set; }

		public DataBinderOptions()
		{
			this.Culture = CultureInfo.InvariantCulture;
			this.BindingMode = DataBindingMode.AllProperties;
		}
	}
}
