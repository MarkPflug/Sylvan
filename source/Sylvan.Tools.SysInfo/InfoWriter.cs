using Sylvan.Terminal;
using System.Collections.Generic;

namespace Sylvan.Tools
{
	class InfoWriter
	{
		const int HeaderWidth = 80;
		const int DefaultNameWidth = 16;
		const char HeaderChar = '-';
		const int PrePadCount = 3;
		static readonly string PrePad = new string(HeaderChar, PrePadCount);


		int HeaderColor = 0x40a0f0;
		int LabelColor = 0x60c060;
		int SeparatorColor = 0xa0a0a0;
		int ValueColor = 0xe0e0e0;

		readonly ColorConsole trm;

		public InfoWriter(ColorConsole trm)
		{
			this.trm = trm;
		}

		void SetForegroundColor(int value)
		{
			var r = (byte)(value >> 16 & 0xff);
			var g = (byte)(value >> 8 & 0xff);
			var b = (byte)(value & 0xff);
			trm.SetForeground(r, g, b);
		}

		public void SetLabelColor()
		{
			SetForegroundColor(LabelColor);
		}

		public void SetSeparatorColor()
		{
			SetForegroundColor(SeparatorColor);
		}

		public void SetValueColor()
		{
			SetForegroundColor(ValueColor);
		}

		public void SetHeaderColor()
		{
			SetForegroundColor(HeaderColor);
		}

		public void Label(string label)
		{
			SetLabelColor();
			trm.Write(label);
		}

		public void Separator(string separator = ": ")
		{
			SetSeparatorColor();
			trm.Write(separator);
		}

		public void Header(string heading)
		{
			trm.WriteLine();
			SetHeaderColor();

			var l = HeaderWidth - 2 - PrePadCount - heading.Length;

			trm.Write(PrePad);
			trm.Write(' ');
			trm.Write(heading);
			trm.Write(' ');
			if (l > 0)
			{
				trm.Write(new string(HeaderChar, l));
			}
			trm.WriteLine();
			trm.SetDefaults();
		}

		public void Value(string name, string value, int width = DefaultNameWidth)
		{
			Value(name, new string[] { value }, width);
		}

		public void Value(string name, IEnumerable<string> values, int width = DefaultNameWidth)
		{
			Label(string.Format("{0," + width + "}", name));
			Separator();
			SetValueColor();

			bool first = true;
			foreach (var value in values)
			{
				if (first)
				{
					first = false;
				}
				else
				{
					trm.Write(new string(' ', width + 2));
				}
				trm.Write(value);
				trm.WriteLine();
			}

			trm.SetDefaults();
		}
	}
}
