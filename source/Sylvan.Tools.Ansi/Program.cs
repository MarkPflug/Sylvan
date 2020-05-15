using SixLabors.ImageSharp;
using SixLabors.ImageSharp.PixelFormats;
using SixLabors.ImageSharp.Processing;
using Sylvan.Terminal;
using System;

namespace Sylvan.Tools.Ansi
{
	class Program
	{
		static void Main(string[] args)
		{
			using (ColorConsole.Enable())
			{
				var vt = new VirtualTerminalWriter(Console.Out);
				var r = new Renderer();
				r.RenderImage(vt, args[0]);
				vt.SetFormatDefault();
				vt.SoftReset();
			}
		}
	}

	class RendererOptions
	{
		public RendererOptions()
		{
			this.Threshold = 6;
		}

		public int Threshold
		{
			get; set;
		}
	}

	class Renderer
	{
		readonly RendererOptions opts;

		public Renderer(RendererOptions? opts = null)
		{
			this.opts = opts ?? new RendererOptions();
		}

		public void RenderImage(VirtualTerminalWriter vvt, string fileName, int width = 80, int height = 80)
		{
			// given an image, this will draw "pixels" by drawing block characters '▀' /u2580
			// where the top half is foreground, and the bottom half is background.
			// Each pass draws two rows of the bitmap, which produces a pixel that is nearly square.
			// if the top and bottom cell are the same, draw a solic block instead, to avoid setting the color twice.

			//var str = new StringWriter();
			//var vvt = new VirtualTerminalWriter(str);

			var img = Image.Load(fileName);
			img.Mutate(x => x.Resize(width, img.Height * width / img.Width));
			var ii = img.CloneAs<Rgba32>();

			byte tr = 0;
			byte tg = 0;
			byte tb = 0;
			byte br = 0;
			byte bg = 0;
			byte bb = 0;			
			vvt.SetForeground(Terminal.Color.Black);
			vvt.SetBackground(Terminal.Color.Black);
			
			vvt.SetCursorPosition(1, 1);
			var h = Math.Min(ii.Height, height * 2);

			for (int y = 0; y < h / 2; y++)
			{
				for (int x = 0; x < ii.Width; x++)
				{
					var t = ii[x, y * 2];
					var b = ii[x, y * 2 + 1];

					// new top rgb
					byte ntr = (byte)(t.R * t.A / 256);
					byte ntg = (byte)(t.G * t.A / 256);
					byte ntb = (byte)(t.B * t.A / 256);

					// new bottom rgb
					byte nbr = (byte)(b.R * b.A / 256);
					byte nbg = (byte)(b.G * b.A / 256);
					byte nbb = (byte)(b.B * b.A / 256);

					if (!Near(ntr, tr) || !Near(ntg, tg) || !Near(ntb, tb))
					{
						vvt.SetForeground(ntr, ntg, ntb);
						tr = ntr;
						tg = ntg;
						tb = ntb;
					}

					if (Near(ntr, nbr) && Near(ntg, nbg) && Near(ntb, nbb))
					{
						vvt.Write('\u2588'); // solid FG block '█'
						continue;
					}
					if (!Near(nbr, br) || !Near(nbg, bg) || !Near(nbb, bb))
					{
						vvt.SetBackground(nbr, nbg, nbb);
						br = nbr;
						bg = nbg;
						bb = nbb;
					}

					vvt.Write('\u2580'); // FG top, BG bot '▀'
				}
				vvt.SetCursorPosition(1, (byte)(y + 2));
			}
			vvt.Flush();			
		}

		bool Near(int l, int r)
		{
			var isnear = (uint)(l + opts.Threshold - r) <= opts.Threshold * 2;
			if (isnear)
			{
				return true;
			}
			return false;
		}
	}
}
