using System;

namespace JPEG
{
	public class DCT
	{
		public static double[,] DCT2D(double[,] input)
		{
			int height = input.GetLength(0);
			int width = input.GetLength(1);
			double[,] coeffs = new double[width, height];

			for(int u = 0; u < width; u++)
			{
				for(int v = 0; v < height; v++)
				{
					double sum = 0d;
					for(int x = 0; x < width; x++)
					{
						for(int y = 0; y < height; y++)
						{
							double a = input[x, y];
							sum += BasisFunction(a, u, v, x, y, height, width);
						}
					}
					coeffs[u, v] = sum * Beta(height, width) * Alpha(u) * Alpha(v);
				}
			}
			return coeffs;
		}

		public static double[,] IDCT2D(double[,] coeffs)
		{
			int height = coeffs.GetLength(0);
			int width = coeffs.GetLength(1);
			double[,] output = new double[width, height];

			for(int x = 0; x < width; x++)
			{
				for(int y = 0; y < height; y++)
				{
					double sum = 0d;

					for(int u = 0; u < width; u++)
					{
						for(int v = 0; v < height; v++)
						{
							double a = coeffs[u, v];
							sum += BasisFunction(a, u, v, x, y, height, width) * Alpha(u) * Alpha(v);
						}
					}
					output[x, y] = sum * Beta(height, width);
				}
			}
			return output;
		}

		public static double BasisFunction(double a, double u, double v, double x, double y, int height, int width)
		{
			double b = Math.Cos(((2d * x + 1d) * u * Math.PI) / (2 * width));
			double c = Math.Cos(((2d * y + 1d) * v * Math.PI) / (2 * height));

			return a * b * c;
		}

		private static double Alpha(int u)
		{
			if(u == 0)
				return 1 / Math.Sqrt(2);
			return 1;
		}

		private static double Beta(int height, int width)
		{
			return 1d / width + 1d / height;
		}
	}
}