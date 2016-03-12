using System;
using System.Collections.Generic;
using System.Drawing;
using System.Drawing.Imaging;
using System.IO;
using System.Runtime.InteropServices;

namespace JPEG
{
	class Program
	{
		const int DCTSize = 8;

		static void Main(string[] args)
		{
			const int CompressionLevel = 8;

			try
			{
				var fileName = @"..\..\sample.bmp";
				var compressedFileName = fileName + ".compressed." + DCTSize + "." + CompressionLevel;
				var uncompressedFileName = fileName + ".uncompressed." + DCTSize + "." + CompressionLevel + ".bmp";

				var bmp = (Bitmap)Image.FromFile(fileName);

				if(bmp.Width % DCTSize != 0 || bmp.Height % DCTSize != 0)
					throw new Exception(string.Format("Image width and height must be multiple of {0}", DCTSize));

				var grayscaleMatrix = BitmapToGrayscaleMatrix(bmp);

				var compressedImage = CompressWithDCT(grayscaleMatrix, CompressionLevel);
				compressedImage.Save(compressedFileName);

				compressedImage = CompressedImage.Load(compressedFileName, DCTSize);
				var uncompressedImage = UncompressWithDCT(compressedImage);
				var grayscaleBmp = GrayscaleMatrixToBitmap(uncompressedImage);
				grayscaleBmp.Save(uncompressedFileName, ImageFormat.Bmp);
			}
			catch(Exception e)
			{
				Console.WriteLine(e);
			}
		}

		static double[,] BitmapToGrayscaleMatrix(Bitmap bmp)
		{
			var result = new double[bmp.Height, bmp.Width];

			BitmapData bitmapData1 = bmp.LockBits(new Rectangle(0, 0, bmp.Width, bmp.Height), ImageLockMode.ReadOnly, bmp.PixelFormat);
			var pixelSize = Image.GetPixelFormatSize(bmp.PixelFormat) / 8;

			int i, j;
			unsafe
			{
				byte* imagePointer1 = (byte*)bitmapData1.Scan0;

				for(j = 0; j < bitmapData1.Height; j++)
				{
					for(i = 0; i < bitmapData1.Width; i++)
					{
						result[j, i] = ((imagePointer1[0] + imagePointer1[1] + imagePointer1[2]) / 3.0);
						imagePointer1 += pixelSize;
					}
					imagePointer1 += bitmapData1.Stride - (bitmapData1.Width * pixelSize);
				}
			}
			bmp.UnlockBits(bitmapData1);
			return result;
		}

		static Bitmap GrayscaleMatrixToBitmap(double[,] grayscaleMatrix)
		{
			var result = new Bitmap(grayscaleMatrix.GetLength(1), grayscaleMatrix.GetLength(0), PixelFormat.Format24bppRgb);
			BitmapData bitmapData1 = result.LockBits(new Rectangle(0, 0, result.Width, result.Height), ImageLockMode.ReadOnly, PixelFormat.Format24bppRgb);

			int i, j;
			unsafe
			{
				byte* imagePointer1 = (byte*)bitmapData1.Scan0;
				for(j = 0; j < bitmapData1.Height; j++)
				{
					for(i = 0; i < bitmapData1.Width; i++)
					{
						var componentValue = (int)grayscaleMatrix[j, i];
						if(componentValue > byte.MaxValue)
							componentValue = byte.MaxValue;
						else if(componentValue < 0)
							componentValue = 0;

						imagePointer1[0] = (byte) componentValue;
						imagePointer1[1] = (byte) componentValue;
						imagePointer1[2] = (byte) componentValue;
						imagePointer1 += 3;
					}
					imagePointer1 += (bitmapData1.Stride - (bitmapData1.Width * 3));
				}
			}
			result.UnlockBits(bitmapData1);

			return result;
		}

		private static CompressedImage CompressWithDCT(double[,] channelPixels, int compressionLevel = 4)
		{
			int frequencesPerBlock = -1;

			var height = channelPixels.GetLength(0);
			var width = channelPixels.GetLength(1);

			var result = new List<double>();

			for(int y = 0; y < height; y += DCTSize)
			{
				for(int x = 0; x < width; x += DCTSize)
				{
					var subMatrix = GetSubMatrix(channelPixels, y, DCTSize, x, DCTSize);
					ShiftMatrixValues(subMatrix, -128);

					var channelFreqs = DCT.DCT2D(subMatrix);

					frequencesPerBlock = DCTSize * DCTSize;
					for(int i = 0; i < DCTSize; i++)
					{
						for(int j = 0; j < DCTSize; j++)
						{
							if(i + j < compressionLevel)
							{
								result.Add(channelFreqs[i, j]);
								continue;
							}
							channelFreqs[i, j] = 0;
							frequencesPerBlock--;
						}
					}
				}
			}

			return new CompressedImage {CompressionLevel =  compressionLevel, FrequencesPerBlock = frequencesPerBlock, Frequences = result, Height = height, Width = width};
		}

		private static double[,] UncompressWithDCT(CompressedImage image)
		{
			var result = new double[image.Height, image.Width];

			int freqNum = 0;
			for(int y = 0; y < image.Height; y += DCTSize)
			{
				for(int x = 0; x < image.Width; x += DCTSize)
				{
					var channelFreqs = new double[DCTSize,DCTSize];
					for(int i = 0; i < DCTSize; i++)
					{
						for(int j = 0; j < DCTSize; j++)
						{
							if(i + j < image.CompressionLevel)
								channelFreqs[i, j] = image.Frequences[freqNum++];
						}
					}
					var processedSubmatrix = DCT.IDCT2D(channelFreqs);
					ShiftMatrixValues(processedSubmatrix, 128);
					SetSubmatrix(result, processedSubmatrix, y, x);
				}
			}
			return result;
		}

		private static void ShiftMatrixValues(double[,] subMatrix, int shiftValue)
		{
			for(int y = 0; y < subMatrix.GetLength(0); y++)
			{
				for(int x = 0; x < subMatrix.GetLength(1); x++)
				{
					subMatrix[y, x] = subMatrix[y, x] + shiftValue;
				}
			}
		}

		private static void SetSubmatrix(double[,] destination, double[,] source, int yOffset, int xOffset)
		{
			for(int y = 0; y < source.GetLength(0); y++)
			{
				for(int x = 0; x < source.GetLength(1); x++)
				{
					destination[yOffset + y, xOffset + x] = source[y, x];
				}
			}
		}

		private static T[,] GetSubMatrix<T>(T[,] array, int yOffset, int yLength, int xOffset, int xLength)
		{
			var result = new T[DCTSize, DCTSize];
			for(int j = 0; j < yLength; j++)
			{
				for(int i = 0; i < xLength; i++)
				{
					result[j, i] = array[yOffset + j, xOffset + i];
				}
			}
			return result;
		}
	}
}
