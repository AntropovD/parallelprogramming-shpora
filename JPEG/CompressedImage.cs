using System;
using System.Collections.Generic;
using System.IO;

namespace JPEG
{
	public class CompressedImage
	{
		public int Height { get; set; }
		public int Width { get; set; }

		public int CompressionLevel { get; set; }
		public int FrequencesPerBlock { get; set; }

		public List<double> Frequences { get; set; }

		public void Save(string path)
		{
			using(var sw = new FileStream(path, FileMode.Create))
			{
				var heightBytes = BitConverter.GetBytes(Height);
				sw.Write(heightBytes, 0, 4);

				var widthBytes = BitConverter.GetBytes(Width);
				sw.Write(widthBytes, 0, 4);

				var compressionLevelBytes = BitConverter.GetBytes(CompressionLevel);
				sw.Write(compressionLevelBytes, 0, 4);

				var frequencesPerBlockBytes = BitConverter.GetBytes(FrequencesPerBlock);
				sw.Write(frequencesPerBlockBytes, 0, 4);

				int blockSize = FrequencesPerBlock;
				for(int blockNum = 0; blockNum * blockSize < Frequences.Count; blockNum++)
				{
					for(int freqNum = 0; freqNum < blockSize; freqNum++)
					{
						var portion = BitConverter.GetBytes((short)Frequences[blockNum * blockSize + freqNum]);
						sw.Write(portion, 0, portion.Length);
					}
				}
			}
		}

		public static CompressedImage Load(string path, int DCTSize)
		{
			var result = new CompressedImage();
			using(var sr = new FileStream(path, FileMode.Open))
			{
				byte[] buffer = new byte[4];

				sr.Read(buffer, 0, 4);
				result.Height = BitConverter.ToInt32(buffer, 0);

				sr.Read(buffer, 0, 4);
				result.Width = BitConverter.ToInt32(buffer, 0);

				sr.Read(buffer, 0, 4);
				result.CompressionLevel = BitConverter.ToInt32(buffer, 0);

				sr.Read(buffer, 0, 4);
				var blockSize = result.FrequencesPerBlock = BitConverter.ToInt32(buffer, 0);

				var blocksCount = result.Height * result.Width / (DCTSize * DCTSize);
				result.Frequences = new List<double>(blocksCount * result.FrequencesPerBlock);

				for(int blockNum = 0; blockNum < blocksCount; blockNum++)
				{
					for(int freqNum = 0; freqNum < blockSize; freqNum++)
					{
						sr.Read(buffer, 0, 2);
						result.Frequences.Add(BitConverter.ToInt16(buffer, 0));
					}
				}
			}
			return result;
		}
	}
}