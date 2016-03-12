using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Patterns.LogParsers
{
    public class LINQLogParser : ILogParser
    {
        public KeyValuePair<string, int>[] GetTop10Users(string logPath)
        {
            return File.ReadLines(logPath)
                .Select(IpInfo.Parse)
                .GroupBy(
                    g => g.Ip,
                    g => g.CallDuration,
                    (key, durations) => new KeyValuePair<string, int>(key, durations.Sum()))
                .OrderByDescending(keyValuePair => keyValuePair.Value).Take(10).ToArray();
        }
    }
}