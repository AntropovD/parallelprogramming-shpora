using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Patterns.LogParsers
{
    public class SingleThreadConcurrentDictionaryLogParser : ILogParser
    {
        public KeyValuePair<string, int>[] GetTop10Users(string logPath)
        {
            var usersStats = new ConcurrentDictionary<string, int>();

            foreach (var line in File.ReadLines(logPath))
            {
                var ipInfo = IpInfo.Parse(line);

                usersStats.AddOrUpdate(ipInfo.Ip, ipInfo.CallDuration, (key, oldDuration) => oldDuration + ipInfo.CallDuration);
            }

            return usersStats.OrderByDescending(keyValuePair => keyValuePair.Value).Take(10).ToArray();
        }
    }
}