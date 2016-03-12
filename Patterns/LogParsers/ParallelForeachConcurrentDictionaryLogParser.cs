using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Patterns.LogParsers
{
    public class ParallelForeachConcurrentDictionaryLogParser : ILogParser
    {
        public KeyValuePair<string, int>[] GetTop10Users(string logPath)
        {
            var usersStats = new ConcurrentDictionary<string, int>();

            Parallel.ForEach(File.ReadLines(logPath),
                line =>
                {
                    var ipInfo = IpInfo.Parse(line);

                    usersStats.AddOrUpdate(ipInfo.Ip, ipInfo.CallDuration,
                        (key, oldDuration) => oldDuration + ipInfo.CallDuration);
                });

            return usersStats.OrderByDescending(keyValuePair => keyValuePair.Value).Take(10).ToArray();
        }
    }
}