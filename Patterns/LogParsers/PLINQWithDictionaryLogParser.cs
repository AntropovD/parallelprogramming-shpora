using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Patterns.LogParsers
{
    public class PLINQWithDictionaryLogParser : ILogParser
    {
        public KeyValuePair<string, int>[] GetTop10Users(string logPath)
        {
            var usersStats = new Dictionary<string, int>();

            File.ReadLines(logPath).AsParallel()
                .Select(IpInfo.Parse)
                .ForAll(ipInfo =>
                        {
                            lock (usersStats)
                            {
                                if (!usersStats.ContainsKey(ipInfo.Ip))
                                    usersStats.Add(ipInfo.Ip, ipInfo.CallDuration);
                                else
                                    usersStats[ipInfo.Ip] += ipInfo.CallDuration;
                            }
                        });

            return usersStats.AsParallel()
                .OrderByDescending(keyValuePair => keyValuePair.Value).Take(10).ToArray();
        }
    }
}