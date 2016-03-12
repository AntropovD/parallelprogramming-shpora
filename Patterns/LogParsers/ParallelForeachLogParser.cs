using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Patterns.LogParsers
{
    public class ParallelForeachLogParser : ILogParser
    {
        public KeyValuePair<string, int>[] GetTop10Users(string logPath)
        {
            var usersStats = new Dictionary<string, int>();

            Parallel.ForEach(File.ReadLines(logPath),
                line =>
                {
                    var ipInfo = IpInfo.Parse(line);

                    lock (usersStats)
                    {
                        if (!usersStats.ContainsKey(ipInfo.Ip))
                            usersStats.Add(ipInfo.Ip, ipInfo.CallDuration);
                        else
                            usersStats[ipInfo.Ip] += ipInfo.CallDuration;
                    }
                });

            return usersStats.OrderByDescending(keyValuePair => keyValuePair.Value).Take(10).ToArray();
        }
    }
}