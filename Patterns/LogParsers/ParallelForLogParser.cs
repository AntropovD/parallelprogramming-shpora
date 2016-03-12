using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Patterns.LogParsers
{
    public class ParallelForLogParser : ILogParser
    {
        public KeyValuePair<string, int>[] GetTop10Users(string logPath)
        {
            var usersStats = new Dictionary<string, int>();
            var fileLines = File.ReadAllLines(logPath);

            Parallel.For(0, fileLines.Length,
                i =>
                {
                    var ipInfo = IpInfo.Parse(fileLines[i]);

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