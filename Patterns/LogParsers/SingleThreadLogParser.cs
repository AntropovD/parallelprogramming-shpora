using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Patterns.LogParsers
{
    public class SingleThreadLogParser : ILogParser
    {
        public KeyValuePair<string, int>[] GetTop10Users(string logPath)
        {
            var usersStats = new Dictionary<string, int>();

            foreach (var line in File.ReadLines(logPath))
            {
                var ipInfo = IpInfo.Parse(line);
                
                if (!usersStats.ContainsKey(ipInfo.Ip))
                    usersStats.Add(ipInfo.Ip, ipInfo.CallDuration);
                else
                    usersStats[ipInfo.Ip] += ipInfo.CallDuration;
            }

            return usersStats.OrderByDescending(keyValuePair => keyValuePair.Value).Take(10).ToArray();
        }
    }
}