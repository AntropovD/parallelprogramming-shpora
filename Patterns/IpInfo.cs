using System.Text.RegularExpressions;

namespace Patterns
{
    public struct IpInfo
    {
        private static readonly Regex LineRE = new Regex(@"(?<ip>\S+)\s+(?<ms>\S+)");

        public static IpInfo Parse(string line)
        {
            var match = LineRE.Match(line);
            return new IpInfo
                   {
                       Ip = match.Groups["ip"].Value,
                       CallDuration = int.Parse(match.Groups["ms"].Value)
                   };
        }

        public string Ip;
        public int CallDuration;
    }
}