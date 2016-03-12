using System.Collections.Generic;

namespace Patterns.LogParsers
{
    public interface ILogParser
    {
        KeyValuePair<string, int>[] GetTop10Users(string logPath);
    }
}