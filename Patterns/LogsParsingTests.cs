using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using NUnit.Framework;
using Patterns.LogParsers;

namespace Patterns
{
    [TestFixture]
    public class LogsParsingTests
    {
        [SetUp]
        public void SetUp()
        {
            if (!File.Exists("Files/ips.txt"))
                ZipFile.ExtractToDirectory("Files/ips.zip", "Files");
        }

        [Test]
        public void SingleThread()
        {
            Test(new SingleThreadLogParser());
        }
        
        [Test]
        public void ParallelForeach()
        {
            Test(new ParallelForeachLogParser());
        }
        
        [Test]
        public void ParallelFor()
        {
            Test(new ParallelForLogParser());
        }

        [Test]
        public void MapReduce()
        {
            Test(new MapReduceLogParser());
        }

        [Test]
        public void LINQ()
        {
            Test(new LINQLogParser());
        }

        [Test]
        public void PLINQ()
        {
            Test(new PLINQLogParser());
        }

        [Test]
        public void PLINQWithDictionary()
        {
            Test(new PLINQWithDictionaryLogParser());
        }

        [Test]
        public void SingleThreadConcurrentDictionary()
        {
            Test(new SingleThreadConcurrentDictionaryLogParser());
        }

        [Test]
        public void ParallelForeachConcurrentDictionary()
        {
            Test(new SingleThreadConcurrentDictionaryLogParser());
        }

        private static void Test(ILogParser parser)
        {
            var expectedResult = new Dictionary<string, int>
                                 {
                                     {"46.17.203.253", 25060434},
                                     {"213.24.62.119", 23502468},
                                     {"91.208.121.254", 20443768},
                                     {"46.17.201.50", 15585076},
                                     {"46.17.201.58", 12967906},
                                     {"195.234.190.100", 12378304},
                                     {"195.43.90.253", 10469620},
                                     {"195.130.216.202", 9731071},
                                     {"194.186.187.146", 7748171},
                                     {"194.190.140.73", 7322802}
                                 };
            const string logPath = "Files/ips.txt";
            var result = parser.GetTop10Users(logPath);
            CollectionAssert.AreEqual(expectedResult, result);
        }
    }
}