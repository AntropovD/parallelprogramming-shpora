using System.Collections.Generic;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Threading.Tasks;
using log4net;
using log4net.Config;

namespace NMAP
{
	class Program
	{
		static void Main(string[] args)
		{
			XmlConfigurator.Configure();

			var ipAddrs = GenIpAddrs();
			var ports = new[] {21, 25, 80, 443, 3389 };

			ScanSequential(ipAddrs, ports);
		}

		private static void ScanSequential(IPAddress[] ipAddrs, int[] ports)
		{
			foreach(var ipAddr in ipAddrs)
			{
				if(PingAddr(ipAddr) != IPStatus.Success)
					continue;

				foreach(var port in ports)
					CheckPort(ipAddr, port);
			}
		}

		private static IPAddress[] GenIpAddrs()
		{
			var konturAddrs = new List<IPAddress>();
			uint focusIpInt = 0x0ACB112E;
			for(int b = 0; b <= byte.MaxValue; b++)
				konturAddrs.Add(new IPAddress((focusIpInt & 0x00FFFFFF) | (uint) b << 24));
			return konturAddrs.ToArray();
		}

		enum Status
		{
			OPEN,
			FILTERED,
			CLOSED
		}

		private static Status CheckPort(IPAddress ipAddr, int port, int timeout = 3000)
		{
			using(var tcpClient = new TcpClient())
			{
				log.InfoFormat("Checking {0}:{1}", ipAddr, port);

				var connectTask = TcpClientExtensions.Connect(tcpClient, ipAddr, port);
				Status status;
				switch(connectTask.Status)
				{
					case TaskStatus.RanToCompletion:
						status = Status.OPEN;
						break;
					case TaskStatus.Faulted:
						status = Status.CLOSED;
						break;
					default:
						status = Status.FILTERED;
						break;
				}
				log.InfoFormat("Checked {0}:{1} - {2}", ipAddr, port, status);
				return status;
			}
		}
		
		static IPStatus PingAddr(IPAddress ipAddr, int timeout = 3000)
		{
			log.InfoFormat("Pinging {0}", ipAddr);
			using(var ping = new Ping())
			{
				var status = ping.Send(ipAddr, timeout).Status;
				log.InfoFormat("Pinged {0}: {1}", ipAddr, status);
				return status;
			}
		}

		private static readonly ILog log = LogManager.GetLogger(typeof(Program));
	}

	static class TcpClientExtensions
	{
		public static Task Connect(this TcpClient tcpClient, IPAddress ipAddr, int port, int timeout = 3000)
		{
			var connectTask = tcpClient.ConnectAsync(ipAddr, port);
			Task.WaitAny(connectTask, Task.Delay(timeout));
			return connectTask;
		}
	}
}
