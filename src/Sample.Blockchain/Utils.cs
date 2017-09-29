using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using NBitcoin;

namespace Sample.Blockchain
{
    public static class Utils
    {
        public static ulong ToUnixTime(this DateTimeOffset target)
        {
            var date = target.ToUniversalTime();
            var unixTimestamp = (target - date).TotalSeconds;

            return (ulong)unixTimestamp;
        }
    }
}
