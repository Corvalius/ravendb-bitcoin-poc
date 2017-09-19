using System;
using System.Diagnostics;
using CommandLine;
using CommandLine.Text;
using NBitcoin;
using NBitcoin.BitcoinCore;
using System.Linq;
using System.IO;
using Raven.Client.Documents;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;

namespace Sample.Blockchain
{
    public class SampleOptions
    {
        [Option("Blocks", HelpText = "The path to the block file", Required = true, DefaultValue = @"data\blocks")]
        public string BlocksPath
        {
            get;
            set;
        }

        [Option("Url", HelpText = "The server url", Required = true, DefaultValue = @"http://localhost:8080")]
        public string ServerUrl
        {
            get;
            set;
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            var options = Parser.Default.ParseArguments<SampleOptions>(args);
            if ( options.Errors.Any() )
            {
                Console.WriteLine(GetUsage(options));
                return;
            }

            var directory = new DirectoryInfo(options.Value.BlocksPath);
            if ( !directory.Exists )
            {
                Console.WriteLine("Directory for reading blocks doesn't exist.");
                Console.WriteLine();
                Console.WriteLine(GetUsage(options));
                return;
            }

            var databaseRecord = new DatabaseRecord("BlockChain");

            var store = new DocumentStore
            {
                Urls = new[] { options.Value.ServerUrl },
                Database = databaseRecord.DatabaseName
            };
            store.Initialize();


            try
            {
                store.Admin.Server.Send(new CreateDatabaseOperation(databaseRecord));
            }
            catch (ConcurrencyException)
            {
                // Nothing to be done here, the database already exist.
            }

            var blockStore = new BlockStore(directory.FullName, Network.Main);
            var bulkInsert = store.BulkInsert();

            int i = 0;
            //foreach (var blk in blockStore.EnumerateFile(Path.Combine(directory.FullName, "blk00958.dat")))            
            foreach (var blk in blockStore.EnumerateFolder())
			{
                var blockModel = new Block(blk.Item);
			    bulkInsert.Store(blockModel);

                foreach (var tx in blk.Item.Transactions)
			    {
			        var txModel = new Transaction(blk.Item, tx);
			        bulkInsert.Store(txModel);
                }

                i++;
            }
        }

        public static string GetUsage(ParserResult<SampleOptions> results)
        {
            return HelpText.AutoBuild(results, (HelpText current) => HelpText.DefaultParsingErrorsHandler(results, current));
        }
    }
}
