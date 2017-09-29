using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using CommandLine;
using CommandLine.Text;
using NBitcoin;
using NBitcoin.BitcoinCore;
using System.Linq;
using System.IO;
using System.IO.Compression;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;

namespace Sample.Blockchain
{


    public class IndexerApp
    {
        public IndexerApp(Options options)
        {
            this._options = options;
        }

        public class Options
        {
            [Option('n', "Network", DefaultValue = "Main", Required = false, HelpText = "The network to use. 'Main' or 'Test'")]
            public string Network { get; set; }

            [Option('b', "IndexBlocks", DefaultValue = false, Required = false, HelpText = "Index blocks into attachments")]
            public bool IndexBlocks { get; set; }

            [Option("IgnoreCheckpoints", HelpText = "Ignore checkpoints (Do not save them, nor load them)", Required = false, DefaultValue = false)]
            public bool IgnoreCheckpoints { get; set; }

            [Option("ListCheckpoints", HelpText = "list checkpoints", Required = false, DefaultValue = false)]
            public bool ListCheckpoints { get; set; }

            [Option("AddCheckpoint", HelpText = "add/set checkpoint (format : \"CheckpointName:Height\")", Required = false)]
            public string AddCheckpoint { get; set; }

            [Option("DeleteCheckpoint", HelpText = "delete checkpoint (format : checkpoint name)", Required = false)]
            public string DeleteCheckpoint { get; set; }

            [Option('c', "CountBlkFiles", HelpText = "Count the number of blk file downloaded by bitcoinq", DefaultValue = false, Required = false)]
            public bool CountBlkFiles { get; set; }

            [Option("From",
                HelpText = "The height of the first block to index",
                DefaultValue = 0,
                Required = false)]
            public int From { get; set; }

            [Option("To",
                HelpText = "The height of the last block (included)",
                DefaultValue = 99999999,
                Required = false)]
            public int To { get; set; }


            [Option('t', "IndexTransactions", DefaultValue = false, Required = false, HelpText = "Index transactions")]
            public bool IndexTransactions { get; set; }

            [Option('w', "IndexWallets", DefaultValue = false, Required = false, HelpText = "Index wallets")]
            public bool IndexWallets { get; set; }

            [Option('a', "IndexAddresses", DefaultValue = false, Required = false, HelpText = "Index bitcoin addresses")]
            public bool IndexAddresses { get; set; }

            [Option('m', "IndexMainChain", DefaultValue = false, Required = false, HelpText = "Index the main chain")]
            public bool IndexChain { get; set; }

            [Option("All", DefaultValue = false, Required = false, HelpText = "Index all objects, equivalent to -m -a -b -t -w")]
            public bool All { get; set; }

            [Option("Checkpoint", DefaultValue = "default", Required = false, HelpText = "The name of the checkpoint for this instance")]
            public string CheckpointName { get; set; }

            [Option("CheckpointInterval", DefaultValue = "00:15:00", Required = false, HelpText = "Interval after which the indexer flush its progress and save a checkpoint")]
            public string CheckpointInterval { get; set; }

            [Option("Server", HelpText = "The server url", Required = true, DefaultValue = @"http://localhost:8080")]
            public string ServerUrl { get; set; }
        }


        private readonly Options _options;
        private DocumentStore _store;

        public static async Task Main(string[] args)
        {
            var options = Parser.Default.ParseArguments<Options>(args);
            if ( options.Errors.Any() )
            {
                Console.WriteLine(GetUsage(options));
                return;
            }

            await new IndexerApp(options.Value)
                .Initialize()
                .Run();
        }

        private IndexerApp Initialize()
        {
            var databaseRecord = new DatabaseRecord("BlockChain");

            _store = new DocumentStore
            {
                Urls = new[] { _options.ServerUrl },
                Database = databaseRecord.DatabaseName
            };
            _store.Initialize();

            try
            {
                _store.Admin.Server.Send(new CreateDatabaseOperation(databaseRecord));
            }
            catch (ConcurrencyException)
            {
                // Nothing to be done here, the database already exist.
            }

            // If we want to index all, we setup that.
            if (_options.All)
            {
                _options.IndexAddresses = true;
                _options.IndexBlocks = true;
                _options.IndexWallets = true;
                _options.IndexChain = true;
                _options.IndexTransactions = true;
            }

            return this;
        }

        private Network ParseNetwork(string network)
        {
            return network.Equals("Main", StringComparison.InvariantCultureIgnoreCase) ? Network.Main
                :  network.Equals("Test", StringComparison.InvariantCultureIgnoreCase) ? Network.TestNet
                    : throw new ArgumentException($"The value '{network}' is not valid.");
        }

        public async Task Run()
        {
            var sp = Stopwatch.StartNew();

            var indexer = new Indexer(_store);
            indexer.CheckpointName = _options.CheckpointName;
            indexer.CheckpointInterval = TimeSpan.Parse(_options.CheckpointInterval);
            indexer.IgnoreCheckpoints = _options.IgnoreCheckpoints;
            indexer.From = _options.From;
            indexer.To = _options.To;
            indexer.Network = ParseNetwork(_options.Network);

            if (_options.ListCheckpoints)
            {
                await ShowCheckpoints(indexer);
            }

            if (!string.IsNullOrWhiteSpace(_options.DeleteCheckpoint))
            {
                await DeleteCheckpoint(indexer);
            }

            if (!string.IsNullOrWhiteSpace(_options.AddCheckpoint))
            {
                await CreateCheckpoint(indexer);
            }

            if (_options.IndexBlocks)
            {
                indexer.IndexBlocks();
            }
            if (_options.IndexTransactions)
            {
                indexer.IndexTransactions();
            }
            if (_options.IndexAddresses)
            {
                indexer.IndexOrderedBalances();
            }
            if (_options.IndexWallets)
            {
                indexer.IndexWalletBalances();
            }
            if (_options.IndexChain)
            {
                indexer.IndexChain();
            }

            Console.WriteLine($"Elapsed: {sp.ElapsedMilliseconds}");
        }

        private async Task CreateCheckpoint(Indexer indexer)
        {
            ChainBase chain = indexer.GetNodeChain();

            var checkpointName = _options.AddCheckpoint;

            var split = _options.AddCheckpoint.Split(':');
            var name = split[0];
            var height = int.Parse(split[1]);
            var b = chain.GetBlock(height);

            using (var session = _store.OpenAsyncSession())
            {
                await Checkpoint.CreateOrUpdate(session, name, indexer.Network, b.GetLocator());
                await session.SaveChangesAsync();

                Console.WriteLine("Checkpoint " + checkpointName + " saved to height " + b.Height);
            }
        }

        private async Task DeleteCheckpoint(Indexer indexer)
        {
            using (var session = _store.OpenAsyncSession())
            {
                session.Delete( Checkpoint.ToId(_options.DeleteCheckpoint, indexer.Network.Name));
                await session.SaveChangesAsync();
            }

            Console.WriteLine("Checkpoint " + _options.DeleteCheckpoint + " deleted");
        }

        private async Task ShowCheckpoints(Indexer indexer)
        {
            ChainBase chain = indexer.GetNodeChain();

            using (var session = _store.OpenAsyncSession())
            {
                var checkpoints = await session.Advanced.LoadStartingWithAsync<Checkpoint>(Checkpoint.ToPrefix(indexer.Network.Name));                                         
                foreach (var checkpoint in checkpoints )
                {
                    await checkpoint.Prepare(session);

                    var fork = chain.FindFork(checkpoint.BlockLocator);

                    Console.WriteLine("Name : " + checkpoint.Name);
                    if (fork != null)
                    {
                        Console.WriteLine("Height : " + fork.Height);
                        Console.WriteLine("Hash : " + fork.HashBlock);
                    }
                    Console.WriteLine();
                }
            }
        }

        public static string GetUsage(ParserResult<Options> results)
        {
            return HelpText.AutoBuild(results, (HelpText current) => HelpText.DefaultParsingErrorsHandler(results, current));
        }
    }
}
