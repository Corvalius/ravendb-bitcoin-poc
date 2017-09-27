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

            var sp = Stopwatch.StartNew();

            InsertMultiThreaded(directory, store);

            Console.WriteLine($"Elapsed: {sp.ElapsedMilliseconds}");
        }
        
        private static void InsertMultiThreaded(DirectoryInfo directory, DocumentStore store)
        {
            var queue = new BlockingCollection<Tuple<Block, List<Transaction>>>(500);
            var filesQueue = new BlockingCollection<FileInfo>();

            var files = directory.EnumerateFiles("blk*.dat")
                .OrderByDescending(x => x.Name);

            foreach (var file in files)
                filesQueue.Add(file);

            var source = new CancellationTokenSource();

            int processedRecords = 0;
            int insertedRecords = 0;

            var workers = new List<Task>();

            for (int i = 0; i < 3; i++)
            {
                var worker = Task.Factory.StartNew(t =>
                {
                    var token = (CancellationToken)t;
                    var bulkInsert = store.BulkInsert();

                    while (!token.IsCancellationRequested)
                    {
                        // We are waiting for no more than 5 seconds a time.
                        if (!queue.TryTake(out var tuple))
                        {
                            Thread.Sleep(250);
                            continue;
                        }

                        bulkInsert.Store(tuple.Item1);
                        foreach (Transaction tx in tuple.Item2)
                            bulkInsert.Store(tx);

                        Interlocked.Increment(ref insertedRecords);
                    }
                }, source.Token);

                workers.Add(worker);
            }                       

            for (int i = 0; i < 10; i++)
            {
                var processor = Task.Factory.StartNew(t =>
                {
                    var token = (CancellationToken)t;
                    while (!token.IsCancellationRequested)
                    {
                        // We are waiting for no more than 5 seconds a time.
                        if (!filesQueue.TryTake(out var file))
                        {
                            Thread.Sleep(250);
                            continue;
                        }

                        Console.WriteLine();
                        Console.WriteLine($"Processing {file.Name}");

                        var blockStore = new BlockStore(file.DirectoryName, Network.Main);
                        foreach (var blk in blockStore.EnumerateFile(file))
                        {
                            if (blk.Item == null)
                                continue;

                            try
                            {
                                var blockModel = new Block(blk.Item);
                                var transactions = new List<Transaction>();
                                foreach (var tx in blk.Item.Transactions)
                                {
                                    if (tx == null)
                                        continue;

                                    transactions.Add(new Transaction(blk.Item, tx));
                                }


                                queue.Add(new Tuple<Block, List<Transaction>>(blockModel, transactions), token);
                            }
                            catch (Exception e)
                            {
                                Console.WriteLine(e);
                            }

                            Interlocked.Increment(ref processedRecords);
                        }
                    }
                }, source.Token);

                workers.Add(processor);
            }

            while (true)
            {
                while (Console.KeyAvailable == false)
                {
                    Console.Write($"\rProcessed: {processedRecords}, Inserted: {insertedRecords}, In-Queue: {processedRecords-insertedRecords}");
                    Thread.Sleep(2000); // Loop until input is entered.
                }

                var cki = Console.ReadKey();
                if (cki.Key == ConsoleKey.Q)
                    break;
            }

            source.Cancel();

            Task.WaitAll(workers.ToArray());
        }

        private static void InsertSingleThreaded(DirectoryInfo directory, DocumentStore store)
        {
            var blockStore = new BlockStore(directory.FullName, Network.Main);
            var bulkInsert = store.BulkInsert();

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
            }
        }

        public static string GetUsage(ParserResult<SampleOptions> results)
        {
            return HelpText.AutoBuild(results, (HelpText current) => HelpText.DefaultParsingErrorsHandler(results, current));
        }
    }
}
