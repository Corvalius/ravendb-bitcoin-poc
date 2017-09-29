using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NBitcoin;
using NBitcoin.Protocol;
using Newtonsoft.Json;
using Raven.Client.Documents;

namespace Sample.Blockchain
{
    public class Indexer
    {
        public readonly DocumentStore Store;

        public string CheckpointName { get; set; }
        public TimeSpan CheckpointInterval { get; set; }
        public bool IgnoreCheckpoints { get; set; }

        public int From { get; set; }
        public int To { get; set; }

        public Network Network { get; set; }
        public string Endpoint { get; set; }

        private readonly Lazy<ChainBase> _chainbase;
        private readonly Lazy<Node> _node;


        public Indexer(DocumentStore store)
        {
            Endpoint = "localhost";

            Store = store;
            _chainbase = new Lazy<ChainBase>(CreateNodeChain);
            _node = new Lazy<Node>(() => ConnectToNode(false));
        }

        public void IndexBlocks()
        {
            var source = new CancellationTokenSource();

            var blockIndexer = new IndexBlockTask(this);
            var blockFetcher = new BlockFetcher(new Checkpoint(this.CheckpointName + ":Blocks", this.Network), _node.Value, _chainbase.Value, source.Token);
            var task = new[] {blockIndexer.Run(blockFetcher)};

            while (!Task.WaitAll(task, 2000))
            {
                Console.WriteLine($"\rProcessed: {blockIndexer.IndexedAttachments} - Partially Inserted: {blockIndexer.IndexedBlocks}");

                if (Console.KeyAvailable)
                {
                    var cki = Console.ReadKey();
                    if (cki.Key == ConsoleKey.Q)
                    {
                        source.Cancel();
                        break;
                    }
                }
            }

            Console.WriteLine("Done!");
        }

        public void IndexTransactions()
        {
            throw new NotImplementedException();
        }

        public void IndexOrderedBalances()
        {
            throw new NotImplementedException();
        }

        public void IndexWalletBalances()
        {
            throw new NotImplementedException();
        }

        public void IndexChain()
        {
            throw new NotImplementedException();
        }

        public ChainBase GetNodeChain()
        {
            return _chainbase.Value;
        }

        public ChainBase CreateNodeChain()
        {
            var chain = new ConcurrentChain(this.Network);
            _node.Value.SynchronizeChain(chain);
            return chain;
        }

        public Node ConnectToNode( bool isRelay = false)
        {
            var node = string.IsNullOrWhiteSpace(Endpoint) 
                ? Node.Connect(this.Network) 
                : Node.Connect(this.Network, Endpoint, isRelay: isRelay);

            node.VersionHandshake();

            return node;
        }
    }
}
