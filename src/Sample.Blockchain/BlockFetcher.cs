using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NBitcoin;
using NBitcoin.Protocol;
using Raven.Client.Documents.Session;

namespace Sample.Blockchain
{
    public class BlockFetcher : IEnumerable<BlockInfo>
    {        
        public readonly Node Node;
        public readonly ChainBase BlockHeaders;

        public Checkpoint Checkpoint { get; private set; }

        public BlockFetcher(Checkpoint checkpoint, Node node, ChainBase chain, CancellationToken token)
        {
            Node = node ?? throw new ArgumentNullException(nameof(node));
            Checkpoint = checkpoint ?? throw new ArgumentNullException(nameof(checkpoint));
            BlockHeaders = chain ?? node.GetChain();
            CancellationToken = token;
            
            InitDefault();
        }

        private void InitDefault()
        {
            NeedSaveInterval = TimeSpan.FromMinutes(15);
            ToHeight = int.MaxValue;
        }

        public TimeSpan NeedSaveInterval { get; set; }
        public CancellationToken CancellationToken { get; set; }

        #region IEnumerable<BlockInfo> Members

        private ChainedBlock _lastProcessed;

        public IEnumerator<BlockInfo> GetEnumerator()
        {
            var fork = BlockHeaders.FindFork(Checkpoint.BlockLocator);
            var headers = BlockHeaders.EnumerateAfter(fork)
                                       .Where(h => h.Height >= FromHeight && h.Height <= ToHeight);

            var first = headers.FirstOrDefault();
            if (first == null)
                yield break;

            var height = first.Height;
            if (first.Height == 1)
            {
                headers = new[] { fork }.Concat(headers);
                height = 0;
            }

            foreach (var block in Node.GetBlocks(headers.Select(b => b.HashBlock), CancellationToken).TakeWhile(b => b != null))
            {
                var header = BlockHeaders.GetBlock(height);
                _lastProcessed = header;

                yield return new BlockInfo(block, height);

                height++;
            }
        }

        #endregion

        #region IEnumerable Members

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        #endregion



        private DateTime _lastSaved = DateTime.UtcNow;
        public bool NeedSave => (DateTime.UtcNow - _lastSaved) > NeedSaveInterval;

        public async Task SaveCheckpoint(IAsyncDocumentSession session)
        {
            if (_lastProcessed != null)
            {
                this.Checkpoint = await Checkpoint.CreateOrUpdate(session, Checkpoint.Id, Checkpoint.Network, _lastProcessed);
            }
            _lastSaved = DateTime.UtcNow;
        }

        public int FromHeight
        {
            get;
            set;
        }

        public int ToHeight
        {
            get;
            set;
        }


    }
}
