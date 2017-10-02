using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using NBitcoin;
using NBitcoin.Protocol;

namespace Sample.Blockchain
{
    public class BlockFetcher : IEnumerable<BlockInfo>
    {        
        public readonly Node Node;
        public readonly ChainBase BlockHeaders;
        public Block LastProcessed { get; private set; }
        public Checkpoint Checkpoint { get; set; }

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
                LastProcessed = block;
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


        public void SkipToEnd()
        {
            var height = Math.Min(ToHeight, BlockHeaders.Tip.Height);            
            var chainedBlock = BlockHeaders.GetBlock(height);

            this.LastProcessed = Node.GetBlocks(chainedBlock.HashBlock).First();
        }
    }
}
