using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NBitcoin;

namespace Sample.Blockchain
{
    public class IndexBlockTask : IndexTask<BlockInfo>
    {
        public int IndexedBlocks;

        private List<BlockInfo> _blocksToProcess;
        public IndexBlockTask(Indexer indexer) : base(indexer)
        {
        }

        protected override async Task EnsureSetup()
        {
        }

        protected override void BeginProcess()
        {
            Debug.Assert(_blocksToProcess == null);
            _blocksToProcess = new List<BlockInfo>();
        }

        protected override bool ProcessBlock(BlockInfo block)
        {
            _blocksToProcess.Add(block);

            return _blocksToProcess.Count >= 10000;
        }

        protected override List<BlockInfo> EndProcess()
        {
            var list = _blocksToProcess;
            _blocksToProcess = null;

            return list;
        }

        protected override void IndexCore(List<BlockInfo> items)
        {
            // We need to ensure we are disposing before to ensure that the objects do exist when uploading. 
            using (var bulkInsert = Store.BulkInsert())
            {
                foreach (var block in items)
                {
                    bulkInsert.Store(block);
                }
            }

            using (var session = Store.OpenSession())
            {
                foreach (var block in items)
                {
                    session.Advanced.StoreAttachment(block.Id, "BlockHeader", new MemoryStream(block.Header.ToBytes()));
                }

                session.SaveChanges();

                Interlocked.Add(ref IndexedBlocks, items.Count);
            }
        }
    }
}
