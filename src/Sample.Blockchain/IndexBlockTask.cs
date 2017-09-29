using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NBitcoin;
using Raven.Client.Documents;

namespace Sample.Blockchain
{
    public class IndexBlockTask
    {
        private readonly Indexer _indexer;
        private readonly DocumentStore _store;
        public int IndexedBlocks;
        public int IndexedAttachments;

        public IndexBlockTask(Indexer indexer)
        {
            this._indexer = indexer;
            this._store = indexer.Store;
        }

        public const int ItemsPerBatch = 1000;

        public async Task Run(BlockFetcher blockFetcher)
        {
            // PERF: This could be fixed to include the attachment when the client support sending
            //       attachments through bulk inserts.
            var attachmentLists = new List<Task>();

            var bulkInsert = _store.BulkInsert();
            var blockList = new List<BlockInfo>(ItemsPerBatch);
            foreach (var block in blockFetcher)
            {
                if (blockList.Count == ItemsPerBatch)
                {
                    // Spawn upload attachments task. 
                    attachmentLists.Add(UploadAttachments(blockList, blockFetcher.CancellationToken));

                    // End the old and create a new bulk insert. 
                    await bulkInsert.DisposeAsync();
                    bulkInsert = _store.BulkInsert();
                    blockList = new List<BlockInfo>(ItemsPerBatch);
                }

                await bulkInsert.StoreAsync(block);
                blockList.Add(block);

                IndexedBlocks++;
            }

            Task.WaitAll(attachmentLists.ToArray(), blockFetcher.CancellationToken);

            if (!_indexer.IgnoreCheckpoints)
            {
                // Write the checkpoint.
                Console.WriteLine("Checkpoint save not implemented yet.");
            }
        }

        protected async Task UploadAttachments(List<BlockInfo> blocks, CancellationToken token)
        {
            using (var session = _store.OpenAsyncSession())
            {
                foreach (var blk in blocks)
                {
                    session.Advanced.StoreAttachment(blk, "BlockHeader", new MemoryStream(blk.Header.ToBytes()));
                }

                await session.SaveChangesAsync(token);

                Interlocked.Add(ref IndexedAttachments, blocks.Count);
            }
        }


    }
}
