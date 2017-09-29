using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NBitcoin;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;

namespace Sample.Blockchain
{
    public class IndexBlockTask
    {
        private readonly Indexer _indexer;
        private readonly DocumentStore _store;
        private readonly LimitedConcurrencyLevelTaskScheduler _uploadScheduler;

        public int IndexedBlocks;
        public int IndexedAttachments;
        public Checkpoint Checkpoint;

        public IndexBlockTask(Indexer indexer)
        {
            this._indexer = indexer;
            this._store = indexer.Store;
            this._uploadScheduler = new LimitedConcurrencyLevelTaskScheduler(10);
        }

        public const int ItemsPerBatch = 10000;

        public Task Run(BlockFetcher blockFetcher)
        {
            this.Checkpoint = blockFetcher.Checkpoint;

            return Task.Factory.StartNew(async () =>
            {
                // PERF: This could be fixed to include the attachment when the client support sending
                //       attachments through bulk inserts.
                var tasks = new List<Tuple<Task, BlockInfo>>();

                var blockList = new List<BlockInfo>(ItemsPerBatch);
                
                BlockInfo lastProcessed = null;
                foreach (var block in blockFetcher)
                {
                    // Is this the first block?
                    if (lastProcessed == null) 
                        lastProcessed = block;

                    if (blockList.Count == ItemsPerBatch)
                    {
                        // Spawn upload attachments task. 

                        var lastBlock = blockList[blockList.Count - 1];
                        var task = Process(blockList, blockFetcher.CancellationToken);

                        lock (tasks)
                        {
                            tasks.Add(new Tuple<Task, BlockInfo>(task, lastBlock));
                        }                        

                        blockList = new List<BlockInfo>(ItemsPerBatch);
                    }

                    blockList.Add(block);
                }

                Task[] allTasks;
                lock (tasks)
                {
                    allTasks = tasks.Select(x => x.Item1).ToArray();                    
                }
                var anyTasks = allTasks;

                _lastSaved = DateTime.UtcNow;

                do
                {
                    int finished = Task.WaitAny(anyTasks, 2000);
                    if (finished != WaitHandle.WaitTimeout)
                    {
                        lock (tasks)
                        {
                            BlockInfo removed = tasks[finished].Item2;
                            if (lastProcessed == null)
                                lastProcessed = removed;

                            tasks[finished] = tasks[tasks.Count - 1];
                            tasks.RemoveAt(tasks.Count - 1);

                            anyTasks = tasks.Select(x => x.Item1).ToArray();

                            if (tasks.All(x => removed.Height > x.Item2.Height))
                                lastProcessed = removed;
                        }
                    }

                    if (NeedSave)
                    {
                        using (var session = _store.OpenAsyncSession())
                        {
                            if (Checkpoint.BlockLocator == null)
                            {
                                await Checkpoint.Prepare(session);
                            }
                            
                            this.Checkpoint = await Checkpoint.CreateOrUpdate(session, Checkpoint.Id, Checkpoint.Network, Checkpoint.BlockLocator);
                        }
                            
                        _lastSaved = DateTime.UtcNow;
                    }
                }
                while (!Task.WaitAll(allTasks, 1));

            }, blockFetcher.CancellationToken);
        }

        public TimeSpan NeedSaveInterval = TimeSpan.FromSeconds(15);
        private DateTime _lastSaved = DateTime.UtcNow;
        public bool NeedSave => (DateTime.UtcNow - _lastSaved) > NeedSaveInterval;

        protected Task Process(List<BlockInfo> blocks, CancellationToken token)
        {
            return Task.Factory.StartNew(async () =>
            {
                // We need to ensure we are disposing before to ensure that the objects do exist when uploading. 
                using (var bulkInsert = _store.BulkInsert())
                {
                    foreach (var block in blocks)
                    {
                        await bulkInsert.StoreAsync(block);

                        Interlocked.Increment(ref IndexedBlocks);
                    }
                }

                await UploadAttachments(blocks, token);
                await ProcessTransactions(blocks, token);

            }, token, TaskCreationOptions.LongRunning, _uploadScheduler);
        }

        protected async Task UploadAttachments(List<BlockInfo> blocks, CancellationToken token)
        {
            using (var session = _store.OpenAsyncSession())
            {
                foreach (var blk in blocks)
                {
                    session.Advanced.StoreAttachment(blk.Id, "BlockHeader", new MemoryStream(blk.Header.ToBytes()));
                }

                await session.SaveChangesAsync(token);

                Interlocked.Add(ref IndexedAttachments, blocks.Count);
            }
        }

        protected async Task ProcessTransactions(List<BlockInfo> blocks, CancellationToken token)
        {
        }
    }
}
