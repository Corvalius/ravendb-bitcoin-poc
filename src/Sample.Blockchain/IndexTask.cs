using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Runtime.ExceptionServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NBitcoin;
using Raven.Client.Documents;

namespace Sample.Blockchain
{
    public interface IIndexTask
    {
        Task Index(BlockFetcher blockFetcher, TaskScheduler scheduler);

        bool SaveProgression
        {
            get;
            set;
        }

        bool EnsureIsSetup
        {
            get;
            set;
        }
    }

    public abstract class IndexTask<TIndexed> : IIndexTask
    {
        protected Indexer Indexer { get; private set; }
        protected DocumentStore Store { get; private set; }

        /// <summary>
        /// Fast forward indexing to the end (if scanning not useful)
        /// </summary>
        protected virtual bool SkipToEnd => false;

        public async Task Index(BlockFetcher blockFetcher, TaskScheduler scheduler)
        {
            ConcurrentDictionary<Task, Task> tasks = new ConcurrentDictionary<Task, Task>();
            try
            {
                if (EnsureIsSetup)
                    await EnsureSetup();

                if (!SkipToEnd)
                {
                    try
                    {
                        BeginProcess();
                        foreach (var block in blockFetcher)
                        {
                            ThrowIfException();
                            if (NeedSave && SaveProgression)
                            {
                                // We will save anyway even if the current batch is not full. 
                                EnqueueTasks(tasks, scheduler);

                                await Save(tasks, blockFetcher); 
                            }

                            // Process the block and check if the batch is done. 
                            if (ProcessBlock(block))
                            {
                                EnqueueTasks(tasks, scheduler);
                            }
                        }

                        EnqueueTasks(tasks, scheduler);
                    }
                    catch (OperationCanceledException ex)
                    {
                        if (ex.CancellationToken != blockFetcher.CancellationToken)
                            throw;
                    }
                }
                else
                {
                    blockFetcher.SkipToEnd();
                }
                    
                if (SaveProgression)
                    await Save(tasks, blockFetcher);

                WaitFinished(tasks);
                ThrowIfException();
            }
            catch (AggregateException aex)
            {
                ExceptionDispatchInfo.Capture(aex.InnerException).Throw();
                throw;
            }
        }

        public bool EnsureIsSetup { get; set; } = true;


        private readonly ExponentialBackoff _retry = new ExponentialBackoff(15, TimeSpan.FromMilliseconds(100),
                                                                                TimeSpan.FromSeconds(10),
                                                                                TimeSpan.FromMilliseconds(200));

        private void EnqueueTasks(ConcurrentDictionary<Task, Task> tasks, TaskScheduler scheduler)
        {
            var items = EndProcess();

            var task = _retry.Do(() => IndexCore(items), scheduler);

            tasks.TryAdd(task, task);

            task.ContinueWith(prev =>
            {
                _exception = prev.Exception ?? _exception;
                tasks.TryRemove(prev, out prev);
            });

            if (tasks.Count > MaxQueued)
            {
                WaitFinished(tasks, MaxQueued / 2);
            }

            BeginProcess();
        }

        public int MaxQueued
        {
            get;
            set;
        }

        private Exception _exception;

        private async Task Save(ConcurrentDictionary<Task, Task> tasks, BlockFetcher fetcher)
        {
            WaitFinished(tasks);
            ThrowIfException();            

            var locator = new BlockLocator();
            locator.Blocks.Add(fetcher.LastProcessed.GetHash());

            using (var session = this.Store.OpenAsyncSession())
            {
                var checkpoint = await Checkpoint.CreateOrUpdate(session, fetcher.Checkpoint, locator);
                await session.SaveChangesAsync();

                fetcher.Checkpoint = checkpoint;
            }

            _lastSaved = DateTime.UtcNow;
        }

        int[] wait = new int[] { 100, 200, 400, 800, 1600 };

        private void WaitFinished(ConcurrentDictionary<Task, Task> tasks, int queuedTarget = 0)
        {
            while (tasks.Count > queuedTarget)
            {
                Thread.Sleep(100);
            }
        }

        private void ThrowIfException()
        {
            if (_exception != null)
                ExceptionDispatchInfo.Capture(_exception).Throw();
        }

        private DateTime _lastSaved = DateTime.UtcNow;

        public TimeSpan NeedSaveInterval = TimeSpan.FromSeconds(15);
        public bool NeedSave => (DateTime.UtcNow - _lastSaved) > NeedSaveInterval;


        public bool SaveProgression
        {
            get;
            set;
        }

        protected abstract Task EnsureSetup();

        protected abstract void BeginProcess();
        protected abstract bool ProcessBlock(BlockInfo block);
        protected abstract List<TIndexed> EndProcess();

        protected abstract void IndexCore(List<TIndexed> items);

        protected IndexTask(Indexer indexer)
        {
            this.Indexer = indexer ?? throw new ArgumentNullException("configuration");
            this.Store = this.Indexer.Store;

            SaveProgression = true;
            MaxQueued = 100;
        }
    }

    /// <summary>
    /// A retry strategy with back-off parameters for calculating the exponential delay between retries.
    /// </summary>
    internal class ExponentialBackoff
    {
        private readonly int retryCount;
        private readonly TimeSpan minBackoff;
        private readonly TimeSpan maxBackoff;
        private readonly TimeSpan deltaBackoff;


        public ExponentialBackoff(int retryCount, TimeSpan minBackoff, TimeSpan maxBackoff, TimeSpan deltaBackoff)
        {
            this.retryCount = retryCount;
            this.minBackoff = minBackoff;
            this.maxBackoff = maxBackoff;
            this.deltaBackoff = deltaBackoff;
        }

        public async Task Do(Action act, TaskScheduler scheduler = null)
        {
            int retryCount = -1;

            while (true)
            {
                Exception lastException = null;
                try
                {
                    var task = new Task(act);
                    task.Start(scheduler);
                    await task.ConfigureAwait(false);
                    break;
                }
                catch (OutOfMemoryException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    lastException = ex;
                }
                retryCount++;

                if (!GetShouldRetry(retryCount, lastException, out var wait))
                {
                    ExceptionDispatchInfo.Capture(lastException).Throw();
                }
                else
                {
                    await Task.Delay(wait).ConfigureAwait(false);
                }
            }
        }

        internal bool GetShouldRetry(int currentRetryCount, Exception lastException, out TimeSpan retryInterval)
        {
            if (currentRetryCount < this.retryCount)
            {
                var random = new Random();

                var delta = (int)((Math.Pow(2.0, currentRetryCount) - 1.0) * random.Next((int)(this.deltaBackoff.TotalMilliseconds * 0.8), (int)(this.deltaBackoff.TotalMilliseconds * 1.2)));
                var interval = (int)Math.Min(checked(this.minBackoff.TotalMilliseconds + delta), this.maxBackoff.TotalMilliseconds);
                retryInterval = TimeSpan.FromMilliseconds(interval);

                return true;
            }

            retryInterval = TimeSpan.Zero;
            return false;
        }
    }
}
