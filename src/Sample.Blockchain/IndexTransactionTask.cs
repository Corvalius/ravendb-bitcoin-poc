using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NBitcoin;

namespace Sample.Blockchain
{
    public class IndexTransactionTask : IndexTask<TransactionEntry>
    {
        public int ScheduledTransactions;
        public int IndexedTransactions;

        
        private List<TransactionEntry> _transactionsToProcess;

        public IndexTransactionTask(Indexer indexer) : base(indexer)
        {
        }

        protected async override Task EnsureSetup()
        {
        }

        protected override void BeginProcess()
        {
            Debug.Assert(_transactionsToProcess == null);
            _transactionsToProcess = new List<TransactionEntry>();
        }

        protected override bool ProcessBlock(BlockInfo item)
        {
            var block = item.Block;
            foreach (var tx in block.Transactions)
                _transactionsToProcess.Add(new TransactionEntry(tx, block));

            Interlocked.Add(ref ScheduledTransactions, block.Transactions.Count);

            return _transactionsToProcess.Count >= 1000;
        }

        protected override List<TransactionEntry> EndProcess()
        {
            var list = _transactionsToProcess;
            _transactionsToProcess = null;

            return list;
        }

        protected override void IndexCore(List<TransactionEntry> items)
        {
            using (var bulkInsert = Store.BulkInsert())
            {
                foreach (var item in items)
                {
                    bulkInsert.Store(item);
                }
            }

            using (var session = Store.OpenSession())
            {
                foreach (var item in items)
                {
                    try
                    {
                        var tx = item.Transaction;
                        session.Advanced.StoreAttachment(TransactionEntry.ToId(tx.GetHash()), "Transaction", new MemoryStream(tx.ToBytes()));
                    }
                    catch
                    { 
                        // It can happen because the blockchain have 2 transactions that violate that the tx.GetHash() is unique. 
                    }
                }

                session.SaveChanges();
            }

            Interlocked.Add(ref IndexedTransactions, items.Count);
        }
    }
}
