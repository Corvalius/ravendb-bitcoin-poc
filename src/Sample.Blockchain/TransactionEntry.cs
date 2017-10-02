using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using NBitcoin;
using System.Threading.Tasks;
using NBitcoin.OpenAsset;
using Newtonsoft.Json;
using Raven.Client.Documents.Session;

namespace Sample.Blockchain
{
    [Flags]
    public enum TransactionType : byte
    {
        Standard = 0,
        Confirmed = 1,
        Colored = 2,        
    }

    public class TransactionEntry
    {
        public const string Prefix = "tx:";

        public string Id { get; set; }
        public string BlockId { get; set; }
        public TransactionType Type { get; set; }

        public long Time; // This is the indexable datetime in UTC units. 
        public long UnixTime;
        public DateTimeOffset HumanTime;

        [JsonIgnore]
        public Transaction Transaction { get; private set; }

        [JsonIgnore]
        public ColoredTransaction ColoredTransaction { get; private set; }

        [JsonIgnore]
        public bool IsColored => (Type & TransactionType.Colored) == TransactionType.Colored;
        [JsonIgnore]
        public bool IsConfirmed => (Type & TransactionType.Confirmed) == TransactionType.Confirmed;

        public TransactionEntry(Transaction tx, Block block)
        {
            this.Id = TransactionEntry.ToId(tx.GetHash());
          
            Update(tx, block);
        }

        public static string ToId(string hash)
        {
            if (hash.StartsWith(Prefix))
                return hash;

            return $"{Prefix}{hash}";
        }

        public static string ToId(uint256 hash)
        {
            return $"{Prefix}{hash}";
        }

        public async Task Prepare(IAsyncDocumentSession session)
        {
            var transactionAttachment = await session.Advanced.GetAttachmentAsync(this, "Transaction");

            var tx = new Transaction();
            tx.ReadWrite(new BitcoinStream(transactionAttachment.Stream, false));
            this.Transaction = tx;

            if (this.IsColored)
            {
                transactionAttachment = await session.Advanced.GetAttachmentAsync(this, "ColoredTransaction");

                var ctx = new ColoredTransaction();
                ctx.ReadWrite(new BitcoinStream(transactionAttachment.Stream, false));
                this.ColoredTransaction = ctx;
            }
        }

        public static async ValueTask<TransactionEntry> CreateOrUpdate(IAsyncDocumentSession session, Transaction tx, Block block)
        {
            var txEntry = await session.LoadAsync<TransactionEntry>(TransactionEntry.ToId(tx.GetHash()));
            if (txEntry == null)
            {
                txEntry = new TransactionEntry(tx, block);
                await session.StoreAsync(txEntry);
            }
            else
            {
                if (txEntry.Update(tx, block) == false)
                    return txEntry; // Nothing to be done this is one of the 2 violations to the protocol.
            }

            session.Advanced.StoreAttachment(txEntry, "Transaction", new MemoryStream(txEntry.Transaction.ToBytes()));

            return txEntry;
        }

        private bool Update(Transaction tx, Block block)
        {
            if (block != null)
            {
                Type |= TransactionType.Confirmed;

                var blockId = BlockEntry.ToId(block.GetHash());
                if (this.BlockId != null && !this.BlockId.Equals(blockId))
                    return false; // There are 2 blocks in the blockchain that violate this property.

                this.BlockId = blockId;
            }

            this.HumanTime = block.Header.BlockTime;
            this.Time = this.HumanTime.UtcTicks;
            this.UnixTime = HumanTime.ToUnixTimeSeconds();

            this.Transaction = tx;
            return true;
        }
    }
}
