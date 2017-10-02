using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NBitcoin;
using Newtonsoft.Json;
using Raven.Client.Documents.Session;

namespace Sample.Blockchain
{
    public class BlockEntry
    {
        public const string Prefix = "blk:";

        public string Id { get; set; }

        public int Height;

        public string PreviousBlock;

        public long Time; // This is the indexable datetime in UTC units. 
        public long UnixTime;
        public DateTimeOffset HumanTime;

        [JsonIgnore]
        public BlockHeader Header { get; private set; }

        protected BlockEntry()
        { }

        public BlockEntry(ChainedBlock block)
        {
            this.Id = BlockEntry.ToId(block.Header.GetHash());

            Update(block);
        }

        public BlockEntry(NBitcoin.Block block, int height)
        {
            this.Id = BlockEntry.ToId(block.Header.GetHash());

            this.PreviousBlock = BlockEntry.ToId(block.Header.HashPrevBlock);

            this.HumanTime = block.Header.BlockTime;
            this.Time = this.HumanTime.UtcTicks;
            this.UnixTime = HumanTime.ToUnixTimeSeconds();

            this.Header = block.Header;
            this.Height = height;
        }

        private void Update(ChainedBlock block)
        {
            this.PreviousBlock = BlockEntry.ToId(block.Header.HashPrevBlock);

            this.HumanTime = block.Header.BlockTime;
            this.Time = this.HumanTime.UtcTicks;
            this.UnixTime = HumanTime.ToUnixTimeSeconds();

            this.Header = block.Header;
            this.Height = block.Height;
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
            var attachment = await session.Advanced.GetAttachmentAsync(this, "BlockHeader");

            var header = new BlockHeader();
            header.ReadWrite(new BitcoinStream(attachment.Stream, false));
            this.Header = header;            
        }

        public static async ValueTask<BlockEntry> CreateOrUpdate(IAsyncDocumentSession session, ChainedBlock chainBlock)
        {
            var blk = await session.LoadAsync<BlockEntry>(BlockEntry.ToId(chainBlock.HashBlock));
            if (blk == null)
            {
                blk = new BlockEntry(chainBlock);
                await session.StoreAsync(blk);
            }
            else
            {
                blk.Update(chainBlock);
            }

            session.Advanced.StoreAttachment(blk, "BlockHeader", new MemoryStream(blk.Header.ToBytes()));

            return blk;
        }
    }
}
