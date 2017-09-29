﻿using System;
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
    public class Block
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

        protected Block()
        { }

        public Block(ChainedBlock block)
        {
            this.Id = Block.ToId(block.Header.GetHash());

            Update(block);
        }

        public Block(NBitcoin.Block block, int height)
        {
            this.Id = Block.ToId(block.Header.GetHash());

            this.PreviousBlock = Block.ToId(block.Header.HashPrevBlock);

            this.HumanTime = block.Header.BlockTime;
            this.Time = this.HumanTime.UtcTicks;
            this.UnixTime = HumanTime.ToUnixTimeSeconds();

            this.Header = block.Header;
            this.Height = height;
        }

        private void Update(ChainedBlock block)
        {
            this.PreviousBlock = Block.ToId(block.Header.HashPrevBlock);

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

        public static async ValueTask<Block> CreateOrUpdate(IAsyncDocumentSession session, ChainedBlock chainBlock)
        {
            var blk = await session.LoadAsync<Block>(Block.ToId(chainBlock.HashBlock));
            if (blk == null)
            {
                blk = new Block(chainBlock);
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