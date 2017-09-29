using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using NBitcoin;
using Newtonsoft.Json;
using Raven.Client.Documents.Session;

namespace Sample.Blockchain
{
    public class Checkpoint
    {
        public const string Prefix = "/Checkpoint/";

        public string Id;

        public readonly string Name;
        public readonly string BlockId;
        public readonly string GenesisId;

        [JsonIgnore]
        public readonly Network Network;

        [JsonIgnore]
        private byte[] _attachmentBytes;

        [JsonIgnore]
        public byte[] AttachmentBytes => _attachmentBytes ?? (_attachmentBytes = BlockLocator.ToBytes());

        [JsonIgnore]
        private BlockLocator _blockLocator;

        [JsonIgnore]
        public BlockLocator BlockLocator
        {
            get => _blockLocator;
            set
            {
                this._blockLocator = value;
                this._attachmentBytes = null;                
            }
        }

        public Checkpoint (string name, Network network, BlockLocator locator = null)
        {
            if (locator == null)
            {
                locator = new BlockLocator();
                locator.Blocks.Add( network.GetGenesis().Header.GetHash() );
            }

            Id = ToId(name, network.Name);
            Name = name;
            Network = network;
            BlockId = BlockInfo.ToId(locator.Blocks[0]);
            GenesisId = BlockInfo.ToId(locator.Blocks[locator.Blocks.Count - 1]);
            BlockLocator = locator;            
        }

        public async Task Prepare(IAsyncDocumentSession session)
        {
            var attachment = await session.Advanced.GetAttachmentAsync(this, "BlockBytes");
            var locator = new BlockLocator();
            locator.ReadWrite(new BitcoinStream(attachment.Stream, false));
            this.BlockLocator = locator;

            Debug.Assert(BlockInfo.ToId(this.BlockLocator.Blocks[0]) == this.BlockId);
            Debug.Assert(BlockInfo.ToId(this.BlockLocator.Blocks[BlockLocator.Blocks.Count - 1]) == this.GenesisId);
        }

        public static string ToId(string name, string network = "Main")
        {
            var prefix = $"{network}{Prefix}";
            if (name.StartsWith(prefix))
                return name;

            return $"{prefix}{name}";
        }


        public static string ToPrefix(string network = "Main")
        {
            return $"{network}{Prefix}";
        }

        public static ValueTask<Checkpoint> CreateOrUpdate(IAsyncDocumentSession session, string name, Network network, ChainedBlock block)
        {
            return CreateOrUpdate(session, name, network, block.GetLocator());
        }

        public static async ValueTask<Checkpoint> CreateOrUpdate(IAsyncDocumentSession session, string name, Network network, BlockLocator locator)
        {
            var checkpoint = await session.LoadAsync<Checkpoint>(Checkpoint.ToId(name, network.Name));
            if (checkpoint == null)
            {                
                checkpoint = new Checkpoint(name, network, locator);                
                await session.StoreAsync(checkpoint);
            }

            session.Advanced.StoreAttachment(checkpoint, "BlockBytes", new MemoryStream(checkpoint.AttachmentBytes));

            return checkpoint; 
        }

        public static async ValueTask<Checkpoint> Load(IAsyncDocumentSession session, string name, Network network)
        {
            var checkpoint = await session.LoadAsync<Checkpoint>(Checkpoint.ToId(name, network.Name));
            if (checkpoint == null)
                throw new ArgumentException($"Checkpoint with id {ToId(name, network.Name)} does not exist");

            var attachment = await session.Advanced.GetAttachmentAsync(checkpoint, "BlockBytes");
            var locator = new BlockLocator();
            locator.ReadWrite(new BitcoinStream(attachment.Stream, false));
            checkpoint.BlockLocator = locator;

            return checkpoint;
        }
        
    }
}
