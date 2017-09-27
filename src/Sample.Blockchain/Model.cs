using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NBitcoin;

namespace Sample.Blockchain
{
    public class Block
    {
        public const string Prefix = "blk:";

        public string Id { get; set; }

        public int Size;
        public int Version;
        public uint Nonce;
        public double Dificulty;
        public int? Height;

		public long Time; // This is the indexable datetime in UTC units. 
        public long UnixTime;
		public DateTimeOffset HumanTime;

        public decimal Bits;
		public string MerkleRoot;
	    public string PreviousBlock;

        public decimal OutputBTC;
        public decimal RewardBTC;
        public decimal FeesBTC;

        public int Transactions;
        public List<string> TransactionList;

        protected Block()
        { }

        public Block(NBitcoin.Block block)
        {
            this.Id = Block.ToId(block.GetHash());

            this.Dificulty = block.Header.Bits.Difficulty;
            this.MerkleRoot = block.Header.HashMerkleRoot.ToString();
            this.Version = block.Header.Version;
	        this.Nonce = block.Header.Nonce;
            
            this.PreviousBlock = Block.ToId(block.Header.HashPrevBlock);

            this.HumanTime = block.Header.BlockTime;
            this.Time = this.HumanTime.UtcTicks;
            this.UnixTime = HumanTime.ToUnixTimeSeconds();

            this.TransactionList = block.Transactions.Select(x => Transaction.ToId(x.GetHash())).ToList();
            this.Transactions = block.Transactions.Count;
        }


        public static string ToId(string hash)
        {
            if (hash.StartsWith(Prefix))
                return hash;

            return $"{Prefix}{hash}";
        }

        public static string ToId(uint256 hash)
        {
            return $"{Prefix}{hash.ToString()}";
        }
    }

    public enum AddressType
    {
        None = 0,
        PublicKey = 1,
        PublicKeyHash = 2,
        MultiplePublicKeyHashes = 3,
        Script = 4,
        ScriptHash = 5,
        TestNet = 6,
        PrivateKeyWIF = 7,
        EncryptedPrivateKey = 8,
        ExtendedPublicKey = 9,
    }

    public class Outpoint
    {
        public string TxId;
        public uint Index;
    }

    public class InTransaction
    {
        public uint Index;
        public AddressType PayerType;
        public string Payer;
        public decimal Value;

        public Outpoint Outpoint;                
    }

    public class OutTransaction
    {
        public uint Index;
        public AddressType PayeeType;
        public string Payee;
        public decimal Value;
    }

    public enum LockType
    {
        None,
        Time,
        Height
    }

    public class Transaction
    {
        public const string Prefix = "txn:";

        /// <summary>
        /// This is the RavenDB transaction identifier.
        /// </summary>
        public string Id;

        /// <summary>
        /// This is the RavenDB block identifier.
        /// </summary>
        public string BlockId;

        public int Size;
        public uint Version;
        
        public LockType LockType;
        public uint LockTime;
        
        public bool IsCoinBase;
        public bool HasWitness;

        public int InputCount;
        public InTransaction[] InputTransactions;
        public decimal InputBtc;

        public int OutputCount;
        public OutTransaction[] OutputTransactions;
        public decimal OutputBtc;

        public decimal FeeBtc;
        
        protected Transaction()
        { }

        public Transaction(NBitcoin.Block parent, NBitcoin.Transaction tx)
        {
            this.Id = Transaction.ToId(tx.GetHash());
            this.BlockId = Block.ToId(parent.GetHash());
            this.Size = tx.GetVirtualSize();
            this.Version = tx.Version;

            this.LockType = tx.GetLockType();
            this.LockTime = tx.LockTime.Value;

            this.IsCoinBase = tx.IsCoinBase;
            this.HasWitness = tx.HasWitness;

            this.InputCount = tx.Inputs.Count;
            this.InputTransactions = new InTransaction[InputCount];

            this.OutputCount = tx.Outputs.Count;
            this.OutputTransactions = new OutTransaction[OutputCount];
            this.OutputBtc = tx.TotalOut.ToDecimal(MoneyUnit.BTC);

            for (int i = 0; i < InputCount; i++)
            {
                var inTx = tx.Inputs[i];

                // Is this a Coinbase?
                string payer = null;                
                if (inTx.PrevOut.IsNull)
                {
                    // Yes.                    
                    payer = inTx.PrevOut.Hash.ToString();
                }

                InputTransactions[i] = new InTransaction
                {
                    Index = (uint)i,
                    Outpoint = new Outpoint { Index = inTx.PrevOut.N, TxId = Transaction.ToId(inTx.PrevOut.Hash.ToString()) },
                    Payer = payer,
                    PayerType = AddressType.None,
                    Value = 0
                };
            }

            for (int i = 0; i < OutputCount; i++)
            {
                var outTx = tx.Outputs[i];

                AddressType type = outTx.GetAddressType();

                string payee;
                if (type == AddressType.PublicKey)
                {
                    payee = outTx.ScriptPubKey.GetDestinationPublicKeys().FirstOrDefault()?.ToString();
                }
                else if (type == AddressType.ScriptHash)
                {
                    payee = outTx.ScriptPubKey.GetScriptAddress(Network.Main).Hash.ToString();
                }
                else if (type == AddressType.MultiplePublicKeyHashes)
                {
                    payee = outTx.ScriptPubKey.GetDestinationPublicKeys()[0].GetAddress(Network.Main).ToString();
                }
                else
                {
                    payee = outTx.ScriptPubKey?.GetDestinationAddress(Network.Main)?.ToString();
                }

                OutputTransactions[i] = new OutTransaction
                {
                    Index = (uint)i,                    
                    Payee = payee,
                    PayeeType = type,
                    Value = outTx.Value.ToDecimal(MoneyUnit.BTC)
                };
            }
        }


        public static string ToId(string hash)
        {
            if (hash.StartsWith(Prefix))
                return hash;

            return $"{Prefix}{hash}";
        }

        public static string ToId(uint256 hash)
        {
            return $"{Prefix}{hash.ToString()}";
        }
    }


}
