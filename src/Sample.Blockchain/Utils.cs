using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using NBitcoin;

namespace Sample.Blockchain
{
    public static class Utils
    {
        public static ulong ToUnixTime(this DateTimeOffset target)
        {
            var date = target.ToUniversalTime();
            var unixTimestamp = (target - date).TotalSeconds;

            return (ulong)unixTimestamp;
        }

        public static LockType GetLockType(this NBitcoin.Transaction tx)
        {
            if (tx.LockTime.IsHeightLock)
                return LockType.Height;
            if (tx.LockTime.IsTimeLock)
                return LockType.Time;
            return LockType.None;
        }

        public static AddressType GetAddressType(this NBitcoin.TxOut tx)
        {
            var template = tx.ScriptPubKey.FindTemplate();

            if (template != null)
            {
                switch (template.Type)
                {
                    case TxOutType.TX_PUBKEY: return AddressType.PublicKey;
                    case TxOutType.TX_PUBKEYHASH: return AddressType.PublicKeyHash;
                    case TxOutType.TX_MULTISIG: return AddressType.MultiplePublicKeyHashes;
                    case TxOutType.TX_SCRIPTHASH: return AddressType.ScriptHash;
                }
            }

            var scriptAddress = tx.ScriptPubKey.GetScriptAddress(Network.Main);
            if (scriptAddress != null)
            {
                switch (scriptAddress.Type)
                {
                    case Base58Type.SCRIPT_ADDRESS: return AddressType.ScriptHash;
                    case Base58Type.PUBKEY_ADDRESS: return AddressType.PublicKey;
                }
            }
            
            throw new NotSupportedException("Not supported yet");
        }

        public static AddressType GetAddressType(this NBitcoin.TxIn tx)
        {
            var type = tx.ScriptSig.FindTemplate().Type;
            switch (type)
            {
                case TxOutType.TX_PUBKEY: return AddressType.PublicKey;
                case TxOutType.TX_PUBKEYHASH: return AddressType.PublicKeyHash;
                case TxOutType.TX_MULTISIG: return AddressType.MultiplePublicKeyHashes;
                case TxOutType.TX_SCRIPTHASH: return AddressType.ScriptHash;
            }

            throw new NotSupportedException("Not supported yet");
        }

    }
}
