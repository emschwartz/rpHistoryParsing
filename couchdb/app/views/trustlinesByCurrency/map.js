function(doc) {
    for (var t = 0, txs = doc.transactions.length; t < txs; t++) {
        var tx = doc.transactions[t];
        for (var n = 0, nodes = tx.metaData.AffectedNodes.length; n < nodes; n++) {

            if (tx.metaData.AffectedNodes[n].hasOwnProperty("CreatedNode") && tx.metaData.AffectedNodes[n].CreatedNode.LedgerEntryType === "RippleState") {
                var cnode = tx.metaData.AffectedNodes[n].CreatedNode;

                var currency = cnode.NewFields.Balance.currency,
                    high_party = cnode.NewFields.HighLimit.issuer,
                    low_party = cnode.NewFields.LowLimit.issuer;

                if (parseFloat(cnode.NewFields.LowLimit.value) > 0) {
                    emit([currency, high_party], {"incoming": 1});
                    emit([currency, low_party], {"outgoing": 1});
                }

                if (parseFloat(cnode.NewFields.HighLimit.value) > 0) {
                    emit([currency, low_party], {"incoming": 1});
                    emit([currency, high_party], {"outgoing": 1});
                }

                if (parseFloat(cnode.NewFields.Balance.value) !== 0) {
                    emit([currency, high_party], {"balance_change": 0 - parseFloat(cnode.NewFields.Balance.value)});
                    emit([currency, low_party], {"balance_change": parseFloat(cnode.NewFields.Balance.value)});
                }

            } else if (tx.metaData.AffectedNodes[n].hasOwnProperty("ModifiedNode") && tx.metaData.AffectedNodes[n].ModifiedNode.LedgerEntryType === "RippleState") {
                var mnode = tx.metaData.AffectedNodes[n].ModifiedNode;

                // high limit changed
                if (mnode.PreviousFields.hasOwnProperty("HighLimit")) {

                    // adding trust line
                    if (parseFloat(mnode.PreviousFields.HighLimit.value) === 0 && parseFloat(mnode.FinalFields.HighLimit.value) > 0) {

                        var currency = mnode.FinalFields.LowLimit.currency,
                            trusting_party = mnode.FinalFields.HighLimit.issuer,
                            trusted_party = mnode.FinalFields.LowLimit.issuer;

                        emit([currency, trusted_party], {"incoming": 1});
                        emit([currency, trusting_party], {"outgoing": 1});
                    }

                    // removing trust line
                    if (parseFloat(mnode.PreviousFields.HighLimit.value) > 0 && parseFloat(mnode.FinalFields.HighLimit.value) === 0) {

                        var currency = mnode.FinalFields.LowLimit.currency,
                            trusting_party = mnode.FinalFields.HighLimit.issuer,
                            trusted_party = mnode.FinalFields.LowLimit.issuer;

                        emit([currency, trusted_party], {"incoming": -1});
                        emit([currency, trusting_party], {"outgoing": -1});
                    }

                }


                // low limit changed
                if (mnode.PreviousFields.hasOwnProperty("LowLimit")) {

                    // adding trust line
                    if (parseFloat(mnode.PreviousFields.LowLimit.value) === 0 && parseFloat(mnode.FinalFields.LowLimit.value) > 0) {

                        var currency = mnode.FinalFields.HighLimit.currency,
                            trusting_party = mnode.FinalFields.LowLimit.issuer,
                            trusted_party = mnode.FinalFields.HighLimit.issuer;

                        emit([currency, trusted_party], {"incoming": 1});
                        emit([currency, trusting_party], {"outgoing": 1});
                    }

                    // removing trust line
                    if (parseFloat(mnode.PreviousFields.LowLimit.value) > 0 && parseFloat(mnode.FinalFields.LowLimit.value) === 0) {

                        var currency = mnode.FinalFields.HighLimit.currency,
                            trusting_party = mnode.FinalFields.LowLimit.issuer,
                            trusted_party = mnode.FinalFields.HighLimit.issuer;

                        emit([currency, trusted_party], {"incoming": -1});
                        emit([currency, trusting_party], {"outgoing": -1});
                    }

                }

                // balance changed
                if (mnode.PreviousFields.hasOwnProperty("Balance")) {

                    var currency = mnode.FinalFields.Balance.currency,
                        low_party = mnode.FinalFields.LowLimit.issuer,
                        high_party = mnode.FinalFields.HighLimit.issuer;

                    emit([currency, low_party], {"balance_change": (mnode.FinalFields.Balance.value - mnode.PreviousFields.Balance.value)});
                    emit([currency, high_party], {"balance_change": (0 - (mnode.FinalFields.Balance.value - mnode.PreviousFields.Balance.value))});

                }
            }
        }
    }
}