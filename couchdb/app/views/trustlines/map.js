function (doc) {
    for (var t = 0, txs = doc.transactions.length; t < txs; t++) {
        var tx = doc.transactions[t];

        for (var n = 0, nodes = tx.metaData.AffectedNodes.length; n < nodes; n++) {

            if (tx.metaData.AffectedNodes[n].hasOwnProperty("CreatedNode")
                && tx.metaData.AffectedNodes[n].CreatedNode.LedgerEntryType === "RippleState") {

                var cnode = tx.metaData.AffectedNodes[n].CreatedNode;

                // high limit is issuer
                if (parseFloat(cnode.NewFields.LowLimit.value) > 0) {
                    emit([cnode.NewFields.HighLimit.issuer, cnode.NewFields.HighLimit.currency], 1);
                }

                // low limit is issuer
                if (parseFloat(cnode.NewFields.HighLimit.value) > 0) {
                    emit([cnode.NewFields.LowLimit.issuer, cnode.NewFields.LowLimit.currency], 1);
                }

            } else if (tx.metaData.AffectedNodes[n].hasOwnProperty("ModifiedNode")
                && tx.metaData.AffectedNodes[n].ModifiedNode.LedgerEntryType === "RippleState") {

                var mnode = tx.metaData.AffectedNodes[n].ModifiedNode;

                // adding new trust line

                // low limit is issuer
                if (mnode.PreviousFields.hasOwnProperty("HighLimit")
                    && parseFloat(mnode.PreviousFields.HighLimit.value) === 0
                    && parseFloat(mnode.FinalFields.HighLimit.value > 0)) {

                    emit([mnode.FinalFields.LowLimit.issuer, mnode.FinalFields.LowLimit.currency], 1);
                }

                // high limit is issuer
                if (mnode.PreviousFields.hasOwnProperty("LowLimit")
                    && parseFloat(mnode.PreviousFields.LowLimit.value) === 0
                    && parseFloat(mnode.FinalFields.LowLimit.value > 0)) {

                    emit([mnode.FinalFields.HighLimit.issuer, mnode.FinalFields.HighLimit.currency], 1);
                }

                // removing trust line

                // low limit is issuer
                if (mnode.PreviousFields.hasOwnProperty("HighLimit")
                    && parseFloat(mnode.PreviousFields.HighLimit.value) > 0
                    && parseFloat(mnode.FinalFields.HighLimit.value === 0)) {

                    emit([mnode.FinalFields.LowLimit.issuer, mnode.FinalFields.LowLimit.currency], -1);
                }

                // high limit is issuer
                if (mnode.PreviousFields.hasOwnProperty("LowLimit")
                    && parseFloat(mnode.PreviousFields.LowLimit.value) > 0
                    && parseFloat(mnode.FinalFields.LowLimit.value === 0)) {

                    emit([mnode.FinalFields.HighLimit.issuer, mnode.FinalFields.HighLimit.currency], -1);
                }

            }

        }
    }
}