function (doc) {

    var time = new Date(doc.close_time_timestamp);

    for (var t = 0, txs = doc.transactions.length; t < txs; t++) {
        var tx = doc.transactions[t],
            meta = tx.metaData,
            affNodes = meta.AffectedNodes;

        for (var n = 0, num_nodes = affNodes.length; n < num_nodes; n++) {
            if (affNodes[n].hasOwnProperty("ModifiedNode")
                && affNodes[n].ModifiedNode.LedgerEntryType === "Offer") {

                var mnode = affNodes[n].ModifiedNode;

                if (mnode.PreviousFields.hasOwnProperty("TakerPays")
                    && mnode.PreviousFields.hasOwnProperty("TakerGets")) {
                    
                    var pay_curr, pay_amnt;
                    if (typeof mnode.PreviousFields.TakerPays === "object") {
                        pay_curr = [mnode.PreviousFields.TakerPays.currency, mnode.PreviousFields.TakerPays.issuer];
                        pay_amnt = mnode.FinalFields.TakerPays.value - mnode.PreviousFields.TakerPays.value;
                    } else {
                        pay_curr = ["XRP"];
                        pay_amnt = mnode.FinalFields.TakerPays - mnode.PreviousFields.TakerPays;
                    }

                    var get_curr, get_amnt;
                    if (typeof mnode.PreviousFields.TakerGets === "object") {
                        get_curr = [mnode.PreviousFields.TakerGets.currency, mnode.PreviousFields.TakerGets.issuer];
                        get_amnt = mnode.FinalFields.TakerGets.value - mnode.PreviousFields.TakerGets.value;
                    } else {
                        get_curr = ["XRP"];
                        get_amnt = mnode.FinalFields.TakerGets - mnode.PreviousFields.TakerGets;
                    }

                    emit([pay_curr, get_curr], [pay_amnt, get_amnt]);
                    emit([get_curr, pay_curr], [get_amnt, pay_amnt]);

                }
            
            } else if (affNodes[n].hasOwnProperty("DeletedNode")
                        && affNodes[n].DeletedNode.LedgerEntryType === "Offer") {
                var dnode = affNodes[n].DeletedNode;

                // difference between previous and final or just final
            }
        }

    }
}