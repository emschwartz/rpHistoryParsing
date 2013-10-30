function(doc) {
    var time = new Date(doc.close_time_timestamp),
        timestamp = [time.getUTCFullYear(), time.getUTCMonth(), time.getUTCDate(), 
                     time.getUTCHours(), time.getUTCMinutes(), time.getUTCSeconds()];

    for (var t = 0, txs = doc.transactions.length; t < txs; t++) {
        var tx = doc.transactions[t];

        if (tx.metaData.TransactionResult !== "tesSUCCESS") 
                continue;

        for (var n = 0, nodes = tx.metaData.AffectedNodes.length; n < nodes; n++) {

            if (tx.metaData.AffectedNodes[n].hasOwnProperty("CreatedNode")) {
                var cnode = tx.metaData.AffectedNodes[n].CreatedNode;
                if (cnode.LedgerEntryType === "Offer") {

                }
            } else if () {
                var mnode = tx.metaData.AffectedNodes[n].ModifiedNode;
                if (mnode.LedgerEntryType === "Offer") {
                    
                }
            } else if () {
                var dnode = tx.metaData.AffectedNodes[n].DeletedNode;
                if (dnode.LedgerEntryType === "Offer") {
                    
                }
            }
        }
    }
}