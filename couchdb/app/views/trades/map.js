function (doc) {
    for (var t = 0, txs = doc.transactions.length; t < txs; t++) {
        var tx = doc.transactions[t],
            meta = tx.metaData,
            affNodes = meta.AffectedNodes;

        for (var n = 0, num_nodes = affNodes.length; n < num_nodes; n++) {
            if (affNodes[n].hasOwnProperty("ModifiedNode") {
                var mnode = affNodes[n].ModifiedNode;
            
            } else if (affNodes[n].hasOwnProperty("DeletedNode") {
                var dnode = affNodes[n].DeletedNode;

            }
        }

    }
}