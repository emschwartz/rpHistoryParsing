/* map product:
key: <account>
value: {
    "trusted_by_others": {
        <currency>: [
                {
                    "other_acct": <trusting party>,
                    "balance": <trustline balance>
                }
            ]
    },
    "trusting_others": {
        <currency>: [
            {
                "other_acct": <trusted party>,
                "balance": <trustline balance>
            }
        ]
    }
}

*/

function (doc) {
    for (var t = 0, txs = doc.transactions.length; t < txs; t++) {
        var tx = doc.transactions[t];
        for (var n = 0, nodes = tx.metaData.AffectedNodes.length; n < nodes; n++) {

            if (tx.metaData.AffectedNodes[n].hasOwnProperty("CreatedNode")
                && tx.metaData.AffectedNodes[n].CreatedNode.LedgerEntryType === "RippleState") {

                var cnode = tx.metaData.AffectedNodes[n].CreatedNode;



                

            } else if (tx.metaData.AffectedNodes[n].hasOwnProperty("ModifiedNode")
                && tx.metaData.AffectedNodes[n].ModifiedNode.LedgerEntryType === "RippleState") {

                var mnode = tx.metaData.AffectedNodes[n].ModifiedNode;

                


            }

        }
    }
}