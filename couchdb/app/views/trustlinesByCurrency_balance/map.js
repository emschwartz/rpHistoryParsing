function(doc) {
    for (var t = 0, txs = doc.transactions.length; t < txs; t++) {
        var tx = doc.transactions[t];
        for (var n = 0, nodes = tx.metaData.AffectedNodes.length; n < nodes; n++) {

            if (tx.metaData.AffectedNodes[n].hasOwnProperty("CreatedNode") && tx.metaData.AffectedNodes[n].CreatedNode.LedgerEntryType === "RippleState") {
                var cnode = tx.metaData.AffectedNodes[n].CreatedNode;

                var currency = cnode.NewFields.Balance.currency,
                    high_party = cnode.NewFields.HighLimit.issuer,
                    low_party = cnode.NewFields.LowLimit.issuer;

                // if (parseFloat(cnode.NewFields.LowLimit.value) > 0) {
                //     emit([low_party, currency], 1);
                // }

                // if (parseFloat(cnode.NewFields.HighLimit.value) > 0) {
                //     emit([high_party, currency], 1);
                // }


                // // add trust lines for high limit party
                // var high = {};
                // high[currency] = {};
                // high[currency].inc_lines = parseFloat(cnode.NewFields.LowLimit.value) > 0 ? 1 : 0;
                // high[currency].out_lines = parseFloat(cnode.NewFields.HighLimit.value) > 0 ? 1 : 0;
                // high[currency].balance_change = 0 - parseFloat(cnode.NewFields.Balance.value);
                // emit(high_party, high);
                emit([currency, high_party], 0 - parseFloat(cnode.NewFields.Balance.value));


                // // add trust lines for low limit party
                // var low = {};
                // low[currency] = {};
                // low[currency].inc_lines = parseFloat(cnode.NewFields.HighLimit.value) > 0 ? 1 : 0;
                // low[currency].out_lines = parseFloat(cnode.NewFields.LowLimit.value) > 0 ? 1 : 0;
                // low[currency].balance_change = parseFloat(cnode.NewFields.Balance.value);
                // emit(low_party, low);
                emit([currency, low_party], parseFloat(cnode.NewFields.Balance.value));

            } else if (tx.metaData.AffectedNodes[n].hasOwnProperty("ModifiedNode") && tx.metaData.AffectedNodes[n].ModifiedNode.LedgerEntryType === "RippleState") {
                var mnode = tx.metaData.AffectedNodes[n].ModifiedNode;

                // // high limit changed
                // if (mnode.PreviousFields.hasOwnProperty("HighLimit")) {

                //     // adding trust line
                //     if (parseFloat(mnode.PreviousFields.HighLimit.value) === 0 && parseFloat(mnode.FinalFields.HighLimit.value) > 0) {

                //         var currency = mnode.FinalFields.LowLimit.currency,
                //             trusting_party = mnode.FinalFields.HighLimit.issuer,
                //             trusted_party = mnode.FinalFields.LowLimit.issuer;

                //         // var trusted = {};
                //         // trusted[currency] = {
                //         //     "inc_lines": 1
                //         // };
                //         // emit({trusted_party, currency], 1);

                //         // var trusting = {};
                //         // trusting[currency] = {
                //         //     "out_lines": 1
                //         // };
                //         emit([trusting_party, currency], 1);
                //     }

                //     // removing trust line
                //     if (parseFloat(mnode.PreviousFields.HighLimit.value) > 0 && parseFloat(mnode.FinalFields.HighLimit.value) === 0) {

                //         var currency = mnode.FinalFields.LowLimit.currency,
                //             trusting_party = mnode.FinalFields.HighLimit.issuer,
                //             trusted_party = mnode.FinalFields.LowLimit.issuer;

                //         // var trusted = {};
                //         // trusted[currency] = {
                //         //     "inc_lines": -1
                //         // };
                //         // emit([trusted_party, currency], -1);

                //         // var trusting = {};
                //         // trusting[currency] = {
                //         //     "out_lines": -1
                //         // };
                //         emit([trusting_party, currency], -1);
                //     }

                // }


                // // low limit changed
                // if (mnode.PreviousFields.hasOwnProperty("LowLimit")) {

                //     // adding trust line
                //     if (parseFloat(mnode.PreviousFields.LowLimit.value) === 0 && parseFloat(mnode.FinalFields.LowLimit.value) > 0) {

                //         var currency = mnode.FinalFields.HighLimit.currency,
                //             trusting_party = mnode.FinalFields.LowLimit.issuer,
                //             trusted_party = mnode.FinalFields.HighLimit.issuer;

                //         // var trusted = {};
                //         // trusted[currency] = {
                //         //     "inc_lines": 1
                //         // };
                //         // emit([trusted_party, currency], 1);

                //         // var trusting = {};
                //         // trusting[currency] = {
                //         //     "out_lines": 1
                //         // };
                //         emit([trusting_party, currency], 1);
                //     }

                //     // removing trust line
                //     if (parseFloat(mnode.PreviousFields.LowLimit.value) > 0 && parseFloat(mnode.FinalFields.LowLimit.value) === 0) {

                //         var currency = mnode.FinalFields.HighLimit.currency,
                //             trusting_party = mnode.FinalFields.LowLimit.issuer,
                //             trusted_party = mnode.FinalFields.HighLimit.issuer;

                //         // var trusted = {};
                //         // trusted[currency] = {
                //         //     "inc_lines": -1
                //         // };
                //         // emit([trusted_party, currency], -1);

                //         // var trusting = {};
                //         // trusting[currency] = {
                //         //     "out_lines": -1
                //         // };
                //         emit([trusting_party, currency], -1);
                //     }

                // }


                // balance changed
                if (mnode.PreviousFields.hasOwnProperty("Balance")) {

                    var currency = mnode.FinalFields.Balance.currency,
                        low_party = mnode.FinalFields.LowLimit.issuer,
                        high_party = mnode.FinalFields.HighLimit.issuer;

                    // var low = {};
                    // low[currency] = {
                    //     "balance_change": (mnode.FinalFields.Balance.value - mnode.PreviousFields.Balance.value)
                    // };
                    emit([currency, low_party], (mnode.FinalFields.Balance.value - mnode.PreviousFields.Balance.value));

                    // var high = {};
                    // high[currency] = {
                    //     "balance_change": (0 - (mnode.FinalFields.Balance.value - mnode.PreviousFields.Balance.value))
                    // };
                    emit([currency, high_party], (0 - (mnode.FinalFields.Balance.value - mnode.PreviousFields.Balance.value)));

                }
            }
        }
    }
}