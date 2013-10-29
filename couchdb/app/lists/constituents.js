function(head, req) {
    var view = req.path.slice(2 + req.path.indexOf("_list"))[0];
    if (view === "eventsByAccount") {

        var include_stats = false;
        if (req.query.include_stats)
            include_stats = true;

        var constituents = {
            "users": [],
            "gateways": [],
            "market_makers": [],
            "merchants": []
        };
        var row;
        while (row = getRow()) {
            var acct = row.key[0];

            if (row.value.TrustSet > 100) {
                // gateway
                if (!include_stats) {
                    constituents.gateways.push(acct);
                } else {
                    send({type: "gateway", acct: acct, stats: row.value});
                }
            } else if (row.value.OfferCreate + row.value.OfferCancel > 100) {
                // market maker
                if (!include_stats) {
                    constituents.market_makers.push(acct);
                } else {
                    send({type: "market_maker", acct: acct, stats: row.value});
                }
            } else if (row.value.Incoming_Payment > 200) {
                // merchant
                if (!include_stats) {
                    constituents.merchants.push(acct);
                } else {
                    send({type: "merchant", acct: acct, stats: row.value});
                }
            } else {
                // other
                if (!include_stats) {
                    constituents.users.push(acct);
                } else {
                    send({type: "user", acct: acct, stats: row.value});
                }
            }
        }
        if (!include_stats)
            send(JSON.stringify(constituents));
    } else {
        send('Error, this view can only be used with the view "eventsByAccount"');
    }
}