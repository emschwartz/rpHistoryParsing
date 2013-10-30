function(head, req) {
    var view = req.path.slice(2 + req.path.indexOf("_list"))[0];
    if (view === "trustlinesByAccount") {

        var stream = false;
        if (req.query.stream || req.query.include_stats)
            stream = true;

        var gateways = {};
        var row;
        while (row = getRow()) {

            if (row.value.incoming > 50 
                && row.value.balance_change < 0) {
                
                var acct = row.key[0],
                    curr = row.key[1];

                if (typeof gateways[acct] === "undefined") 
                    gateways[acct] = {};

                if (typeof gateways[acct][curr] === "undefined") 
                    gateways[acct][curr] = {};

                if (!stream) {
                    gateways[acct][curr].in = row.value.incoming;
                    gateways[acct][curr].out = row.value.outgoing;
                    gateways[acct][curr].bal = row.value.balance_change;
                } else {
                    var gateway = {
                        acct: acct, 
                        curr: curr, 
                        in: row.value.incoming,
                        out: row.value.outgoing,
                        bal: row.value.balance_change
                    };
                    send(JSON.stringify(gateway));
                }
            }
        }
        send(JSON.stringify(gateways));
    } else if (view === "trustlinesByCurrency") {
        var gateways = {};
        var row;
        while (row = getRow()) {

            if (row.value.incoming > 100 
                && row.value.balance_change < 0) {
                
                var acct = row.key[1],
                    curr = row.key[0];

                if (typeof gateways[curr] === "undefined") 
                    gateways[curr] = {};

                if (typeof gateways[curr][acct] === "undefined") 
                    gateways[curr][acct] = {};

                gateways[curr][acct].in = row.value.incoming;
                gateways[curr][acct].out = row.value.outgoing;
                gateways[curr][acct].bal = row.value.balance_change;
            }
        }
        send(JSON.stringify(gateways));
    } else {
        send('Error, this view can only be used with the views "trustlinesByCurrency" and "trustlinesByAccount"');
    }
}