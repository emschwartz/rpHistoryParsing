function(head, req) {
    var view = req.path.slice(2 + req.path.indexOf("_list"))[0];
    if (view === "trustlinesByAccount") {
        // var gateways = {};
        // var gateways = {};
        var row;
        while (row = getRow()) {
            if (row.value.incoming > 100 
                && row.value.balance_change < 0) {
                
                var acct = row.key[0],
                    curr = row.key[1];

                // if (typeof gateways[acct] === "undefined") 
                //     gateways[acct] = [];
                // gateways[acct].push(curr);
                var gateway = {
                    acct: acct, 
                    curr: curr, 
                    in: row.value.incoming, 
                    out: row.value.outgoing, 
                    bal: row.value.balance_change
                };

                send(JSON.stringify(gateway))
                // if (typeof gateways[acct][curr] === "undefined") gateways[acct][curr] = {};
                // gateways[acct][curr]. in = row.value.incoming;
                // gateways[acct][curr].out = row.value.outgoing;
                // gateways[acct][curr].bal = row.value.balance_change;
            }
        }
        // send(JSON.stringify(gateways));
    } else if (view === "trustlinesByCurrency") {
        send("trustlinesByCurrency");
    } else {
        send('Error, this view can only be used with the views "trustlinesByCurrency" and "trustlinesByAccount"');
    }
}