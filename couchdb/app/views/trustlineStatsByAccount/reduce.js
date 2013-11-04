function(keys, values) {

    var results = {
        "incoming": 0,
        "outgoing": 0,
        "balance_change": 0,
        // "trusted_parties": []
    };

    // var trusted = {};

    values.forEach(function(val){
        if (typeof val.incoming === "number")
            results.incoming += val.incoming;
        if (typeof val.outgoing === "number")
            results.outgoing += val.outgoing;
        if (typeof val.balance_change === "number")
            results.balance_change += val.balance_change;
        // if (typeof val.trusted_parties === "object")
        //     val.trusted_parties.forEach(function(addr){
        //         trusted[addr] = 1;
        //     });
    });

    // results.trusted_parties = Object.keys(trusted);

    return results;
}