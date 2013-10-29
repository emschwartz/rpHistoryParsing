function(keys, values) {
    var results = {
        "incoming": 0,
        "outgoing": 0,
        "balance_change": 0,
        "trusted_parties": []
    };

    for (var v = 0, vlen = values.length; v < vlen; v++) {
        if (typeof values[v].incoming === "number")
            results.incoming += values[v].incoming;
        if (typeof values[v].outgoing === "number")
            results.outgoing += values[v].outgoing;
        if (typeof values[v].balance_change === "number")
            results.balance_change += values[v].balance_change;
        if (typeof values[v].trusted_parties === "object")
            results.trusted_parties.concat(values[v].trusted_parties);
    }

    return results;
}