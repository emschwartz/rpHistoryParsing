function (keys, values) {

    var currencies = {};

    for (var v = 0, vlen = values.length; v < vlen; v++) {
        var currs = Object.keys(values[v]);
        for (var c = 0; c < currs.length; c++) {
            var currency = currs[c];
            if (typeof currencies[currency] === "undefined")
                currencies[currency] = {"inc_lines": 0, "out_lines": 0, "balance_change": 0};

            if (typeof values[v][currency].inc_lines === "number")
                currencies[currency].inc_lines += values[v][currency].inc_lines;
            if (typeof values[v][currency].out_lines === "number")
                currencies[currency].out_lines += values[v][currency].out_lines;
            if (typeof values[v][currency].balance_change === "number")
                currencies[currency].balance_change += values[v][currency].balance_change;
        }
    }

    return currencies;
}