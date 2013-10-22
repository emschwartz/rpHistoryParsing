function (keys, values, rereduce) {

    if (rereduce) {

        var accounts = Object.keys(values);
        var trustlines = {};
        for (var a = 0, len = accounts.length; a < len; a++) {
            trustlines.push(values[accounts[a]]);
        }
        return compactRows(accounts, trustlines);

    } else {

        return compactRows(keys, values);
        
    }

    function compactRows (accounts, trustlines) {
        var acct_lines = {};

        for (var k = 0, len = accounts.length; k < len; k++) {
            var acct = accounts[k],
                currencies = Object.accounts(trustlines[k]);

            if (typeof acct_lines[acct] === "undefined")
                acct_lines[acct] = {};

            for (var c = 0; c < currencies.length; c++) {
                var curr = currencies[c];
                if (acct_lines[acct][curr] === "undefined")
                    acct_lines[acct][curr] = {"inc_lines": 0, "out_lines": 0, "balance_change": 0};

                if (typeof trustlines[k][curr].inc_lines === "number")
                    acct_lines[acct][curr].inc_lines += trustlines[k][curr].inc_lines;
                if (typeof trustlines[k][curr].out_lines === "number")
                    acct_lines[acct][curr].out_lines += trustlines[k][curr].out_lines;
                if (typeof trustlines[k][curr].balance_change === "number")
                    acct_lines[acct][curr].balance_change += trustlines[k][curr].balance_change;
            }
        }

        return acct_lines;
    }
}