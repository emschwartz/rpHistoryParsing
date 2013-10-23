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

    function compactRows (accounts, partial_lines) {
        var acct_lines = {};

        for (var k = 0, num_accts = accounts.length; k < num_accts; k++) {
            var acct = accounts[k],
                partial_line = partial_lines[k],
                currencies = Object.keys(partial_line);

            if (typeof acct_lines[acct] === "undefined")
                acct_lines[acct] = {};

            for (var c = 0; c < currencies.length; c++) {
                var curr = currencies[c];
                if (typeof acct_lines[acct][curr] === "undefined")
                    acct_lines[acct][curr] = {"inc_lines": 0, "out_lines": 0, "balance_change": 0};

                if (typeof partial_line[curr].inc_lines === "number")
                    acct_lines[acct][curr].inc_lines += partial_line[curr].inc_lines;
                if (typeof partial_line[curr].out_lines === "number")
                    acct_lines[acct][curr].out_lines += partial_line[curr].out_lines;
                if (typeof partial_line[curr].balance_change === "number")
                    acct_lines[acct][curr].balance_change += partial_line[curr].balance_change;
            }
        }

        return acct_lines;
    }
}