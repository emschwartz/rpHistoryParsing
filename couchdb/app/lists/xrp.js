function(head, req) {
    var view = req.path.slice(2 + req.path.indexOf("_list"))[0];
    if (view === "xrp_totals") {
        if (req.query.group_level === "1") {

            var stream = false;
            if (req.query.stream || req.query.include_balances)
                stream = true;

            var xrp_total = 0;
            var row;
            while (row = getRow()) {
                xrp_total += row.value[0];
                if (stream)
                    send(JSON.stringify([row.key[0], row.value[0]]));
            }
            send("XRP Total: " + (xrp_total / 1000000.0));
        } else {
            send('Error, this view should be used with query group_level=1');
        }
    } else {
        send('Error, this view can only be used with the view "xrp_totals"');
    }
}