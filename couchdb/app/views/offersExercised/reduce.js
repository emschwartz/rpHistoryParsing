function (keys, values, rereduce) {

    if (!rereduce) {

        var stats = {};
        var first = values[0],
            first_price = first[0]/first[1];
        var last = values[values.length - 1],
            last_price = last[0]/last[1];

        stats.high = first_price;
        stats.low = first_price;

        stats.start = first_price;
        stats.end = last_price;

        stats.vwav_numerator = first_price * first[0];
        stats.vwav_denominator = first[0];

        for (var v = 1, vlen = values.length; v < vlen; v++) {
            var trade = values[v],
                rate = trade[0]/trade[1];
            
            stats.high = Math.max(stats.high, rate);
            stats.low = Math.min(stats.low, rate);

            stats.vwav_numerator += rate * trade[0];
            stats.vwav_denominator += trade[0];
        }

        stats.volume_weighted_avg = stats.vwav_numerator / stats.vwav_denominator;

        return stats;

    } else {

        var stats = values[0];
        stats.end = values[values.length - 1].end; // this could be wrong. is this getting the values in order?

        for (var v = 1, vlen = values.length; v < vlen; v++) {
            var segment = values[v];

            stats.high = Math.max(stats.high, segment.high);
            stats.low = Math.min(stats.low, segment.low);

            stats.vwav_numerator += segment.vwav_numerator;
            stats.vwav_denominator += segment.vwav_denominator;
        }

        stats.volume_weighted_avg = stats.vwav_numerator / stats.vwav_denominator;

        return stats;

    }
}