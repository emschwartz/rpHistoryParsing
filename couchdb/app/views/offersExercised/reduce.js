function (keys, values, rereduce) {

    if (!rereduce) {

        var stats = {};
        stats.open = (values[0].length === 3 ? values[0][2] : values[0][0] / values[0][1]);
        stats.close = (values[values.length-1].length === 3 ? values[values.length-1][2] : values[values.length-1][0] / values[values.length-1][1]);

        // starting values for other stats
        stats.high = stats.open;
        stats.low = stats.open;
        stats.curr1_vwav_numerator = 0;
        stats.curr1_volume = 0;
        stats.curr2_volume = 0;
        stats.num_trades = 0;

        values.forEach(function(trade){
            var rate = (trade.length === 3 ? trade[2] : trade[0] / trade[1]);
            stats.high = Math.max(stats.high, rate);
            stats.low = Math.min(stats.low, rate);
            stats.curr1_vwav_numerator += rate * trade[0];
            stats.curr1_volume += trade[0];
            stats.curr2_volume += trade[1];
            stats.num_trades ++; 
        });

        stats.volume_weighted_avg = stats.curr1_vwav_numerator / stats.curr1_volume;

        return stats;

    } else {

        var stats = values[0];
        stats.close = values[values.length - 1].close;

        values.forEach(function(segment){
            stats.high = Math.max(stats.high, segment.high);
            stats.low = Math.min(stats.low, segment.low);

            stats.curr1_vwav_numerator += segment.curr1_vwav_numerator;
            stats.curr1_volume += segment.curr1_volume;
            stats.curr2_volume += segment.curr2_volume;
            stats.num_trades += segment.num_trades;

        });

        stats.volume_weighted_avg = stats.curr1_vwav_numerator / stats.curr1_volume;

        return stats;

    }
}