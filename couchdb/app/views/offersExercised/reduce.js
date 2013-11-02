function (keys, values, rereduce) {

    if (!rereduce) {

        var stats = {};
        var first_price = (values[0].length === 3 ? values[0][2] : values[0][0] / values[0][1]);

        stats.open_time = keys[0].splice(2);
        stats.open = first_price;
        stats.close_time = keys[0].splice(2);
        stats.close = first_price;

        stats.high = stats.open;
        stats.low = stats.open;

        stats.curr1_vwav_numerator = 0;
        stats.curr1_volume = 0;
        stats.curr2_volume = 0;

        stats.num_trades = 0;

        values.forEach(function(trade, index){
            var rate = (trade.length === 3 ? trade[2] : trade[0] / trade[1]);
            
            if (keys[index].splice(2) < stats.open_time) {
                stats.open_time = keys[index].splice(2);
                stats.open = rate;
            }
            if (keys[index].splice(2) > stats.close_time) {
                stats.close_time = keys[index].splice(2);
                stats.close = rate;
            }

            stats.high = Math.max(stats.high, rate);
            stats.low = Math.min(stats.low, rate);
            stats.curr1_vwav_numerator += rate * trade[0];
            stats.curr1_volume += trade[0];
            stats.curr2_volume += trade[1];
            stats.num_trades++;
        });

        stats.volume_weighted_avg = stats.curr1_vwav_numerator / stats.curr1_volume;

        return stats;

    } else {

        var stats = {
            open_time: values[0].open_time,
            open: values[0].open,
            close_time: values[0].close_time,
            close: values[0].close,
            curr1_vwav_numerator: 0,
            curr1_volume: 0,
            curr2_volume: 0,
            num_trades: 0
        };

        values.forEach(function(segment){
            if (segment.open_time < stats.open_time) {
                stats.open_time = segment.open_time;
                stats.open = segment.open;
            }
            if (segment.close_time > stats.close_time) {
                stats.close_time = segment.close_time;
                stats.close = segment.close;
            }

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