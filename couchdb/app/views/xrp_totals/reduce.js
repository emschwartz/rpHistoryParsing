function(keys, values, rereduce) {
  if (!rereduce) {

    // key [account, year, month, day, hour, minute, second]
    // value balance

    var most_recent = [], 
        acct_balance;

    for (var a = 0, num_keys = keys.length; a < num_keys; a++) {
      var timestamp = keys[a].slice(1);

      if (timestamp > most_recent) {
        most_recent = timestamp;
        acct_balance = values[a];
      }
    }

    return [acct_balance].concat(most_recent);

  } else {

    var most_recent = [], 
        acct_balance;

    for (var a = 0, num_vals = values.length; a < num_vals; a++) {
      var timestamp = values[a].slice(1);

      if (timestamp > most_recent) {
        most_recent = timestamp;
        acct_balance = values[a][0];
      }
    }

    return [acct_balance].concat(most_recent);

  }
}