function(doc) {
  var transactions = doc.transactions,
      timestamp = doc.close_time_timestamp;
  for (var i = 0, n = transactions.length; i < n; ++i) {
    var t = transactions[i];
    if (t.TransactionType !== "Payment") continue;
    emit([t.Account, t.Destination], i);
  }
}
