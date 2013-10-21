function (doc) {
    emit(doc.ledger_index, doc.transactions.length);
}