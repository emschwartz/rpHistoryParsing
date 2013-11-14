var sqlite3 = require( 'sqlite3' ).verbose( ),
  winston = require( 'winston' ),
  path = require( 'path' ),
  moment = require( 'moment' ),
  _ = require( 'lodash' ),
  async = require( 'async' ),
  ripple = require( 'ripple-lib' ),
  Ledger = require( './node_modules/ripple-lib/src/js/ripple/ledger' ).Ledger,
  Remote = ripple.Remote;

var config = require( './config' );

var FIRSTLEDGER = 32570;
var FIRSTCLOSETIME = 410325670;


function RippledQuerier( maxIterators ) {

  if ( !maxIterators )
    maxIterators = 1000;

  var dbs = {
    ledb: new sqlite3.Database( path.resolve( config.dbPath || "/ripple/server/db", 'ledger.db' ) ),
    txdb: new sqlite3.Database( path.resolve( config.dbPath || "/ripple/server/db", 'transaction.db' ) )
  };

  var rq = {};

  rq.FIRSTLEDGER = FIRSTLEDGER;
  rq.FIRSTCLOSETIME = FIRSTCLOSETIME;


  // RippledQuerier functions

  rq.getLatestLedgerIndex = function( callback ) {
    getLatestLedgerIndex( dbs, callback );
  };

  rq.getLedger = function( ledgerIndex, callback ) {
    getLedger( dbs, ledgerIndex, callback );
  };

  rq.searchLedgerByClosingTime = function( rpepoch, callback ) {
    searchLedgerByClosingTime( dbs, rpepoch, callback );
  };

  rq.getLedgerRange = function( start, end, callback ) {
    getLedgerRange( dbs, start, end, maxIterators, callback );
  };

  rq.getLedgersForRpEpochRange = function( rpStart, rpEnd, callback ) {
    getLedgersForRpEpochRange( dbs, rpStart, rpEnd, maxIterators, callback );
  };

  // rq.getLedgersForTimeRange gets the PARSED ledgers between the two given momentjs-readable times
  rq.getLedgersForTimeRange = function( start, end, callback ) {

    var startEpoch = rpEpochFromTimestamp( moment( start ).valueOf( ) );
    var endEpoch = rpEpochFromTimestamp( moment( end ).valueOf( ) );

    getLedgersForRpEpochRange( dbs, startEpoch, endEpoch, maxIterators, callback );
  };

  return rq;

}



// PRIVATE FUNCTIONS


// printCallback is used as the default callback function

function printCallback( err, result ) {
  if ( err ) {
    winston.error( err );
  } else {
    winston.info( result );
  }
}

// rpEpochFromTimestamp converts the ripple epochs to a javascript timestamp

function rpEpochFromTimestamp( timestamp ) {
  return timestamp / 1000 - 0x386D4380;
}


// getRawLedger gets the raw ledger ledb database 

function getRawLedger( dbs, ledgerIndex, callback ) {
  if ( !callback ) callback = printCallback;
  if ( !dbs ) winston.error( "dbs is not defined in getRawLedger" );


  dbs.ledb.all( "SELECT * FROM Ledgers WHERE LedgerSeq = ?;", [ ledgerIndex ],
    function( err, rows ) {
      if ( err ) {
        winston.error( "Error getting raw ledger:", ledgerIndex, rows );
        callback( err );
        return;
      }

      if ( rows.length === 0 ) {

        callback( new Error( "dbs.ledb has no ledger of index: " + ledgerIndex ) );
        return;

      } else if ( rows.length === 1 ) {

        callback( null, rows[ 0 ] );

      } else if ( rows.length > 1 ) {

        // get the next ledger's parent_hash to determine which of the conflicting
        // ledgers headers here is the correct one
        winston.info("Multiple rows for index:", ledgerIndex, JSON.stringify(rows));
        dbs.ledb.all( "SELECT * FROM Ledgers WHERE LedgerSeq = ?;", [ ledgerIndex + 1 ],
          function( err, nextRows ) {
            if ( err ) {
              winston.error( "Error getting raw ledger:", ledgerIndex, nextRows );
              callback( err );
              return;
            }

            if ( nextRows.length === 1 ) {

              winston.info("Next rows:", JSON.stringify(nextRows));

              var correctHeader = _.find( rows, function( row ) {
                return row.ledger_hash === nextRows[ 0 ].parent_hash;
              } );

              winston.info("correctHeader:", JSON.stringify(correctHeader));

              correctHeader.conflicting_ledger_headers = _.filter( rows, function( row ) {
                return row.ledger_hash !== nextRows[ 0 ].parent_hash;
              } );

              winston.info("conflicting_ledger_headers:", JSON.stringify(correctHeader.conflicting_ledger_headers));

              callback( null, correctHeader );


            } else {
              callback( new Error( "Error: multiple consecutive ledgers have conflicting headers" ) );
              return;
            }

          } );
      }
    } );
}

// getRawTxForLedger gets the raw tx blobs from the txdb database

function getRawTxForLedger( dbs, ledgerIndex, callback ) {
  if ( !callback ) callback = printCallback;

  dbs.txdb.all( "SELECT * FROM Transactions WHERE LedgerSeq = ?;", [ ledgerIndex ],
    function( err, rows ) {
      if ( err ) {
        winston.error( "Error getting raw txs for ledger:", ledgerIndex );
        callback( err );
        return;
      }

      callback( null, rows );
    } );
}


function parseRawLedgerHeader( rawHeader ) {

  return {
    account_hash: rawHeader.AccountSetHash,
    close_time_rpepoch: rawHeader.ClosingTime,
    close_time_timestamp: ripple.utils.toTimestamp( rawHeader.ClosingTime ),
    close_time_human: moment( ripple.utils.toTimestamp( rawHeader.ClosingTime ) ).utc( ).format( "YYYY-MM-DD HH:mm:ss Z" ),
    close_time_resolution: rawHeader.CloseTimeRes,
    ledger_hash: rawHeader.LedgerHash,
    ledger_index: rawHeader.LedgerSeq,
    parent_hash: rawHeader.PrevHash,
    total_coins: rawHeader.TotalCoins,
    transaction_hash: rawHeader.TransSetHash
  };

}


function blobToJSON( blob ) {
  var buff = new Buffer( blob );
  var buffArray = [ ];
  for ( var i = 0, len = buff.length; i < len; i++ ) {
    buffArray.push( buff[ i ] );
  }
  var serializedObj = new ripple.SerializedObject( buffArray );
  return serializedObj.to_json( );
}

// parseLedger parses the raw ledger and associated raw txs into a single json ledger

function parseLedger( rawLedger, raw_txs, callback ) {

  var ledger = parseRawLedgerHeader( rawLedger );

  // store conflicting headers if there are multiple headers for a given ledger_index
  if ( rawLedger.conflicting_ledger_headers && rawLedger.conflicting_ledger_headers.length > 0 ) {
    ledger.conflicting_ledger_headers = _.map( rawLedger.conflicting_ledger_headers, parseRawLedgerHeader );
  }

  ledger.transactions = _.map( raw_txs, function( raw_tx ) {

    var parsedTx = blobToJSON( raw_tx.RawTxn );
    parsedTx.metaData = blobToJSON( raw_tx.TxnMeta );

    // add exchange_rate to Offer nodes in the metaData
    for ( var n = 0, nlen = parsedTx.metaData.AffectedNodes.length; n < nlen; n++ ) {
      var node = parsedTx.metaData.AffectedNodes[ n ].CreatedNode || parsedTx.metaData.AffectedNodes[ n ].ModifiedNode || parsedTx.metaData.AffectedNodes[ n ].DeletedNode;
      if ( node.LedgerEntryType === "Offer" ) {

        var fields = node.FinalFields || node.NewFields;

        if ( typeof fields.BookDirectory === "string" ) {
          node.exchange_rate = ripple.Amount.from_quality( fields.BookDirectory ).to_json( ).value;
        }
      }
    }
    return parsedTx;

  } );


  // check that transaction hash is correct
  var ledgerJsonTxHash = Ledger.from_json( ledger ).calc_tx_hash( ).to_hex( );
  if ( ledgerJsonTxHash === ledger.transaction_hash ) {

    callback( null, ledger );

  } else {

    winston.info("Getting ledger from API because", "\n  ledgerJsonTxHash:", ledgerJsonTxHash, "\n  ledger.transaction_hash:", ledger.transaction_hash, "\n\n  Incorrect ledger:", JSON.stringify(ledger));
    getLedgerFromApi( ledger.ledger_hash, callback );

  }
}

function getLedgerFromApi( ledgerHash, callback ) {

  var remote = new Remote( {
    servers: [ {
      host: 's1.ripple.com',
      port: 443,
      secure: true
    } ]
  } );

  remote.connect( function( ) {
    remote.request_ledger( ledgerHash, {
      transactions: true,
      expand: true
    }, function( err, res ) {

      if ( err ) {
        winston.error( "Error getting ledger from rippled:", err );
        callback( err );
        return;
      }

      // add/edit fields that aren't in rippled's json format
      var ledger = res.ledger;
      ledger.close_time_rpepoch = ledger.close_time;
      ledger.close_time_timestamp = ripple.utils.toTimestamp( ledger.close_time );
      ledger.close_time_human = moment( ripple.utils.toTimestamp( ledger.close_time ) ).utc( ).format( "YYYY-MM-DD HH:mm:ss Z" );
      ledger.from_rippled_api = true;

      // remove fields that do not appear in format defined above in parseLedger
      delete ledger.close_time;
      delete ledger.hash;
      delete ledger.accepted;
      delete ledger.totalCoins;
      delete ledger.closed;
      delete ledger.seqNum;

      // add exchange rate field to metadata entries
      ledger.transactions.forEach( function( transaction ) {
        transaction.metaData.AffectedNodes.forEach( function( affNode ) {
          var node = affNode.CreatedNode || affNode.ModifiedNode || affNode.DeletedNode;

          if ( node.LedgerEntryType === "Offer" ) {

            var fields = node.FinalFields || node.NewFields;

            if ( typeof fields.BookDirectory === "string" ) {
              node.exchange_rate = ripple.Amount.from_quality( fields.BookDirectory ).to_json( ).value;
            }
          }
        } );
      } );

      // check the transaction hash of the ledger we got from the api call
      var ledgerJsonTxHash = Ledger.from_json( ledger ).calc_tx_hash( ).to_hex( );
      if ( ledgerJsonTxHash === ledger.transaction_hash ) {

        callback( null, ledger );

      } else {

        callback( new Error( "Error with ledger from rippled api call, transactions do not hash to expected value" +
          "\n  Actual:   " + ledgerJsonTxHash +
          "\n  Expected: " + ledger.transaction_hash +
          "\n\n  Ledger: " + JSON.stringify( ledger ) + "\n\n" ) );

      }
    } );
  } );
}

// getLedger gets the PARSED ledger (and associated transactions) corresponding to the ledger_index

function getLedger( dbs, ledgerIndex, callback ) {
  if ( !callback ) callback = printCallback;
  if ( !dbs ) winston.error( "dbs is not defined in getLedger" );

  getRawLedger( dbs, ledgerIndex, function( err, rawLedger ) {
    if ( err ) {
      winston.error( "Error getting raw ledger", ledgerIndex, "err", err );
      callback( err );
      return;
    }

    getRawTxForLedger( dbs, ledgerIndex, function( err, raw_txs ) {
      if ( err ) {
        winston.error( "Error getting raw tx for ledger", ledgerIndex );
        callback( err );
        return;
      }

      parseLedger( rawLedger, raw_txs, function( err, parsedLedger ) {
        if ( err ) {
          winston.error( "Error parsing ledger:", err );
          callback( err );
          return;
        }
        callback( null, parsedLedger );
      } );
    } );
  } );
}

// getLedgerRange gets the PARSED ledgers for the given range of indices

function getLedgerRange( dbs, start, end, maxIterators, callback ) {
  if ( !callback ) callback = printCallback;
  if ( !dbs ) {
    winston.error( "dbs is not defined in getLedgerRange" );
    return;
  }

  var indices = _.range( start, end );

  async.mapLimit( indices, maxIterators, function( ledgerIndex, asyncCallback ) {
    getLedger( dbs, ledgerIndex, asyncCallback );
  }, function( err, ledgers ) {
    if ( err ) {
      winston.error( "Error getting ledger range:", err );
      callback( err );
      return;
    }

    if ( ledgers.length === 0 )
      winston.info( "getLedgerRange got 0 ledgers for range", start, end );

    callback( null, ledgers );
  } );

}

// getLedgersForRpEpochRange gets the PARSED ledgers that closed between the given ripple epoch times

function getLedgersForRpEpochRange( dbs, startEpoch, endEpoch, maxIterators, callback ) {
  if ( !callback ) callback = printCallback;

  if ( endEpoch < startEpoch ) {
    var temp = endEpoch;
    endEpoch = startEpoch;
    startEpoch = temp;
  }

  if ( startEpoch < FIRSTCLOSETIME )
    startEpoch = FIRSTCLOSETIME;

  searchLedgerByClosingTime( dbs, startEpoch, function( err, startIndex ) {
    if ( err ) {
      callback( err );
      return;
    }

    // winston.info("startEpoch", startEpoch, "startIndex", startIndex);

    searchLedgerByClosingTime( dbs, endEpoch, function( err, endIndex ) {
      if ( err ) {
        callback( err );
        return;
      }

      getLedgerRange( dbs, startIndex, endIndex + 1, maxIterators, callback );

    } );
  } );
}


// getLatestLedgerIndex gets the most recent ledger index in the ledger db

function getLatestLedgerIndex( dbs, callback ) {
  if ( !callback ) callback = printCallback;

  dbs.ledb.all( "SELECT LedgerSeq FROM Ledgers ORDER BY LedgerSeq DESC LIMIT 1;", function( err, rows ) {
    if ( err ) {
      callback( err );
      return;
    }
    callback( null, rows[ 0 ].LedgerSeq );
  } );
}


// searchLedgerByClosingTime finds the ledger index of the ledger that closed nearest to the given rpepoch

function searchLedgerByClosingTime( dbs, rpepoch, callback ) {
  if ( !callback ) callback = printCallback;

  if ( rpepoch < FIRSTCLOSETIME ) {
    callback( null, FIRSTLEDGER );
    return;
  }

  getLatestLedgerIndex( dbs, function( err, latestIndex ) {
    if ( err ) {
      callback( err );
      return;
    }

    getRawLedger( dbs, latestIndex, function( err, latestLedger ) {
      if ( err ) {
        callback( err );
        return;
      }

      if ( rpepoch >= latestLedger.ClosingTime ) {
        callback( null, latestIndex );
        return;
      }

      dbRecursiveSearch( dbs.ledb, "Ledgers", "LedgerSeq", FIRSTLEDGER, latestIndex, "ClosingTime", rpepoch, callback );


    } );

  } );

}


// dbRecursiveSearch is like a binary search but with 20 divisions each time instead of 2
// (because querying the db is slower than iterating through 20 results)

function dbRecursiveSearch( db, table, index, start, end, key, val, callback ) {
  if ( !callback ) callback = printCallback;

  var numQueries = 20;

  if ( end - start <= numQueries ) {

    var queryStrFinal = "SELECT " + index + " FROM " + table + " " +
      "WHERE (" + index + ">=" + start + " " +
      "and " + index + "<" + end + " " +
      "and " + key + "<=" + val + ") " +
      "ORDER BY ABS(" + key + "-" + val + ") ASC;";

    db.all( queryStrFinal, function( err, rows ) {
      // winston.info("search got:", rows[0]);
      callback( err, rows[ 0 ][ index ] );
    } );

    return;
  }

  var indices = _.map( _.range( numQueries ), function( segment ) {
    return start + segment * Math.floor( ( end - start ) / numQueries );
  } );
  indices.push( end );

  var indexStr = indices.join( ", " ),
    queryStrRecur = "SELECT * FROM " + table + " " +
      "WHERE " + index + " IN (" + indexStr + ") " +
      "ORDER BY " + index + " ASC;";

  db.all( queryStrRecur, function( err, rows ) {

    if ( err ) {
      callback( err );
      return;
    }

    for ( var i = 0; i < rows.length - 1; i++ ) {
      // winston.info("rows[i][index]",rows[i][index], "rows[i][key]", rows[i][key], "val", val, "rows[i][index]", rows[i][index], "rows[i + 1][key]", rows[i + 1][key]);
      if ( rows[ i ][ key ] <= val && val < rows[ i + 1 ][ key ] ) {
        setImmediate( function( ) {
          dbRecursiveSearch( db, table, index, rows[ i ][ index ], rows[ i + 1 ][ index ], key, val, callback );
        } );
        return;
      }
    }
    callback( new Error( "Error in recursive search" ) );
  } );
}


module.exports = RippledQuerier;