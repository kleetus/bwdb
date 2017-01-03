'use strict';

var assert = require('assert');
var net = require('net');

var _ = require('lodash');
var async = require('async');
var bitcore = require('bitcore-lib');
var bitcoreNode = require('bitcore-node');
var log = bitcoreNode.log;
var lmdb = require('node-lmdb');

var BlockFilter = require('./block-filter');
var Config = require('./config');
var GlobalJobStatus = require('./global-job-status');
var db = require('./db');
var messages = require('./messages');
var models = require('./models');
var utils = require('./utils');

function WriterWorker(options) {
  this.db = null;
  this._globalJobStatus = null;
  this.stopping = false;
  this.syncing = false;
  this.blockFilter = null;
  this.walletBlock = null;
  this._server = null;
  this._initOptions(options);
  this._initClients();
  this._initQueue(options);
}

WriterWorker.DEFAULT_MAX_WORK_QUEUE = 16;
WriterWorker.DEFAULT_PRUNE_DEPTH = 4032;

WriterWorker.prototype._initOptions = function(options) {
  this.network = bitcore.Networks.get(options.network);
  assert(this.network, '"network" is an expected option, please specify "livenet", "testnet" or "regtest"');
  assert(options.bitcoinHeight >= 0, '"bitcoinHeight" is expected');
  assert(options.bitcoinHash, '"bitcoinHash" is expected');
  assert(options.clientsConfig && options.clientsConfig.length > 0, '"clientsConfig" is expected');
  assert(options.listen, '"listen" is expected');
  this.listen = options.listen;
  this.bitcoinHeight = options.bitcoinHeight;
  this.bitcoinHash = options.bitcoinHash;
  this.pruneDepth = options.pruneDepth || WriterWorker.DEFAULT_PRUNE_DEPTH;
  this.clientsConfig = options.clientsConfig;
  this.config = new Config({
    network: options.network,
    path: options.configPath
  });
};

WriterWorker.prototype._initClients = function() {
  var clients = utils.getClients(this.clientsConfig);
  utils.setClients(this, clients);
};

WriterWorker.prototype._tryAllClients = function(func, options, callback) {
  if(_.isFunction(options)) {
    callback = options;
    options = {};
  }
  utils.tryAllClients(this, func, options, callback);
};

WriterWorker.prototype._initQueue = function(options) {
  var self = this;
  this.methodsMap = this._getMethodsMap();
  this.maxWorkQueue = options.maxWorkQueue || WriterWorker.DEFAULT_MAX_WORK_QUEUE;
  this.queue = async.priorityQueue(function(task, callback) {
    self._queueWorkerIterator(task, callback);
  }, 2);
};

WriterWorker.prototype._loadLatestWalletBlock = function(callback) {
  var self = this;
  var txn = this.db.env.beginTxn({readOnly: true});
  var cursor = new lmdb.Cursor(txn, this.db.blocks);
  var found = cursor.goToLast();

  if (found === null) {
    // we will create the wallet later
    callback();
  } else {
    cursor.getCurrentBinary(function(key, value) {
      self.walletBlock = models.WalletBlock.fromBuffer(key, value);

      self.blockFilter = new BlockFilter({
        network: self.network,
        addressFilter: self.walletBlock.addressFilter
      });

      cursor.close();
      txn.abort();

      callback();
    });
  }
};

WriterWorker.prototype._setupGlobalJobStatus = function(callback) {
  this._globalJobStatus = new GlobalJobStatus();
  log.info('Starting job queue');
  callback();
};

WriterWorker.prototype._setupDatabase = function(callback) {
  var self = this;
  var dbPath = self.config.getDatabasePath();

  async.series([
    function(next) {
      utils.setupDirectory(dbPath, next);
    }, function(next) {
      self.db = db.open(dbPath);
      next();
    }
  ], callback);
};

WriterWorker.prototype._startListener = function(callback) {
  var self = this;

  // TODO handle EADDRINUSE
  this._server = net.createServer(function(socket) {
    socket.on('data', messages.parser(function(msg) {
      var task = msg.task;
      task.socket = socket;

      var priority = msg.priority || 10;

      if (self.queue.length() >= self.maxWorkQueue) {
        return self._sendResponse(socket, task.id, {
          message: 'Work queue depth exceeded'
        });
      }
      self._pushTask(task, priority);
    }));
  });
  this._server.on('error', function(err) {
    log.error(err);
  });

  this._server.listen(self.listen, function() {
    callback();
  });
};

WriterWorker.prototype._pushTask = function(task, priority) {
  if (this.methodsMap[task.method].batch && !this._globalJobStatus.createNewJob(task)) {
    return this._sendResponse(task.socket, task.id, {
      message: 'Could not create new job. JobId: ' + task.id +
        ' it is already created or too many jobs currently running.' });
  }
  log.info('Pushing: ' + task.id + ' task name: ' + task.method + ' priority: ' + priority);
  this.queue.push(task, priority);
};

WriterWorker.prototype.start = function(callback) {
  var self = this;

  async.series([
    function(next) {
      var appPath = self.config.getApplicationPath();
      utils.setupDirectory(appPath, next);
    },
    function(next) {
      self._setupDatabase(next);
    },
    function(next) {
      self._setupGlobalJobStatus(next);
    },
    function(next) {
      self._loadLatestWalletBlock(next);
    },
    function(next) {
      self._startListener(next);
    }
  ], callback);
};

WriterWorker.prototype._initWalletBlock = function() {
  if (!this.walletBlock) {
    // Needed for the first wallet creation only
    var height = this.bitcoinHeight;
    var blockHash = this.bitcoinHash;
    this.walletBlock = models.WalletBlock.create(height, blockHash);
    this.blockFilter = new BlockFilter({
      network: this.network,
      addressFilter: this.walletBlock.addressFilter
    });
    return this.walletBlock;
  }
  return false;
};

WriterWorker.prototype.stop = function(callback) {
  this.stopping = true;

  if (this._server) {
    this._server.close();
  }

  if (this.db) {
    db.close(this.db);
    setImmediate(callback);
  } else {
    setImmediate(callback);
  }
};

WriterWorker.prototype._getMethodsMap = function() {
  return {
    sync: {
      fn: this.sync,
      args: 1
    },
    importWalletAddresses: {
      fn: this.importWalletAddresses,
      args: 2,
      batch: true
    },
    saveTransaction: {
      fn: this.saveTransaction,
      args: 2
    },
    createWallet: {
      fn: this.createWallet,
      args: 1
    },
    getJobStatus: {
      fn: this.getJobStatus,
      args: 1
    }
  };
};

WriterWorker.prototype.getJobStatus = function(id, callback) {
  var job = this._globalJobStatus.get(id);

  if (!job) {
    return callback(new Error('jobId: ' + id));
  }
  callback(null, job);
};

WriterWorker.prototype._sendResponse = function(socket, id, error, result) {
  if (socket && !this._globalJobStatus.setResult(id, error, result)) {
    var msg = messages.encodeReaderMessage(id, error, result);
    socket.write(msg);
  }
  if (error) {
    log.error('Write task error:', error);
  } else {
    log.info('Completed write task: ' + id);
  }
};

WriterWorker.prototype._queueWorkerIterator = function(task, next) {
  var self = this;

  var writerMethods = self.methodsMap[task.method];
  if (writerMethods) {
    var params = task.params;

    if (!params || !params.length) {
      params = [];
    }

    if (params.length !== writerMethods.args) {
      var error = {message: 'Expected ' + writerMethods.args + ' parameter(s)'};
      self._sendResponse(task.socket, task.id, error);
      return next();
    }

    var callback = function(err, result) {
      if (err && err.deferrable && !task.deferred) {
        task.deferred = true;
        var message = 'Deferring task id: ' + task.id + ', ' +
          'method: ' + task.method + ', error: ' + err.message;
        self._globalJobStatus.update(task.id, {
          status: 'deferred',
          message: message
        });
        self._pushTask(task, 100);
        log.info(message);
        next();
      } else {
        var error = err ? {message: err.message}: null;
        self._sendResponse(task.socket, task.id, error, result);
        next();
      }
    };

    params = params.concat(callback);
    if (writerMethods.batch) {
      params.unshift(task);
    }
    log.info('Running task id: ' + task.id + ' name: ' + task.method);
    self._globalJobStatus.update(task.id, { status: 'running' });
    writerMethods.fn.apply(self, params);
  } else {
    self._sendResponse(task.socket, task.id, {message: 'Method Not Found'});
    next();
  }
};

WriterWorker.prototype._addUTXO = function(txn, walletId, utxoData) {
  assert(utxoData.satoshis >= 0);
  assert(utxoData.height >= 0);
  assert(utxoData.txid);
  assert(utxoData.index >= 0);
  assert(utxoData.address);

  var utxo = models.WalletUTXO.create(walletId, utxoData);
  txn.putBinary(this.db.utxos, utxo.getKey(), utxo.getValue());

  var utxoSat = models.WalletUTXOBySatoshis.create(walletId, utxoData);
  txn.putBinary(this.db.utxosBySatoshis, utxoSat.getKey(), utxoSat.getValue());

  var utxoHeight = models.WalletUTXOByHeight.create(walletId, utxoData);
  txn.putBinary(this.db.utxosByHeight, utxoHeight.getKey(), utxoHeight.getValue());
};

WriterWorker.prototype._undoAddUTXO = function(txn, walletId, utxoData) {
  assert(utxoData.satoshis >= 0);
  assert(utxoData.height >= 0);
  assert(utxoData.txid);
  assert(utxoData.index >= 0);
  assert(utxoData.address);

  var utxo = models.WalletUTXO.create(walletId, utxoData);
  txn.del(this.db.utxos, utxo.getKey());

  var utxoSat = models.WalletUTXOBySatoshis.create(walletId, utxoData);
  txn.del(this.db.utxosBySatoshis, utxoSat.getKey());

  var utxoHeight = models.WalletUTXOByHeight.create(walletId, utxoData);
  txn.del(this.db.utxosByHeight, utxoHeight.getKey());
};

WriterWorker.prototype._removeUTXO = function(txn, walletId, delta, spentOutputs) {
  var utxoKey = models.WalletUTXO.getKey(walletId, delta.prevtxid, delta.prevout);
  var utxoBuffer = txn.getBinary(this.db.utxos, utxoKey);
  assert(utxoBuffer, '"utxo" could not be found');

  var utxo = models.WalletUTXO.fromBuffer(utxoKey, utxoBuffer, this.network);

  // Keep track of spent utxos removed to be able to undo this action
  spentOutputs[utxoKey.toString('hex')] = utxo.toObject();

  txn.del(this.db.utxos, utxoKey);

  var satKey = models.WalletUTXOBySatoshis.getKey(walletId, utxo.satoshis, delta.prevtxid, delta.prevout);
  txn.del(this.db.utxosBySatoshis, satKey);

  var heightKey = models.WalletUTXOByHeight.getKey(walletId, utxo.height, delta.prevtxid, delta.prevout);
  txn.del(this.db.utxosByHeight, heightKey);
};

WriterWorker.prototype._undoRemoveUTXO = function(txn, walletId, delta, spentOutputs) {
  var utxoKey = models.WalletUTXO.getKey(walletId, delta.prevtxid, delta.prevout);
  assert(spentOutputs[utxoKey.toString('hex')], 'undo information not available to restore utxo');
  var utxo = models.WalletUTXO.create(walletId, spentOutputs[utxoKey.toString('hex')]);

  txn.putBinary(this.db.utxos, utxoKey, utxo.getValue());

  var utxoData = utxo.toObject();
  assert(utxoData.satoshis >= 0);
  assert(utxoData.height >= 0);
  assert(utxoData.txid);
  assert(utxoData.index >= 0);
  assert(utxoData.address);

  txn.putBinary(this.db.utxos, utxo.getKey(), utxo.getValue());

  var utxoSat = models.WalletUTXOBySatoshis.create(walletId, utxoData);
  txn.putBinary(this.db.utxosBySatoshis, utxoSat.getKey(), utxoSat.getValue());

  var utxoHeight = models.WalletUTXOByHeight.create(walletId, utxoData);
  txn.putBinary(this.db.utxosByHeight, utxoHeight.getKey(), utxoHeight.getValue());
};

WriterWorker.prototype._connectUTXO = function(txn, walletId, height, transaction, delta, spentOutputs) {
  if (delta.satoshis > 0) {
    var utxoData = {
      satoshis: delta.satoshis,
      height: height,
      txid: transaction.txid,
      index: delta.index,
      address: delta.address
    };
    this._addUTXO(txn, walletId, utxoData);
  } else {
    assert(delta.satoshis <= 0);
    this._removeUTXO(txn, walletId, delta, spentOutputs);
  }
};

WriterWorker.prototype._disconnectUTXO = function(txn, walletId, height, transaction, delta, spentOutputs) {
  if (delta.satoshis > 0) {
    var utxoData = {
      satoshis: delta.satoshis,
      height: height,
      txid: transaction.txid,
      index: delta.index,
      address: delta.address
    };
    this._undoAddUTXO(txn, walletId, utxoData);
  } else {
    assert(delta.satoshis <= 0);
    this._undoRemoveUTXO(txn, walletId, delta, spentOutputs);
  }
};

/**
 * This will insert txids into txn. Does not modify the current wallet
 * reference, but the arguments passed into the function.

 * @param {Object} txn - Database transaction
 * @param {Object} wallets - An object to hold updated wallets
 * @param {Object} data
 * @param {Object} data.blockHeight - The block height of deltas
 * @param {String} data.address - The base58 encoded hex string
 * @param {String} data.deltas - The deltas for the address as returned from block handler
 * @param {Function} callback
 * @param {}
 */
WriterWorker.prototype._connectTransaction = function(txn, wallets, height, transaction, spentOutputs, callback) {
  var self = this;

  function applyDelta(delta) {
    // Make sure that the address exists in the wallet (false positives from bloom filter)
    var key = models.WalletAddressMap.getKey(delta.address, self.network);
    var buffer = txn.getBinary(self.db.addressesMap, key);
    if (!buffer) {
      return;
    }

    var walletIds = utils.splitBuffer(buffer, 32);
    walletIds.forEach(function(walletId) {

      var satoshisDelta = 0;

      // update txid
      var txid = models.WalletTxid.create(walletId, height, transaction.index, transaction.txid);
      txn.putBinary(self.db.txids, txid.getKey(), txid.getValue());

      // sum the satoshis
      satoshisDelta += delta.satoshis;

      // update the utxo
      self._connectUTXO(txn, walletId, height, transaction, delta, spentOutputs);

      // update wallet balance
      var walletKey = walletId.toString('hex');
      if (!wallets[walletKey]) {
        var walletBuffer = txn.getBinary(self.db.wallets, walletId);
        var wallet = models.Wallet.fromBuffer(walletId, walletBuffer);
        wallets[walletKey] = wallet;
      }
      wallets[walletKey].addBalance(satoshisDelta);

    });
  }

  transaction.inputs.forEach(applyDelta);
  transaction.outputs.forEach(applyDelta);

  callback();
};

WriterWorker.prototype._pruneWalletBlocks = function(callback) {
  /* jshint maxstatements: 30 */
  var txn = this.db.env.beginTxn();

  var self = this;
  var cursor = new lmdb.Cursor(txn, this.db.blocks);
  var found = cursor.goToLast();
  if (found === null) {
    return abort();
  }

  var currentHeight;
  var pruneHeight = 0;
  cursor.getCurrentBinary(function(key, value) {
    var walletBlock = models.WalletBlock.fromBuffer(key, value);
    pruneHeight = Math.max(0, walletBlock.height - self.pruneDepth);
    log.info('Pruning wallet blocks from height', pruneHeight);
  });

  var pruneKey = models.WalletBlock.getKey(pruneHeight);
  found = cursor.goToKey(pruneKey);

  if (found === null) {
    var prev = cursor.goToPrev();
    if (prev === null) {
      return abort();
    }
    cursor.getCurrentBinary(function(key, value) {
      var block = models.WalletBlock.fromBuffer(key, value);
      if (block.height < pruneHeight) {
        currentHeight = block.height;
        prune();
      } else {
        abort();
      }
    });
  } else {
    currentHeight = pruneHeight;
    prune();
  }

  function prune() {
    log.info('Pruning wallet block at height', currentHeight);
    cursor.del();
    var prev = cursor.goToPrev();

    if (prev !== null) {
      cursor.getCurrentBinary(function(key, value) {
        var block = models.WalletBlock.fromBuffer(key, value);
        currentHeight = block.height;
        if (block.height < pruneHeight) {
          setImmediate(prune);
        } else {
          log.info('Previous block not less than prune height');
          commit();
        }
      });
    } else {
      log.info('No previous block found to prune');
      commit();
    }
  }

  function abort() {
    cursor.close();
    txn.abort();
    callback();
  }

  function commit() {
    cursor.close();
    txn.commit();
    self.db.env.sync(callback);
  }
};

/**
 * This will commit any changes to the database and update the
 * current wallet reference to this data.
 *
 * @param {Object} txn - Transaction with changes
 * @param {Object} wallets - An object with updated wallets
 * @param {Block} block - The block being commited
 * @param {Function} callback
 */
WriterWorker.prototype._connectBlockCommit = function(txn, wallets, block, spentOutputs, callback) {
  var self = this;

  // Prevent in memory modifications until we know the changes
  // have been persisted to disk, so that the method can be reattempted without
  // causing state issues
  var walletBlock = this.walletBlock.clone();

  // Update the latest status of the blocks
  walletBlock.blockHash = new Buffer(block.hash, 'hex');
  walletBlock.height = block.height;

  // Keep the deltas applied with this block so that we can undo the action later if needed
  // and record which outputs were spent and removed so we can reverse the action later.
  walletBlock.deltas = block.deltas;
  walletBlock.spentOutputs = spentOutputs;

  txn.putBinary(this.db.blocks, walletBlock.getKey(), walletBlock.getValue());

  // Update all of the wallets
  for (var key in wallets) {
    var wallet = wallets[key];
    txn.putBinary(self.db.wallets, wallet.getKey(), wallet.getValue());
  }

  txn.commit();

  this.db.env.sync(function(err) {
    if (err) {
      return callback(err);
    }

    self.walletBlock = walletBlock;
    self.blockFilter = new BlockFilter({
      network: self.network,
      addressFilter: self.walletBlock.addressFilter
    });

    log.info('Block ' + self.walletBlock.blockHash.toString('hex') +
             ' connected to wallets at height ' + self.walletBlock.height);

    self._pruneWalletBlocks(function(err) {
      if (err) {
        log.error(err);
      }
      self._globalJobStatus.prune(callback);
    });

  });
};

/**
 * This will take a block and parse it for addresses that apply to this wallet
 * and update the database with the new transactions.
 * @param {Block} block
 * @param {Function} callback
 */
WriterWorker.prototype._connectBlock = function(block, callback) {
  var self = this;

  var transactions = this.blockFilter.filterDeltas(block);

  var txn = this.db.env.beginTxn();

  var wallets = {};
  var spentOutputs = {};

  async.eachSeries(transactions, function(transaction, next) {
    self._connectTransaction(txn, wallets, block.height, transaction, spentOutputs, next);
  }, function(err) {
    if (err) {
      txn.abort();
      return callback(err);
    }
    self._connectBlockCommit(txn, wallets, block, spentOutputs, callback);
  });
};

WriterWorker.prototype._disconnectTransaction = function(txn, wallets, height, transaction, spentOutputs, callback) {
  var self = this;

  function removeDelta(delta) {
    // Make sure that the address exists in the wallet (false positives from bloom filter)
    var key = models.WalletAddressMap.getKey(delta.address, self.network);
    var buffer = txn.getBinary(self.db.addressesMap, key);
    if (!buffer) {
      return;
    }

    var walletIds = utils.splitBuffer(buffer, 32);
    walletIds.forEach(function(walletId) {
      var satoshisDelta = 0;

      // remove the txid for this wallet, there may be several deltas that reference
      // this same txid, and as such each will remove the txid, however after the
      // first removal the following attempts will get an error because it's already
      // been removed, we can handle this error and continue
      var txid = models.WalletTxid.create(walletId, height, transaction.index, transaction.txid);
      try {
        txn.del(self.db.txids, txid.getKey());
      } catch(e) {
        if (!e.message.match(/^MDB_NOTFOUND/)) {
          throw e;
        }
      }

      // restore the previous balance
      satoshisDelta -= delta.satoshis;

      // restore the utxos values
      self._disconnectUTXO(txn, walletId, height, transaction, delta, spentOutputs);

      // update wallet balance
      var walletKey = walletId.toString('hex');
      if (!wallets[walletKey]) {
        var walletBuffer = txn.getBinary(self.db.wallets, walletId);
        var wallet = models.Wallet.fromBuffer(walletId, walletBuffer);
        wallets[walletKey] = wallet;
      }
      wallets[walletKey].addBalance(satoshisDelta);
    });
  }

  transaction.inputs.forEach(removeDelta);
  transaction.outputs.forEach(removeDelta);

  callback();
};

WriterWorker.prototype._disconnectBlockCommit = function(txn, wallets, walletBlock, callback) {
  var self = this;

  var blockKey = models.WalletBlock.getKey(walletBlock.height - 1);
  var blockValue = txn.getBinary(this.db.blocks, blockKey);
  assert(blockValue, 'could not disconnect tip, previous wallet block not found');
  var prevWalletBlock = models.WalletBlock.fromBuffer(blockKey, blockValue);

  // Update all of the wallets
  for (var key in wallets) {
    var wallet = wallets[key];
    txn.putBinary(self.db.wallets, wallet.getKey(), wallet.getValue());
  }

  txn.commit();

  this.db.env.sync(function(err) {
    if (err) {
      return callback(err);
    }

    self.walletBlock = prevWalletBlock;
    self.blockFilter = new BlockFilter({
      network: self.network,
      addressFilter: self.walletBlock.addressFilter
    });

    log.info('Block ' + walletBlock.blockHash.toString('hex') +
             ' disconnected from wallet at height ' + walletBlock.height);
    callback();
  });

};

WriterWorker.prototype._disconnectTip = function(callback) {
  var self = this;

  var txn = this.db.env.beginTxn();

  var walletBlock = this.walletBlock.clone();
  var wallets = {};

  async.eachSeries(walletBlock.deltas, function(transaction, next) {
    self._disconnectTransaction(txn, wallets, walletBlock.height, transaction, walletBlock.spentOutputs, next);
  }, function(err) {
    if (err) {
      txn.abort();
      return callback(err);
    }
    self._disconnectBlockCommit(txn, wallets, walletBlock, callback);
  });

};

WriterWorker.prototype._maybeGetBlockHash = function(blockArg, callback) {
  var self = this;

  if (_.isNumber(blockArg) || (blockArg.length < 40 && /^[0-9]+$/.test(blockArg))) {
    self._tryAllClients(function(client, done) {
      client.getBlockHash(blockArg, function(err, response) {
        if (err) {
          return done(utils.wrapRPCError(err));
        }
        done(null, response.result);
      });
    }, callback);
  } else {
    callback(null, blockArg);
  }
};

WriterWorker.prototype._getBlockDeltas = function(blockArg, callback) {
  var self = this;

  function queryBlock(err, blockhash) {
    if (err) {
      return callback(err);
    }

    self._tryAllClients(function(client, done) {
      client.getBlockDeltas(blockhash, function(err, response) {
        if (err) {
          return done(utils.wrapRPCError(err));
        }
        done(null, response.result);
      });
    }, callback);
  }
  self._maybeGetBlockHash(blockArg, queryBlock);
};

/**
 * This will either add the next block to the wallet or will remove the current
 * block tip in the event of a reorganization.
 * @param {Number} height - The current height
 * @param {Function} callback
 */
WriterWorker.prototype._updateTip = function(height, callback) {
  var self = this;

  self._getBlockDeltas(height + 1, function(err, blockDeltas) {
    if (err) {
      return callback(err);
    }

    var prevHash = blockDeltas.previousblockhash;

    if (prevHash === self.walletBlock.blockHash.toString('hex')) {

      // This block appends to the current chain tip and we can
      // immediately add it to the chain and create indexes.
      self._connectBlock(blockDeltas, function(err) {
        if (err) {
          return callback(err);
        }
        // TODO send event?
        callback();
      });
    } else {
      // This block doesn't progress the current tip, so we'll attempt
      // to rewind the chain to the common ancestor of the block and
      // then we can resume syncing.
      log.warn('Reorg detected! Current tip: ' + self.walletBlock.blockHash.toString('hex'));
      self._disconnectTip(function(err) {
        if (err) {
          return callback(err);
        }
        log.warn('Disconnected current tip. New tip is ' + self.walletBlock.blockHash.toString('hex'));
        callback();
      });
    }
  });
};


/**
 * This function will continously update the block tip of the chain until it matches
 * the bitcoin height.
 */
WriterWorker.prototype.sync = function(options, callback) {

  // Update the current state of bitcoind chain
  assert(options.bitcoinHeight >= 0, '"bitcoinHeight" is expected');
  assert(options.bitcoinHash, '"bitcoinHash" is expected');

  this.bitcoinHeight = options.bitcoinHeight;
  this.bitcoinHash = options.bitcoinHash;

  var self = this;
  if (self.syncing || self.stopping || !self.walletBlock) {
    return callback();
  }
  log.info('Starting sync, height: ' + this.walletBlock.height +
               ' hash:', this.walletBlock.blockHash.toString('hex'));

  self.syncing = true;

  var height;
  async.whilst(function() {
    if (self.stopping) {
      return false;
    }
    height = self.walletBlock.height;
    return height < self.bitcoinHeight;
  }, function(done) {
    self._updateTip(height, done);
  }, function(err) {
    self.syncing = false;
    if (err) {
      log.error('Unable to sync:', err.stack);
      return callback(err);
    }

    log.info('Finished sync, height: ' + self.walletBlock.height +
             ' hash:', self.walletBlock.blockHash.toString('hex'));
    callback();
  });

};

WriterWorker.prototype._addAddressesToWalletTxid = function(txn, walletId, delta) {
  var txid = models.WalletTxid.create(walletId, delta.height, delta.blockindex, delta.txid);
  txn.putBinary(this.db.txids, txid.getKey(), txid.getValue());
  try {
    // Flush any imported/cached transactions with this txid
    txn.del(this.db.txs, models.WalletTransaction.getKey(walletId, delta.txid));
  } catch(err) {
    // noop
  }
};

/* jshint maxparams:8 */
WriterWorker.prototype._addAddressesToWallet = function(task, txn, walletBlock,
  walletId, wallet, newAddresses, callback) {
  var self = this;

  self._globalJobStatus.update(task.id, { progress: 0.25 });
  log.info('Adding addresses to wallet: ', walletId.toString('hex'));

  var addresses = newAddresses.map(function(a) {
    return a.address.toString();
  });

  // split the large query into smaller queries as it's possible
  // to reach a maximum string length in the responses
  assert(walletBlock.height >= 0, 'walletBlock "height" property is expected to be a number');
  var rangeMax = Math.max(walletBlock.height, 2);
  var ranges = utils.splitRange(1, rangeMax, 25000);
  var queries = [];
  for (var i = 0; i < ranges.length; i++) {
    queries.push({
      addresses: addresses,
      start: ranges[i][0],
      end: ranges[i][1],
      chainInfo: true
    });
  }

  var lastHash;
  var lastHeight;

  async.eachSeries(queries, function(query, next) {
    self.clients.getAddressDeltas(query, function(err, response) {
      if (err) {
        return next(utils.wrapRPCError(err));
      }

      // find the balance delta and new transactions
      var balanceDelta = 0;

      var deltas = response.result.deltas;

      lastHash = response.result.end.hash;
      lastHeight = response.result.end.height;

      for (var i = 0; i < deltas.length; i++) {
        var delta = deltas[i];

        balanceDelta += delta.satoshis;

        // add the txid
        self._addAddressesToWalletTxid(txn, walletId, delta);
      }

      // update wallet balance
      wallet.balance += balanceDelta;

      // update bloom filters with new address
      for (var j = 0; j < newAddresses.length; j++) {
        var hashBuffer = newAddresses[j].address.hashBuffer;
        walletBlock.addressFilter.insert(hashBuffer);
        wallet.addressFilter.insert(hashBuffer);
      }

      next();
    });

  }, function(err) {
    if (err) {
      return callback(err);
    }

    // Verify that the hash of the chain from the results matches what we expect
    //why would the response.result.end.hash change besides the fact that a reorg happened?
    if (lastHash !== walletBlock.blockHash.toString('hex')) {
      return callback(new Error('Unexpected chain hash from address deltas'));
    }
    var currentProgress = self._globalJobStatus.get(task.id);
    self._globalJobStatus.update(task.id, { progress: (0.75/18)+currentProgress.progress });
    callback();
  });

};

/* jshint maxparams:7 */
WriterWorker.prototype._commitWalletAddresses = function(txn, walletBlock, walletId, wallet, newAddresses, callback) {
  /* jshint maxstatements:20 */

  log.info('Commiting addresses to wallet: ', walletId.toString('hex'));

  var self = this;

  for (var i = 0; i < newAddresses.length; i++) {

    // Update the address
    var walletAddress = newAddresses[i];
    txn.putBinary(this.db.addresses, walletAddress.getKey(), walletAddress.getValue());

    // Update the address map
    var key = models.WalletAddressMap.getKey(walletAddress.address, this.network);
    var value = txn.getBinary(this.db.addressesMap, key);
    var addressMap;
    if (value) {
      addressMap = models.WalletAddressMap.fromBuffer(key, value, this.network);
      addressMap.insert(walletId);
    } else {
      addressMap = models.WalletAddressMap.create(walletAddress.address, [walletId], this.network);
    }
    txn.putBinary(this.db.addressesMap, addressMap.getKey(), addressMap.getValue());
  }

  // Update the wallet
  txn.putBinary(this.db.wallets, wallet.getKey(), wallet.getValue());

  // Update the wallet block
  txn.putBinary(this.db.blocks, walletBlock.getKey(), walletBlock.getValue());

  // Commit the changes
  txn.commit();
  this.db.env.sync(function(err) {
    if (err) {
      return callback(err);
    }
    self.walletBlock = walletBlock;
    self.blockFilter = new BlockFilter({
      network: self.network,
      addressFilter: self.walletBlock.addressFilter
    });

    callback();
  });
};

WriterWorker.prototype._filterNewAddresses = function(txn, walletAddresses) {
  var self = this;
  var newAddresses = walletAddresses.filter(function(address) {
    var buffer = txn.getBinary(self.db.addresses, address.getKey());
    if (!buffer) {
      return true;
    } else {
      return false;
    }
  });
  return newAddresses;
};

WriterWorker.prototype._queueSyncTask = function(height, hash) {
  assert(bitcore.util.js.isNaturalNumber(height), '"height" is expected to be a natural number');
  assert(bitcore.util.js.isHexa(hash), '"hash" is expected to be a hexa string');
  assert(hash.length === 64, '"hash" length is expected to be 64');
  var taskId = utils.getTaskId();
  var task = {
    id: taskId,
    method: 'sync',
    params: [{
      bitcoinHeight: height,
      bitcoinHash: hash
    }]
  };
  this._pushTask(task, 0);
};

WriterWorker.prototype._addUTXOSToWallet = function(txn, walletBlock, walletId, newAddresses, callback) {
  var self = this;

  log.info('Adding utxos to wallet: ', walletId.toString('hex'));

  var addresses = newAddresses.map(function(a) {
    return a.address.toString();
  });

  this.clients.getAddressUtxos({
    addresses: addresses,
    chainInfo: true
  }, function(err, response) {
    if (err) {
      return callback(utils.wrapRPCError(err));
    }

    var result = response.result.utxos;

    if (response.result.hash === walletBlock.blockHash.toString('hex')) {
      for (var i = 0; i < result.length; i++) {
        var utxo = result[i];
        var utxoData = {
          height: utxo.height,
          address: utxo.address,
          txid: utxo.txid,
          index: utxo.outputIndex,
          satoshis: utxo.satoshis
        };
        self._addUTXO(txn, walletId, utxoData);
      }
    } else {
      if (response.result.height > walletBlock.height) {

        // Queue a sync task as we need to catch up first
        self._queueSyncTask(response.result.height, response.result.hash);

        // Give back an error with deferrable flag set to try this task once again
        var error = new Error('Unexpected greater chain tip height from bitcoind query');
        error.deferrable = true;
        return callback(error);
      }
      return callback(new Error('Unexpected chain tip hash from address utxos bitcoind query'));
    }

    callback();

  });
};

/**
 * Will import an address and key pair into the wallet and will keep track
 * of the balance and transactions.
 * @param {Array} addresses - Array of base58 encoded addresses
 */
WriterWorker.prototype.importWalletAddresses = function(task, walletId, addresses, callback) {
  /* jshint maxstatements: 25 */
  var self = this;
  walletId = utils.toHexBuffer(walletId);

  if (self.syncing) {
    return callback(new Error('Sync or import in progress'));
  }
  self.syncing = true;

  if (!this.walletBlock) {
    self.syncing = false;
    return callback(new Error('Wallet does not exist, missing wallet block'));
  }
  // Prevent in memory modifications until we know the changes
  // have been persisted to disk.
  var walletBlock = this.walletBlock.clone();

  var txn = this.db.env.beginTxn();

  var buffer = txn.getBinary(this.db.wallets, walletId);
  if (!buffer) {
    self.syncing = false;
    txn.abort();
    return callback(new Error('Wallet does not exist'));
  }

  var wallet = models.Wallet.fromBuffer(walletId, buffer);

  var walletAddresses = addresses.map(function(address) {
    return models.WalletAddress(walletId, address);
  });

  var newAddresses = self._filterNewAddresses(txn, walletAddresses);

  if (newAddresses.length === 0) {
    self.syncing = false;
    txn.abort();
    return callback(null, newAddresses);
  }

  async.series([
    function(next) {
      // TODO If getaddressdeltas response includes prevTxId and prevOut for spending
      // deltas, we will not need to make a seperate RPC call to bitcoind to retrieve
      // the current UTXO state for the addresses.
      self._addUTXOSToWallet(txn, walletBlock, walletId, newAddresses, next);
    },
    function(next) {
      self._addAddressesToWallet(task, txn, walletBlock, walletId, wallet, newAddresses, next);
    },
    function(next) {
      self._commitWalletAddresses(txn, walletBlock, walletId, wallet, newAddresses, next);
    }
  ], function(err) {
    self.syncing = false;
    if (err) {
      txn.abort();
      return callback(err);
    }
    callback(null, newAddresses);
  });
};

/**
 * Saves a transaction to the database
 *
 * @param {Object} transaction - The transaction object (response from verbose getrawtransaction)
 * @param {Function} callback
 */
WriterWorker.prototype.saveTransaction = function(walletId, transaction, callback) {
  var self = this;
  var walletTransaction = models.WalletTransaction.create(walletId, transaction);
  var txn = this.db.env.beginTxn();
  var value = walletTransaction.getValue();
  txn.putBinary(self.db.txs, walletTransaction.getKey(), value);
  txn.commit();

  this.db.env.sync(function(err) {
    if (err) {
      return callback(err);
    }
    callback();
  });
};

/**
 * Creates a new wallet by walletId. If an existing wallet exists with the walletId
 * the existing wallet will be returned as the response.
 *
 * @param {Object} walletObj - Object representing the wallet
 * @param {Function} callback
 */
WriterWorker.prototype.createWallet = function(walletId, callback) {

  var txn = this.db.env.beginTxn();

  // Create the initial wallet block if it doesn't exist
  var walletBlock = this._initWalletBlock();
  if (walletBlock) {
    txn.putBinary(this.db.blocks, walletBlock.getKey(), walletBlock.getValue());
  }

  var wallet = models.Wallet.create(walletId);

  var key = wallet.getKey();
  var buffer = txn.getBinary(this.db.wallets, key);
  if (buffer) {
    txn.abort();
    return callback();
  } else {
    txn.putBinary(this.db.wallets, key, wallet.getValue());
    txn.commit();
  }

  this.db.env.sync(function(err) {
    if (err) {
      return callback(err);
    }
    callback(null, walletId);
  });
};

/* istanbul ignore next */
if (require.main === module) {

  process.title = 'bwdb-writer';

  var options = JSON.parse(process.argv[2]);
  var worker = new WriterWorker(options);
  worker.start(function(err) {
    if (err) {
      throw err;
    }
    process.send('ready');
  });
  process.on('SIGINT', function() {
    worker.stop(function(err) {
      if (err) {
        throw err;
      }
      process.exit(0);
    });
  });
}

module.exports = WriterWorker;
