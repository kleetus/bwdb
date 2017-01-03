'use strict';

var assert = require('assert');
var EventEmitter = require('events').EventEmitter;
var FormData = require('form-data');
var http = require('http');
var https = require('https');
var inherits = require('util').inherits;
var querystring = require('querystring');
var url = require('url');

var bitcore = require('bitcore-lib');
var secp = require('secp256k1');

var CSVStream = require('./streams/csv');
var ListStream = require('./streams/list');
var RawTransactionsStream = require('./streams/rawtransactions');
var TransactionsStream = require('./streams/transactions');
var TxidsStream = require('./streams/txids');
var db = require('./db');
var utils = require('../utils');
var version = require('../../package.json').version;

function Client(options) {
  if (!(this instanceof Client)) {
    return new Client(options);
  }
  if (!options) {
    options = {};
  }
  this.saveKnownHostHandler = options.saveKnownHostHandler || false;
  this.knownHosts = options.knownHosts || false;
  if (options.apiPrivateKey && !Buffer.isBuffer(options.apiPrivateKey)) {
    this.apiPrivateKey = new Buffer(options.apiPrivateKey, 'hex');
  } else {
    this.apiPrivateKey = options.apiPrivateKey || false;
  }
  if (options.apiPublicKey && !Buffer.isBuffer(options.apiPublicKey)) {
    this.apiPublicKey = new Buffer(options.apiPublicKey, 'hex');
  } else {
    this.apiPublicKey = options.apiPublicKey || false;
  }
  this.network = bitcore.Networks.get(options.network);
  this.url = options.url;
  assert(this.network, 'Network is expected.');
  assert(this.url, 'Url is expected.');
  this.bitcoinHeight = null;
  this.bitcoinHash = null;
  this.socket = null;
  this.db = null;
}
inherits(Client, EventEmitter);


Client.prototype.disconnect = function() {
  if (this.db) {
    db.close(this.db);
  }
};

Client.prototype._maybeCallback = function(callback, err) {
  if (callback) {
    return callback(err);
  }
  if (err) {
    this.emit('error', err);
  }
};

Client.prototype.getNetworkName = function() {
  var network = this.network.name;
  if (this.network.regtestEnabled) {
    network = 'regtest';
  }
  return network;
};


Client.prototype._getResponseError = function(res, body) {
  var err = null;
  if (res.statusCode === 404) {
    err = new Error('404 Not Found');
    err.statusCode = 404;
  } else if (res.statusCode === 400) {
    err = new Error('400 Bad Request: ' + body);
    err.statusCode = 400;
  } else if (res.statusCode === 401) {
    err = new Error('401 Unauthorized: ' + body);
    err.statusCode = 401;
  } else if (res.statusCode >= 500) {
    err = new Error(res.statusCode + ' Server Error: ' + body);
    err.statusCode = res.statusCode;
  } else if (res.headers['x-bitcoin-network']) {
    var serverNetwork = res.headers['x-bitcoin-network'];
    if (this.getNetworkName() !== serverNetwork) {
      err = new Error('Network mismatch, server network is: ' + serverNetwork);
    }
  }
  return err;
};


Client.prototype._signRequest = function(options) {

  var self = this;

  if (self.apiPublicKey) {
    var nonce = utils.generateNonce();
    var fullUrl = options.url + options.path;
    var hashedData = utils.generateHashForRequest(options.method, fullUrl, nonce);
    var sigObj = secp.sign(hashedData, self.apiPrivateKey);
    var signatureExport = secp.signatureExport(sigObj.signature);

    options.headers['x-identity'] = self.apiPublicKey.toString('hex');
    options.headers['x-signature'] = signatureExport.toString('hex');
    options.headers['x-nonce'] = nonce.toString('hex');
  }

  if (self.knownHosts) {
    options.ca = self.knownHosts;
  } else {
    options.rejectUnauthorized = false;
  }

};

Client.prototype._isMultipart = function(options) {
  return options && options.multipart && options.value;
};

Client.prototype._request = function(method, endpoint, params, callback) {
  var self = this;

  var options = self._processOptions(method, endpoint, params);

  self._signRequest(options.httpOptions);

  options.otherOptions.called = false;

  var req = (options.isTLS ? https : http).request(options.httpOptions,
    function(res) {
     self._processResponse({
       res: res,
       options: options.otherOptions
     }, callback);
  });

  req.on('error', function(e) {
    if (!options.otherOptions.called) {
      options.otherOptions.called = true;
      callback(e);
    }
  });

  if (self._isMultipart(options.otherOptions.params)) {
    options.otherOptions.form.pipe(req);
  } else {
    req.write(options.httpOptions.body);
    req.end();
  }

};

Client.prototype._processResponse = function(opts, callback) {

  var self = this;
  var body = '';
  var res = opts.res;
  var options = opts.options;
  var isTLS = options.isTLS;
  var called  = options.called;

  res.setEncoding('utf8');

  var certificate = false;

  if (isTLS) {
    certificate = res.socket.getPeerCertificate(false);
  }

  res.on('data', function(chunk) {
    body += chunk;
  });

  function finish(err) {
    if (err) {
      return callback(err);
    }
    err = self._getResponseError(res, body);
    if (err) {
      return callback(err);
    }
    var json;
    if (body) {
      try {
        json = JSON.parse(body);
      } catch(e) {
        return callback(e);
      }
    }
    self.bitcoinHeight = parseInt(res.headers['x-bitcoin-height']);
    self.bitcoinash = res.headers['x-bitcoin-hash'];
    callback(err, res, json);
  }

  res.on('end', function() {
    if (!called) {
      called = true;

      if (certificate && certificate.fingerprint &&
        self.saveKnownHostHandler) {
        self.saveKnownHostHandler(certificate, finish);
      } else {
        finish();
      }
    }
  });
};

Client.prototype._processOptions = function(method, endpoint, params) {

  var self = this;
  var parsedUrl = url.parse(self.url);

  var httpOptions = {
    protocol: parsedUrl.protocol,
    hostname: parsedUrl.hostname,
    port: parsedUrl.port || 80,
    method: method,
    url: self.url,
    path: endpoint,
    json: true,
    headers: {},
    body: ''
  };

  var otherOptions = { params: params };
  Client.prototype._mergeRequestTypeOptions(httpOptions, otherOptions);

  httpOptions.headers['user-agent'] = 'bwdb-' + version;

  return {
    httpOptions: httpOptions,
    otherOptions: otherOptions
  };
};

Client.prototype._mergeRequestTypeOptions = function(httpOptions, otherOptions) {
  var self = this;
  otherOptions.isTLS = (httpOptions.protocol === 'https:');

  if (otherOptions.params && httpOptions.method.toUpperCase() === 'GET') {
    httpOptions.path += '?' + querystring.stringify(otherOptions.params);
  } else if (otherOptions.params) {
    httpOptions.headers['content-type'] = 'application/json';
    httpOptions.body = otherOptions.params;
  }
  self._mergeMultipartOptions(httpOptions, otherOptions);
};

Client.prototype._mergeMultipartOptions = function(httpOptions, otherOptions) {

  var self = this;

  if (self._isMultipart(httpOptions.body)) {
    otherOptions.form = new FormData();
    var data = httpOptions.body.value;
    var CRLF = '\r\n';
    var formOptions = {
      header: CRLF + '--' + otherOptions.form.getBoundary() + CRLF +
        'Content-Disposition: form-data; name="addresses"; filename="nofile.json;' +
        CRLF + CRLF,
      knownLength: data.length
    };
    otherOptions.form.append('addresses', data, formOptions);
    httpOptions.headers = otherOptions.form.getHeaders();
  } else {
    httpOptions['content-length'] = Buffer.byteLength(new Buffer(JSON.stringify(otherOptions.params) || 0));
  }

};

Client.prototype._put = function(endpoint, callback) {
  this._request('PUT', endpoint, false, callback);
};

Client.prototype._get = function(endpoint, params, callback) {
  this._request('GET', endpoint, params, callback);
};

Client.prototype._post = function(endpoint, body, callback) {
  this._request('POST', endpoint, body, callback);
};

/**
 * TODO
 * - have an option for a watch only wallet (no encryption needed)
 * - for spending wallets, create a new secret for wallet
 * - encrypt that secret with the hash of a passphrase
 * - store that secret to encrypt all private keys for the wallet
 * - be able to define the type of wallet: non-hd, hd(bip44), hd(bip45)
 */
Client.prototype.createWallet = function(walletId, callback) {
  this._put('/wallets/' + walletId, callback);
};

Client.prototype.importAddress = function(walletId, address, callback) {
  this._put('/wallets/' + walletId + '/addresses/' + address, callback);
};

Client.prototype.importAddresses = function(walletId, body, callback) {
  this._post('/wallets/' + walletId + '/addresses', body, callback);
};

Client.prototype.getTransactions = function(walletId, options, callback) {
  this._get('/wallets/' + walletId + '/transactions', options, callback);
};

Client.prototype.getUTXOs = function(walletId, options, callback) {
  this._get('/wallets/' + walletId + '/utxos', options, callback);
};

Client.prototype.getTxids = function(walletId, options, callback) {
  this._get('/wallets/' + walletId + '/txids', options, callback);
};

Client.prototype.getBalance = function(walletId, callback) {
  this._get('/wallets/' + walletId + '/balance', {}, callback);
};

Client.prototype.getJobInfo = function(jobId, callback) {
  this._get('/jobs/' + jobId, {}, callback);
};

Client.prototype.getInfo = function(callback) {
  this._get('/info', {}, callback);
};

Client.prototype.getHeightsFromTimestamps = function(options, callback) {
  this._get('/info/timestamps', options, callback);
};

Client.TransactionsStream = TransactionsStream;
Client.prototype.getTransactionsStream = function(walletId, options) {
  options.client = this;
  var stream = new TransactionsStream(walletId, options);
  return stream;
};

Client.RawTransactionsStream = RawTransactionsStream;
Client.prototype.getRawTransactionsStream = function(walletId, options) {
  options.client = this;
  var stream = new RawTransactionsStream(walletId, options);
  return stream;
};

Client.TxidsStream = TxidsStream;
Client.prototype.getTxidsStream = function(walletId, options) {
  options.client = this;
  var stream = new TxidsStream(walletId, options);
  return stream;
};

Client.CSVStream = CSVStream;
Client.prototype.getTransactionsCSVStream = function(walletId, options) {
  options.client = this;
  var stream = new CSVStream(walletId, options);
  return stream;
};

Client.ListStream = ListStream;
Client.prototype.getTransactionsListStream = function(walletId, options) {
  options.client = this;
  var stream = new ListStream(walletId, options);
  return stream;
};

module.exports = Client;
