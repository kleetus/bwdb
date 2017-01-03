'use strict';

var AssertionError = require('assert').AssertionError;
var async = require('async');
var chai = require('chai');
var bitcore = require('bitcore-lib');
var BitcoinRPC = require('bitcoind-rpc');
var rimraf = require('rimraf');
var fs = require('fs');
var should = chai.should();
var expect = chai.expect;
var index = require('..');
var testUtils = require('./utils');
var utils = require('../lib/utils');
var Server = index.Server;
var ClientConfig = index.ClientConfig;

var bitcoinClient = new BitcoinRPC({
  protocol: 'http',
  host: '127.0.0.1',
  port: 30331,
  user: 'bitcoin',
  pass: 'local321',
  rejectUnauthorized: false
});
var server;
var client;
var config;
var startingNumOfBlocks = 105;
var walletDatAddresses;
var configPath = __dirname + '/data';
var walletBase = configPath + '/bitcoin/regtest/wallet';
var options = {
  height: 0,
  index: 0,
  limit: 10,
  end: startingNumOfBlocks + 20
};

describe('Import Addresses', function() {

  before(function(done) {
    console.log('This test suite is designed to run as a unit!' +
      ' Individual tests will not run successfully in isolation.');
    this.timeout(60000);

    //configPath = '/home/bwdb/.bwdb';
    config = new ClientConfig({path: configPath, network: 'regtest'});

    async.series([
      function(next) {
        rimraf(configPath + '/bitcoin/regtest', next);
      },
      function(next) {
        rimraf(configPath + '/regtest.lmdb', next);
      },
      function(next) {
        config.setup(function(err) {
          if (err) {
            next(err);
          }
          config.unlockClient(function(err, _client) {
            if (err) {
              next(err);
            }
            client = _client;
            next();
          });
        });
      }
    ], function(err) {
      if (err) {
        return done(err);
      }

      server = new Server({network: 'regtest', configPath: configPath});

      var syncedHandler = function(height) {
        var walletCreated = false;
        server.node.services.bitcoind.removeListener('synced', syncedHandler);
        var localHeight = height || 0;
        async.whilst(function() {
          return localHeight < startingNumOfBlocks
        }, function(cb) {
          setTimeout(function() {
            client.getInfo(function(err, response) {
              if(err) {
                return cb(null, localHeight);
              }
              localHeight = parseInt(response.headers['x-bitcoin-height']);
              if (!walletCreated) {
                testUtils.importWalletDat({
                  client: client,
                  config: config,
                  path: walletBase + '.dat'
                }, function(err, response) {
                  if(err) {
                    return done(err);
                  }
                  walletCreated = true;
                  cb(null, localHeight);
                });
              } else {
                cb(null, localHeight);
              }
            });
          }, 2000);
        }, function(err, res) {
          utils.readWalletFile(walletBase + '.dat',
          'regtest', function(err, addresses) {
            if (err) {
              return done(err);
            }
            walletDatAddresses = addresses;
            fs.writeFile(walletBase + '.json',
            JSON.stringify(walletDatAddresses), function(err) {
              if(err) {
                return done(err);
              }
              done();
            });
          });
        });
      }

      server.start(function(err) {
        if (err) {
          return done(err);
        }

        bitcoinClient.generate(startingNumOfBlocks, function(err) {
          if (err) {
            throw err;
          }
        });

        if (!server.node.services.bitcoind.initiallySynced) {
          server.node.services.bitcoind.on('synced', syncedHandler);
        } else {
          syncedHandler();
        }

      });
    });
  });

  after(function(done) {
    this.timeout(20000);
    server.stop(function(err) {
      if (err) {
        throw err;
      }
      done();
    });
  });

  function importTestAddresses(walletValue, done) {
    testUtils.createWallet({
      client: client
    }, function(err, response) {
      if(err) {
        return done(err);
      }
      var walletId = response.walletId;
      client.importAddresses(walletId, {
        multipart: true,
        value: walletValue
      }, function(err, res, body) {
        if(err) {
          return done(err);
        }
        Object.keys(body).should.deep.equal(['jobId']);
        var jobId = body.jobId;
        async.retry({
          interval: 1000,
          times: 100
        }, function(next, results) {
          client.getJobInfo(walletId, jobId, function(err, res, body) {
            if(err) {
              return next(err);
            }
            if (body.status !== 'complete') {
              return next(body);
            }
            next(null, body);
          });
        }, function(err, result) {
          if(err) {
            return done(err);
          }
          result.data.addresses.should.deep.equal(walletDatAddresses);
          result.status.should.equal('complete');
          result.progress.should.equal(100);
          done();
        });
      });
    });
  }

  it('should import a list of addresses from a javascript object', function(done) {
    this.timeout(8000);
    importTestAddresses(JSON.stringify(walletDatAddresses), done);
  });

  it('should import a list of addresses from a json file', function(done) {
    this.timeout(8000);
    importTestAddresses(fs.createReadStream(walletBase + '.json'), done);
  });

  it('should take a custom readable stream', function(done) {
    this.timeout(8000);
    var Readable = require('stream').Readable;
    var rs = new Readable;
    walletDatAddresses.forEach(function(address) {
      rs.push(JSON.stringify(address) + '\n');
    });
    rs.push(null);
    importTestAddresses(rs, done);
  });

  it('should take addresses from stdin', function(done) {
    this.timeout(8000);
    var stdin = process.stdin;
    var index = 0;
    var interval = setInterval(function() {
      if (index === walletDatAddresses.length) {
        stdin.push(null);
        clearInterval(interval);
      } else {
        stdin.push(JSON.stringify(walletDatAddresses[index++]) + '\n');
      }
    }, 200);
    importTestAddresses(stdin, done);
  });
});
