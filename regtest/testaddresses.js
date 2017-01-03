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

var server;
var client;
var config;
var walletInfo;
var configPath = '/home/k/.bwdb';

describe('Import Addresses', function() {

  before(function(done) {
    console.log('This test suite is designed to run as a unit!' +
      ' Individual tests will not run successfully in isolation.');
    this.timeout(600000);

    config = new ClientConfig({path: configPath, network: 'livenet'});

    async.series([
      function(next) {
        rimraf(configPath + '/livenet.lmdb', next);
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

      server = new Server({network: 'livenet', configPath: configPath});

      var syncedHandler = function(height) {
        if (!height || height > -1) {
          server.node.services.bitcoind.removeListener('synced', syncedHandler);
        }

        async.retry({times: 10, interval: 1000}, function(next) {
          client.getInfo(function(err, response) {
            if(err) {
              return next('try again');
            }
            next(null, response);
          });
        }, done);
      };


      server.start(function(err) {
        if (err) {
          return done(err);
        }

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

  function importLivenetAddresses(walletValue, done) {
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
        var delay = 10000;
        var interval = setInterval(function() {
          client.getJobInfo(jobId, function(err, res, json) {
            if(err) {
              clearInterval(interval);
              done(err);
            }
            console.log(json);
            if (json.status === 'complete') {
              clearInterval(interval);
              console.log('Complete.');
            }
            if (json.status === 'error') {
              clearInterval(interval);
              console.log('error', json);
            }
          });
        }, delay);
      });
    });
  }

  it('should import a list of addresses from a javascript object', function(done) {
    this.timeout(10000000000);
    importLivenetAddresses(fs.createReadStream('/home/k/source/bwdb/scripts/files/addresses.json'), done);
    //setTimeout(function() {
    //  done();
    //}, 1000000);
  });

});
