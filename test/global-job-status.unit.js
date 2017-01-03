'use strict';

var should = require('chai').should();
var GlobalJobStatus = require('../lib/global-job-status.js');

describe('GlobalJobStatus', function() {

  describe('#prune', function() {

    var globalJobStatus;
    var createTime;

    before(function() {
      globalJobStatus = new GlobalJobStatus({maxJobAge: 1});
      globalJobStatus.createNewJob({id: 'abc123', method: 'test task'});
      createTime = globalJobStatus.get('abc123').createtime;
    });

    it('should prune jobs older than the maxJobAge', function(done) {
      globalJobStatus.setResult('abc123');
      Object.keys(globalJobStatus._jobs).length.should.equal(1);
      setTimeout(function(err, res) {
        globalJobStatus.prune(function() {
          Object.keys(globalJobStatus._jobs).length.should.equal(0);
          done();
        });
      }, 1001);
    });

  });

  describe('#update', function() {

    var globalJobStatus;
    var createTime;
    var expected;

    before(function() {
      globalJobStatus = new GlobalJobStatus({maxJobAge: 1});
      globalJobStatus.createNewJob({id: 'abc123', method: 'test task'});
      createTime = globalJobStatus.get('abc123').createtime;
      expected = {
        status: 'running',
        progress: 0,
        createtime: createTime,
        data: '',
        name: 'test name'
      };
      globalJobStatus.update('abc123', expected);
    });

    it('should not set the data value for a non-preexisting job.', function() {
      globalJobStatus.update('doesnotexist').should.equal(false);
    });

    it('should update the data values on a preexisting job.', function() {
      var currentData = globalJobStatus.get('abc123');
      currentData.status.should.equal('running');
      currentData.progress.should.equal(0);
      currentData.data.should.equal('');
      currentData.createtime.should.deep.equal(createTime);
      var newExpected = {
        status: 'weirdstatus',
        progress: 100,
        data: 'somedata'
      };
      globalJobStatus.update('abc123', newExpected);
      var newData = globalJobStatus.get('abc123');
      newData.status.should.equal('weirdstatus');
      newData.progress.should.equal(100);
      newData.data.should.equal('somedata');
      currentData.createtime.should.deep.equal(createTime);
    });
  });

  describe('#get', function() {

    var globalJobStatus;
    var createTime;

    before(function() {
      globalJobStatus = new GlobalJobStatus({maxJobAge: 1});
      globalJobStatus.createNewJob({id: 'abc123', method: 'test task'});
      createTime = globalJobStatus.get('abc123').createtime;
    });

    it('should get the value of a given jobId', function() {
      globalJobStatus.get('abc123').should.deep.equal({
        id: 'abc123',
        status: 'queued',
        progress: 0,
        createtime: createTime,
        name: 'test task'
      });
    });

  });

});
