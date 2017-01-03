'use strict';

var async = require('async');
var utils = require('./utils');
var _ = require('lodash');

//this is here to persist long-running jobs and their statuses

function GlobalJobStatus(opts) {
  opts = opts || {};
  this._maxJobAge = opts.maxJobAge || 3600; // in seconds
  this._jobs = {};
  this._maxJobs = 100;
  this._jobsMetaData = {};
}

GlobalJobStatus.prototype.prune = function(callback) {
  var jobIds = Object.keys(this._jobs);
  async.filter(jobIds, this._isPrunable.bind(this), this._delete.bind(this, callback));
};

GlobalJobStatus.prototype._isPrunable = function(id, next) {
  var job = this.get(id);
  if (job && job.expirationtime && job.expirationtime < Date.now()) {
    next(null, true);
  } else {
    next(null, false);
  }
};

GlobalJobStatus.prototype.update = function(jobId, value) {
  var currentData = this.get(jobId);
  if (!_.isPlainObject(value) || !currentData) {
    return false;
  }
  if (currentData.status === 'queued' && value.status === 'running') {
    this._jobsMetaData[jobId]._hrLaunched = process.hrtime();
  }
  _.merge(currentData, value);
};

GlobalJobStatus.prototype._setTimeBasedResults = function(id) {
  var meta = this._jobsMetaData[id];
  var value = this.get(id);
  value.endtime = Date.now();
  value.expirationtime = value.endtime + (this._maxJobAge * 1000);
  if (meta._hrLaunched) {
    value.runtime = utils.diffTime(meta._hrLaunched);
  }
  value.queuedtime = utils.diffTime(meta._hrQueued);
};

GlobalJobStatus.prototype.setResult = function(id, err, result) {
  var value = this.get(id);
  if (!value) {
    return false;
  }
  if (err) {
    value.status = 'error';
    value.message = err.message;
  } else {
    value.status = 'complete';
    value.data = (result || '');
  }
  value.progress = 1;
  this._setTimeBasedResults(id);
  return true;
};

GlobalJobStatus.prototype.createNewJob = function(task) {
  var job = this.get(task.id);
  if (job && job.status !== 'deferred') {
    return null ;
  } else if (job && job.status === 'deferred') {
    job.status = 'queued';
    job.progress = 0;
    return job;
  }
  if (this._jobs.length >= this._maxJobs) {
    return null;
  }
  this._jobs[task.id] = {
    id: task.id,
    status: 'queued',
    progress: 0,
    name: task.method,
    createtime: Date.now()
  };
  this._jobsMetaData[task.id] = {
    _hrQueued: process.hrtime(),
    _hrLaunched: null
  };
  return this._jobs[task.id];
};

GlobalJobStatus.prototype.get = function(jobId) {
  return this._jobs[jobId];
};

GlobalJobStatus.prototype._delete = function(callback, err, ids) {
  if(err) {
    return callback(err);
  }
  for(var i = 0; i < ids.length; i++) {
    delete this._jobsMetaData[ids[i]];
    delete this._jobs[ids[i]];
  }
  callback();
};

module.exports = GlobalJobStatus;
