
function DistributedModule(redisclient, pubsubclient) {
    this.redisclient = redisclient;

	this.pubsubclient = pubsubclient;
	
	var Redlock = require('redlock');
	this.redlock = new Redlock(
		[redisclient],
		{
			driftFactor: 0.01,
			retryCount:  3,
			retryDelay:  200
		}
	);
	
	var uuid = require('node-uuid');
	this.id = uuid.v1();
	
	this.subscribedElectionChannels = {};
}

DistributedModule.prototype.mqueue = function (key, array, callback) {
	var multi = this.redisclient.multi();
	var arrayLength = array.length;
	for (var i = 0; i < arrayLength; i++) {
		multi.rpush(key, array[i]);
	}
	multi.exec(function(errors, results) {
		if (callback) callback(error, results);
	});
	this.pubsubclient.publish('queue'+key, this.id, { });
};

DistributedModule.prototype.queue = function (key, value, callback) {
	this.redisclient.rpush(key, value, function(errors, results) {
		if (callback) callback(error, results);
	});
	
	this.pubsubclient.publish('queue'+key, this.id, { });
}

DistributedModule.prototype.dequeue = function dequeue(key, callback, timeout) {
	this.redisclient.lpop(key, timeout, function(errors, result) {
		if (result) callback(result);
		else {
			this.pubsubclient.subscribe('queue'+key, function(msg){
				this.redisclient.lpop(key, timeout, function(errors, result) {
					if (result)  {
						callback(result);
						this.pubsubclient.unsubscribe('leaveBarrier'+key);
					}
				});
			});
		}
	});
}

DistributedModule.prototype.bDequeue = function bDequeue(key, callback, timeout) {
	if (!timeout) timeout = 0;
	this.redisclient.blpop(key, timeout, function(errors, result) {
		callback(result);
	});
}

DistributedModule.prototype.enterBarrier = function enterBarrier(key, size, callback) {
	this.redlock.lock('barrier'+key, 1000).then(function(lock) {
		this.redisclient.incr(key);
		this.redisclient.get(key, function(err, value) {

			if (callback) {
				this.pubsubclient.subscribe('enterBarrier'+key, function(msg){
					if (message.content.size === size) {
						this.pubsubclient.unsubscribe('enterBarrier'+key);
						callback();
					}
				});
				
				this.pubsubclient.publish('enterBarrier'+key, this.id, {
					size : value
				});
			}
			
			lock.unlock();
		});
	});
}

DistributedModule.prototype.leaveBarrier = function leaveBarrier(key, callback) {
	this.redlock.lock('barrier'+key, 1000).then(function(lock) {
		this.redisclient.decr(key);
		this.redisclient.get(key, function(err, value) {
			if (callback) {
				this.pubsubclient.subscribe('leaveBarrier'+key, function(msg){
					if (message.content.size === 0) {
						this.pubsubclient.unsubscribe('leaveBarrier'+key);
						callback();
					}
				});
				
				this.pubsubclient.publish('enterBarrier'+key, this.id, {
					size : value
				});
			}
			
			lock.unlock();
		});
	});
}

DistributedModule.prototype.lock = function lock(key, callback) {
	this.redlock.lock(key, 1000).then(function(lock) {
		callback(lock);
	});
	
}

function appendBallot(key, redisclient, value, id) {
	value.push({
		id : id
	});
	redisclient.set('nodes'+key, value);
	return value;
}

function startLeaderBroadcast(key, pubsubclient, id) {
	return setInterval(function() {
			pubsubclient.publish('ping'+key, id, { });
		}, 1000 * 5);
}

function followTheLeader(leaderInterval, key, redisclient, leaderId, id, onElected) {
	pubsubclient.subscribe('ping'+key, function(msg){
		if (leaderInterval) {
			clearInterval(leaderInterval);
			leaderInterval = null;
		}
		leaderPinged = true;
	});
	
	var leaderPinged = false;
	
	return setInterval(function() {
			if (leaderPinged) {
				leaderPinged = false;
				return;
			}

			lock('leader'+key, function(lock) {
				redisclient.get('nodes'+key, function(err, ballot) {
					if (ballot[0].id === leaderId) ballot.shift();
					
					leaderId = ballot[0].id;
					onElected(leaderId);
					
					redisclient.set('nodes'+key, ballot);
					
					if (leaderId === id) {
						leaderInterval = setInterval(function() {
							pubsubclient.publish('ping'+key, this.id, { });
						}, 1000 * 5);
					}
					
					lock.unlock();
				});
				
			});
		}, 1000 * 15);
}

DistributedModule.prototype.electLeader = function (key, onElected) {
	var leaderInterval = null;
	var followerInterval = null;

	this.redlock.lock('leader'+key, 1000).then(function(lock) {
		this.redisclient.get('nodes'+key, function(err, ballot) {
			if (!ballot) ballot = [];
			
			var ballot = appendBallot(key, this.redisclient, ballot, this.id);

			var leaderId = ballot[0].id;
			
			onElected(leaderId);
			
			if (leaderId === this.id) {
				leaderInterval = startLeaderBroadcast(key, this.pubsubclient, this.id);
			}
			else {
				var leaderPinged = false;
				if (!subscribedElectionChannels[key]) {
					subscribedElectionChannels[key] = [];
					
					subscribedElectionChannels[key].push(leaderInterval);
					
					followerInterval = followTheLeader(leaderInterval, key, this.redisclient, leaderId, this.id, onElected);
					
					subscribedElectionChannels[key].push(followerInterval);
				}
				
			}
		});
		lock.unlock();
	});
};

DistributedModule.prototype.dropFromRace = function (key) {
	if (subscribedElectionChannels[key]) {
		subscribedElectionChannels[key].forEach(function(interval) {
			clearInterval(interval);
		});
		delete subscribedElectionChannels[key];
	}
		
};

module.exports = DistributedModule;