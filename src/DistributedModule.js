
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

DistributedModule.prototype.dequeue = function dequeue(key, callback) {
	this.redisclient.lpop(key, function(errors, result) {
		if (result) callback(result);
		else {
			this.pubsubclient.subscribe('queue'+key, function(msg){
				this.redisclient.lpop(key, function(errors, result) {
					if (result)  {
						callback(result);
						this.pubsubclient.unsubscribe('queue'+key);
					}
				});
			});
		}
	});
}
/*
DistributedModule.prototype.bDequeue = function bDequeue(key, callback, timeout) {
	if (!timeout) timeout = 0;
	this.redisclient.blpop(key, timeout, function(errors, result) {
		callback(result);
	});
}
*/
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
				
				this.pubsubclient.publish('leaveBarrier'+key, this.id, {
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
	redisclient.set('ballot'+key, value);
	return value;
}

function startLeaderBroadcast(key, pubsubclient, id) {
	return setInterval(function() {
			pubsubclient.publish('ping'+key, id, { });
		}, 1000 * 5);
}

function followTheLeader(leaderInterval, key, redisclient, leaderId, id, onElected) {
	var leaderPinged = false;
	
	pubsubclient.subscribe('ping'+key, function(msg){
		if (leaderInterval) {
			clearInterval(leaderInterval);
			leaderInterval = null;
		}
		leaderPinged = true;
	});
	
	return setInterval(function() {
			if (leaderPinged) {
				leaderPinged = false;
				return;
			}

			lock('leader'+key, function(lock) {
				redisclient.get('ballot'+key, function(err, ballot) {
					if (ballot[0].id === leaderId) ballot.shift();
					
					leaderId = ballot[0].id;
					onElected(leaderId);
					
					redisclient.set('ballot'+key, ballot);
					
					if (leaderId === id) {
						// not passed by reference; needs to be fixed
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
		this.redisclient.get('ballot'+key, function(err, ballot) {
			if (!ballot) ballot = [];
			
			var ballot = appendBallot(key, this.redisclient, ballot, this.id);

			var leaderId = ballot[0].id;
			
			onElected(leaderId);
			
			if (leaderId === this.id) {
				leaderInterval = startLeaderBroadcast(key, this.pubsubclient, this.id);
			}
			else {
				var leaderPinged = false;
				if (!this.subscribedElectionChannels[key]) {
					this.subscribedElectionChannels[key] = [];
					
					this.subscribedElectionChannels[key].push(leaderInterval);
					
					followerInterval = followTheLeader(leaderInterval, key, this.redisclient, leaderId, this.id, onElected);
					
					this.subscribedElectionChannels[key].push(followerInterval);
				}
				
			}
			lock.unlock();
		});
		
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

DistributedModule.prototype.commit = function (key, canCommit, commit, rollback, onComplete) {
	lock('leader'+key, function(lock) {
		this.redisclient.get('cohorts'+key, function(err, cohorts) {			
			if (!cohorts) cohorts = {};
			
			var state = 'waiting';
			cohorts[this.id] = {
				state : state
			};
			
			this.redisclient.set('cohorts'+key, cohorts);
			
			var coordinatorId = null;
						
			pubsubclient.subscribe('CommitTx'+key+id, function(msg) {
				if (msg.content.command === 'completed') {
					pubsubclient.unsubscribe('CommitTx'+key+id);
					onComplete(msg.content.status);
					return;
				}
				
				if (state === 'waiting' && msg.content.command === 'query to commit') {
					coordinatorId = msg.id;
					if (canCommit()) {
						state = 'preparing'
						// reply yes
						this.pubsubclient.publish('CommitTx'+key+coordinatorId, this.id, { reply : 'yes' });
					} else {
						state = 'aborted'
						// reply no
						this.pubsubclient.publish('CommitTx'+key+coordinatorId, this.id, { reply : 'no' });
					}
				}
				else if (state === 'preparing') {
					if (msg.content.command === 'commit') {
						commit(function () {
							state = 'committed'
							//ack
							this.pubsubclient.publish('CommitTx'+key+coordinatorId, this.id, { reply : 'ack' });
							//pubsubclient.unsubscribe('CommitTx'+key+id)
						})
					} else if (msg.content.command === 'abort') {
						rollback(function() {
							state = 'aborted'
							// ack
							this.pubsubclient.publish('CommitTx'+key+coordinatorId, this.id, { reply : 'ack' });
							//pubsubclient.unsubscribe('CommitTx'+key+id)
						});
					}
				}
			});
			
			lock.unlock();
		});
				
	});
}

function publishToCohorts(key, id, cohorts, pubsubclient, message) {
	for(var propertyName in cohorts) {
	   var cohort = cohorts[propertyName];
	   pubsubclient.publish('CommitTx'+key+propertyName, id, message);
	} 
}

DistributedModule.prototype.startTransaction = function (key, onComplete) {
	this.redisclient.get('cohorts'+key, function(err, cohorts) {			
		if (!cohorts) return;
		
		publishToCohorts(key, id, cohorts, this.pubsubclient, { command : 'query to commit' });
		
		var status = 'Okay';
		setTimeout(function() {
			if (state === 'completed') return;
			this.pubsubclient.unsubscribe('CommitTx'+key+id);
			onComplete(status);
			// Send complete message
			publishToCohorts(key, id, cohorts, this.pubsubclient, { command : 'completed', status : status });
		}, 1000 * 20);
		
		this.pubsubclient.subscribe('CommitTx'+key+id, function(msg) {
			var id = msg.id;
			if (!cohorts[id]) return;
			
			var msgType = msg.content.type;
			
			var state = 'querying'
			
			if (state === 'querying' && msgType === 'query to commit reply') {
				cohorts[id]['query commit reply'] = msg.content.reply;
				
				var allYes = true;
				var anyNo = false;
				for(var propertyName in cohorts) {
				   var cohort = cohorts[propertyName];
				   if (!cohorts[id]['query commit reply']) {
					   allYes = false;
				   } 
				   if (cohorts[id]['query commit reply'] === 'no') {
					   anyNo = true;
				   }
				}
				
				if (anyNo) {
					// send abort
					publishToCohorts(key, id, cohorts, this.pubsubclient, { command : 'abort' });
					status = 'Failed'
					state = 'waiting for ack'
				} else if (allYes) {
					// send commit
					publishToCohorts(key, id, cohorts, this.pubsubclient, { command : 'commit' });
					state = 'waiting for ack'
				}
				
			} else (state === 'waiting for ack' && msgType === 'acknowledgement') {
				cohorts[id]['acknowledged'] = true;
				
				var allAck = true;
				for(var propertyName in cohorts) {
				   var cohort = cohorts[propertyName];
				   if (!cohorts[propertyName]['acknowledged']) allAck = false;
				}
				
				if (allAck && state != 'completed') { 
					this.pubsubclient.unsubscribe('CommitTx'+key+id);
					onComplete(status);
					state = 'completed';
					// Send complete message
					publishToCohorts(key, id, cohorts, this.pubsubclient, { command : 'completed', status : status });
				}
			}
			
		});
		
	});
			
}

module.exports = DistributedModule;