var assert = require('assert');
var module1 = null;
var module2 = null;

describe('ChannelRegistar', function() {	
	beforeEach(function() {	
		var redis = require('redis');
		var config = require(__dirname+'/../../../Config');
		var redisclient = redis.createClient(config.redis.port, config.redis.host);
		var pubsubclient = require(__dirname+'/../../../PubSubClient/PubSubClient');
		pubsubclient.init(redis, config, function() {
			
		});
		var DistributedModuleClass = require(__dirname+'/../DistributedModule');
		module1 = new DistributedModuleClass(redisclient, pubsubclient);
		module2 = new DistributedModuleClass(redisclient, pubsubclient);
	});
	
	before(function() {
	});

	describe('#queue()', function () {
		it('Queues and dequeues an item', function (done) {
			var key = 'key'
			var value = 'value'
			
			module2.dequeue(key, function(result) {
				assert.equal(result, value);
				done();
			})
			
			module1.queue(key, value, function(err, result) {
				assert.equal(err, null);
				assert.equal(result != null, true);
			});
			
		});
	});
	
	describe('#enterBarrier()', function () {
		it('Enters a and leaves a barrier', function (done) {
			var key = 'barrier'
			
			module2.enterBarrier(key, 1, function() {
				module2.leaveBarrier(key, function() {
					done();
				});
			});

		});
	});
	
	describe('#lock()', function() {
		it('Hold a lock', function(done) {
			var key = 'lock'
			
			module1.lock(key, function(lock) {
				module2.lock(key, function(lock1) {
					assert.equal(false, true);
					lock1.unlock();				
				});
				lock.unlock();
				done();
			})	
		})
	});
	
	
});