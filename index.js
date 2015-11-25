var includeAll = require('include-all');
var amqp = require('amqp');
var _ = require('lodash');
var workers = includeAll({
	'dirname': __dirname + '/../../api/workers',
	'filter': /(.+)(\.js|\.coffee|\.ts)$/,
});

module.exports = function(sails){
	if(!sails) return;
	sails.queues = { 'connections':{}, 'workers':{}, 'publish':{} };

	for(var c in sails.config.connections){

		if(sails.config.connections[c].adapter === "amqp-sails"){
			sails.queues.connections[c] = amqp.createConnection(sails.config.connections[c]);

			for(var w in workers){
				if(workers[w].connection === c){
					setQueue(w, sails, sails.config.connections[c], sails.queues.connections[c]);
				}
			}

		}
	}
};


function setQueue(queue, sails, config, conn){
	var worker = workers[queue];
	worker.respawn = worker.respawn !== undefined ? !!(worker.respawn) : true;
	conn.on('ready', function(){
		sails.queues.publish[queue] = { 'send': function (m, o){ return conn.publish(queue, (m || {}), (o || {})); }};
		if(sails.config.globals.workers) global[queue] = sails.queues.publish[queue];

		function setWorker(){
			conn.queue(queue, config.options, function (q) {
				// Catch all messages
				q.bind(worker.bind);
				// Receive messages
				return q.subscribe(worker.config, function (message, headers, deliveryInfo, ack){
					try{ worker.directive(message, headers, deliveryInfo, ack); }
					catch(error){
						console.log("Worker:", queue, "Error:", error);
						if(worker.respawn) setWorker();
					}
				});
			});
		}

		_.times(worker.workers, setWorker);

	});
};