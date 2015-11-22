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
				console.log(w);
				if(workers[w].connection === c){
					setQueue(c, w, sails, sails.queues.connections[c]);
				}
			}

		}
	}
};


function setQueue(queue, publish, sails, conn){
	var worker = workers[publish];
	var config = sails.config.connections[queue];
	conn.on('ready', function(){
		sails.queues.publish[publish] = { 'send': function (m, o){ return conn.publish(queue, (m || {}), (o || {})); }};
		if(sails.config.globals.workers) global[publish] = sails.queues.publish[publish];

		_.times(worker.workers, function(){
			conn.queue(queue, config.options, function (q) {
				// Catch all messages
				q.bind(worker.bind);
				// Receive messages
				return q.subscribe(worker.config, worker.directive);
			});
		});
	});
};