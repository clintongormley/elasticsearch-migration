"use strict";

var controller_init_time;

function ReindexController(host, log, table, enable_creds) {

  require('Index.js');
  require('Indices.js');
  require('Reindexer.js');
  require('MonitorTask.js');
  require('MonitorHealth.js');

  var es = new Client(host, enable_creds);
  var version;
  var current;
  var queue = [];

  function enqueue(index) {
    queue.push(index);
    reindex_next();
  }

  function dequeue(index) {
    var found = _.remove(queue, function(el) {
      return el.name === index.name
    });
    return found.length > 0;
  }

  function reindex_next() {
    var reindex_init_time = controller_init_time;

    if (current) {
      return;
    }

    function _next() {
      if (reindex_init_time !== controller_init_time) {
        return;
      }

      current = queue.shift();
      if (current) {
        Promise.resolve().then(function() {
          if (current.get_reindex_status() === 'queued') {
            return current.set_reindex_status('starting')
          }
        })

        .then(function() {
          return new Reindexer(current).reindex()
        })

        .caught(show_error).lastly(function() {
          current = undefined;
        })

        .delay(0).then(_next)
      }
    }

    _next();
  }

  function show_error(e) {
    jQuery(log).empty().html(e);
    throw (e);
  }

  controller_init_time = Date.now();

  jQuery(log).empty();
  jQuery(table).empty();

  console.log('Connecting to: ' + host);

  es.get_version().then(function(v) {
    version = v;
    if (version.lt('2.3.*') || version.gt('2.*')) {
      throw ('This plugin only works with Elasticsearch versions 2.3.0 - 2.x')
    }
    return new Indices(table);
  }).caught(show_error);
}
