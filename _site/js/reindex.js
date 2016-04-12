"use strict";

var controller_init_time;

function ReindexController(host, log, table, enable_creds) {

function Index(name, info, state, on_change) {

  this.name = name;
  this.info = info;
  this.state = state || {
    reindex_status : ''
  };
  this.extra = '';

  this.sort_key = function() {
    return [
      this.info.state === 'close' ? '1' : '0',
      _.padStart(1000000 - parseInt(this.info.priority || 0), 7, '0'),
      (2000000000000 - this.info.created_timestamp),
      this.name
    ].join('~')
  };

  this.get_extra = function() {
    return this.extra
      || (this.info.state === 'close' && 'Index is closed')
      || (this.info.health !== 'green' && 'Index is not green')
      || (this.get_reindex_status() === 'error' && this.state.error)
      || '';
  };

  this.set_extra = function(extra) {
    this.extra = extra;
    return on_change(this.name)
  };

  this.get_reindex_status = function() {
    return this.state.reindex_status;
  }

  this.set_reindex_status = function(status) {
    if (status && this.state.reindex_status === 'cancelled') {
      return Promise.resolve();
    }
    if (status === this.state.reindex_status) {
      return Promise.resolve();
    }
    console.log("Setting status `" + status + "`");
    this.state.reindex_status = status;
    return status === 'finished' ? this.del() : this.save();
  }

  this.save = function() {
    var name = this.name;
    return es.put('/' + Index.reindex_index + '/index/' + name, {
      refresh : true
    }, this.state)

    .then(function() {
      return on_change(name)
    });
  };

  this.del = function() {
    var name = this.name;
    return es.del('/' + Index.reindex_index + '/index/' + name, {
      refresh : true
    })

    .then(function() {
      return on_change(name)
    });
  };

  this.status = function() {

    if (this.info.state === 'close') {
      return 'Closed';
    }

    switch (this.info.health) {
    case 'red':
      return 'Red';
    case 'yellow':
      return 'Yellow';
    }

    switch (this.state.reindex_status) {
    case 'queued':
      return 'Queued';
    case 'error':
      return 'Error';
    case '':
      return 'Pending';
    case 'cancelled':
      return 'Cancelled';
    default:
      return 'Reindexing'
    }
  };

  this.action = function() {

    var self = this;

    function cancel() {
      return [
        'Cancel',
        function() {
          var status = self.get_reindex_status();

          console.log("Cancelling status `" + status + "`");

          if (status === 'finished'
            || status === 'src_deleted'
            || status === 'green') {
            console.log("Too late to cancel");
            return;
          }

          if (dequeue(self)) {
            console.log('Found job in queue')
            return self.set_reindex_status('');
          }

          return self.set_reindex_status('cancelled');

        }
      ]
    }

    function reset() {
      return [
        'Reset', function() {
          return new Reindexer(self).reset()
        }
      ]
    }

    function queue() {
      return [
        'Reindex', function() {
          return self.set_reindex_status('queued') //
          .then(function() {
            enqueue(self);
          });
        }
      ];
    }

    var status = this.status();
    switch (status) {
    case 'Closed':
    case 'Red':
    case 'Yellow':
      return;
    case 'Queued':
      return cancel();
    case 'Error':
    case 'Cancelled':
      return reset();
    case 'Reindexing':
      return cancel();
    case 'Pending':
      return queue();
    default:
      throw ("Unknown status: " + status);
    }
  }

}

Index.reindex_index = '.reindex-status';

Index.init_all_indices = function(on_change) {
  var indices = {};
  return Index.init_index()

  .then(function() {
    console.log('Loading index data');
    return es.post('/_refresh')
  })

  .then(
    function() {
      return Promise.all([
        es.get('/_cluster/state/metadata', {
          "filter_path" : "metadata.indices.*.state,"
            + "metadata.indices.*.settings.index.version.created,"
            + "metadata.indices.*.settings.index.creation_date,"
            + "metadata.indices.*.settings.index.number*,"
            + "metadata.indices.*.settings.index.priority"
        }),
        es.get('/_cluster/health', {
          "level" : "indices",
          "filter_path" : "indices.*.status"
        }),
        es.get('/_stats', {
          "human" : true,
          "filter_path" : "indices.*.primaries.docs.count,"
            + "indices.*.primaries.store.size"
        })

      ])
    })

  .then(
    function(d) {

      function format_version(version) {
        return version.substr(0, 1)
          + '.'
          + parseInt(version.substr(1, 2))
          + '.'
          + parseInt(version.substr(3, 2));
      }

      function format_date(timestamp) {
        var date = new Date(parseInt(timestamp));
        return date.getFullYear()
          + '-'
          + _.padStart(date.getMonth(), 2, '0')
          + '-'
          + _.padStart(date.getDay(), 2, '0');
      }

      var state = d[0].metadata.indices;
      var health = d[1].indices;
      var stats = d[2].indices || {};

      _.forEach(state, function(v, k) {

        var version = v.settings.index.version.created;
        if (version >= 2000000) {
          return;
        }

        indices[k] = {
          version : format_version(version),
          state : v.state,
          shards : v.settings.index.number_of_shards,
          replicas : v.settings.index.number_of_replicas,
          created : format_date(v.settings.index.creation_date),
          created_timestamp : v.settings.index.creation_date,
          priority : v.settings.index.priority || '',
          health : health[k] ? health[k].status : '',
          docs : stats[k] ? stats[k].primaries.docs.count : '',
          size : stats[k] ? stats[k].primaries.store.size : ''
        };

      });

      if (_.keys(indices).length === 0) {
        return Promise.resolve({
          docs : []
        });
      }
      return es.post('/' + Index.reindex_index + '/index/_mget', {
        filter_path : "docs._id,docs._source"
      }, {
        ids : _.keys(indices)
      });
    })

  .then(function(r) {
    _.forEach(r.docs, function(v) {
      indices[v._id] = new Index(v._id, indices[v._id], v._source, on_change)
    });
    return indices;
  });
}

Index.init_index = function() {
  var index_name = Index.reindex_index;

  console.log('Creating index: `' + index_name + '`');
  return es.put('/' + index_name, {}, {
    "settings" : {
      "number_of_shards" : 1
    },
    "mappings" : {
      "index" : {
        "properties" : {
          "reindex_status" : {
            "type" : "string",
            "index" : "not_analyzed"
          },
          "task_id" : {
            "type" : "string",
            "index" : "not_analyzed"
          },
          "error" : {
            "enabled" : false
          },
          "aliases" : {
            "enabled" : false
          },
          "replicas" : {
            "enabled" : false
          },
          "refresh" : {
            "enabled" : false
          }

        }
      }
    }
  }) //
  .then(function() {
    console.log('Index `' + index_name + '` created successfully')
  }) //
  .caught(ES_Error, function(e) {
    if (e.type === 'index_already_exists_exception') {
      console.log('Index `' + index_name + '` already exists - skipping')
    } else {
      throw (e)
    }
  });
}
function Indices(wrapper_id) {

  var indices;
  var table;

  function on_change(name) {
    var index = indices[name];
    var row = jQuery('#' + name_to_id(name));
    if (index.get_reindex_status() === 'finished') {
      row.remove()
    } else {
      row.attr('class', index.status());
      row.empty().append(render_row(index));
    }
    table.trigger('update');
  }

  function name_to_id(name) {
    return 'index_' + name.replace(/[. ]/g, '_');
  }

  function init_queue() {
    var queued = [];

    _.forEach(sorted_indices(), function(name) {
      var index = indices[name];
      var status = index.status();
      if (status === 'Reindexing') {
        enqueue(index);
      } else if (status === 'Queued') {
        queued.push(index)
      }
    });

    _.forEach(queued, function(index) {
      enqueue(index)
    });

  }

  function render_table() {
    var headers = [
      'Name',
      'Version',
      'Created',
      'Docs',
      'Size',
      'Shards',
      'Replicas',
      'Status',
      'Action',
      'Info',
    ];

    table = jQuery(

    '<table>'
      + col(headers)
      + '<thead>'
      + '<tr>'
      + th(headers)
      + '</tr>'
      + '</thead>'
      + '<tbody></tbody>'
      + '</table>');

    var tbody = table.find('tbody');
    _.forEach(sorted_indices(), function(name) {
      var index = indices[name];
      var row = jQuery(
        '<tr id="'
          + name_to_id(name)
          + '" class="'
          + index.status()
          + '"></tr>)') //
      .append(render_row(index));
      tbody.append(row);
    });

    jQuery(wrapper_id).empty().append(table);
    table.tablesorter({
      cssAsc : 'asc',
      cssDesc : 'desc'
    });
  }

  function render_row(index) {

    return jQuery(
      td([
        index.name,
        index.info.version,
        index.info.created,
        index.info.docs,
        index.info.size,
        index.info.shards,
        index.info.replicas,
        index.status()
      ])).add(build_action(index.action())).add(td([
      index.get_extra()
    ]));
  }

  function col(cols) {
    var html = '';
    _.forEach(cols, function(v) {
      html += '<col class="' + v + '">'
    });
    return html;
  }

  function th(ths) {
    var html = '';
    _.forEach(ths, function(v) {
      html += '<th>' + v + '</th>'
    });
    return html;
  }

  function td(tds) {
    var html = '';
    _.forEach(tds, function(v) {
      html += '<td>' + v + '</td>'
    });
    return html;
  }

  function build_action(action) {
    if (!action) {
      return '<td></td>'
    }
    var button = jQuery('<button>' + action[0] + '</button>').click(action[1]);
    return jQuery('<td>').append(button);
  }

  function sorted_indices() {
    var sort_keys = {};
    _.forEach(indices, function(v, k) {
      sort_keys[k] = v.sort_key()
    });

    return _.keys(sort_keys).sort(function(a, b) {
      if (sort_keys[a] < sort_keys[b]) {
        return -1
      }
      if (sort_keys[b] < sort_keys[a]) {
        return 1
      }
      return 0;
    })
  }

  return Index.init_all_indices(on_change)

  .then(function(i) {
    indices = i;
    render_table();
    init_queue();
  });

}
function Reindexer(index) {
  var src = index.name;
  var dest = src + "-" + version;

  function create_dest_index() {
    if (index.get_reindex_status() !== 'starting') {
      return Promise.resolve()
    }
    return es.get('/' + src) //
    .then(function(d) {
      d = d[src];

      delete d.warmers;

      index.state.aliases = d.aliases;
      delete d.aliases;

      index.state.refresh = d.settings.index.refresh_interval || '1s';
      index.state.replicas = d.settings.index.number_of_replicas;

      d.settings.index.refresh_interval = -1;
      d.settings.index.number_of_replicas = 0;

      delete d.settings.index.version.created;
      delete d.settings.index.creation_date;
      delete d.settings.index.blocks;

      console.log('Creating index `' + dest + '`');
      return es.put('/' + dest, {}, d)

      .then(function() {
        return index.set_reindex_status('index_created')
      });
    });
  }

  function set_src_read_only() {
    if (index.get_reindex_status() !== 'index_created') {
      return Promise.resolve()
    }
    console.log('Setting index `' + src + '` to read-only');
    return es.put('/' + src + '/_settings', {}, {
      "index.blocks.write" : true
    });
  }

  function start_reindex() {
    if (index.get_reindex_status() !== 'index_created') {
      return Promise.resolve()
    }
    console.log('Starting reindex');
    return es.post('/_reindex', {
      wait_for_completion : false
    }, {
      source : {
        index : src
      },
      dest : {
        index : dest,
        version_type : "external"
      }
    }) //
    .then(function(r) {
      index.state.task_id = r.task;
      return index.set_reindex_status('reindexing');
    });
  }

  function monitor_reindex() {
    if (index.get_reindex_status() !== 'reindexing') {
      return Promise.resolve();
    }
    if (!index.state.task_id) {
      throw ("Index should be reindexing, but there is no task ID");
    }

    return new MonitorTask(index, index.state.task_id).then(function() {
      index.set_reindex_status('reindexed');
      return es.post('/' + dest + '/_refresh');
    });
  }

  function check_success() {
    if (index.get_reindex_status() !== 'reindexed') {
      return Promise.resolve();
    }
    return Promise.all([
      es.get('/' + src + '/_count'), es.get('/' + dest + '/_count')
    ]) //
    .then(
      function(d) {
        if (d[0].count !== d[1].count) {
          throw ('Index `'
            + src
            + '` has `'
            + d[0].count
            + '` docs, but index `'
            + dest
            + '` has `'
            + d[1].count + '` docs');
        }
        console.log('Indices `'
          + src
          + '` and `'
          + dest
          + '` have the same number of docs');
      });
  }

  function finalise_dest() {
    if (index.get_reindex_status() !== 'reindexed') {
      return Promise.resolve();
    }
    var settings = {
      "index.number_of_replicas" : index.state.replicas,
      "index.refresh_interval" : index.state.refresh
    };

    console.log('Adding replicas to index `' + dest + '`');

    return es.put('/' + dest + '/_settings', {}, settings) //
    .then(function() {
      console.log('Waiting for index `' + dest + '` to turn green');
      index.set_extra('Waiting for index `' + dest + '` to turn `green`');
      return new MonitorHealth(index, dest)
    })

  }

  function delete_src() {
    if (index.get_reindex_status() !== 'green') {
      return Promise.resolve();
    }
    console.log('Deleting index `' + src + '`');
    return es.del('/' + src) //
    .then(function() {
      return index.set_reindex_status('src_deleted');
    });
  }

  function add_aliases_to_dest() {
    if (index.get_reindex_status() !== 'src_deleted') {
      return Promise.resolve();
    }
    console.log('Adding aliases to index `' + src + '`');
    var actions = [
      {
        add : {
          index : dest,
          alias : src
        }
      }
    ];

    _.forEach(index.state.aliases, function(v, k) {
      v.index = dest;
      v.alias = k;
      actions.push({
        add : v
      });
    });

    return es.post('/_aliases', {}, {
      actions : actions
    })

    .then(function() {
      return index.set_reindex_status('finished');
    });
  }

  function reset() {
    console.log('Resetting index `' + src + '` and index `' + dest + '`');
    index.set_extra('');
    return Promise
      .all([ //
        es.get('/' + src + '/_count'), //
        es.get('/_cluster/health/' + src, {
          level : "indices"
        })
      //
      ])

      .then(
        function(r) {
          if (r[0].count !== index.info.docs) {
            throw ('Doc count in index `' + src + '` has changed. Not resetting.')
          }

          var health = _.get(r[1], 'indices.' + src + '.status') || 'missing';
          if (health !== 'green') {
            throw ('Health of index `'
              + src
              + '` is `'
              + health
              + '`, not `green`. ' + 'Not resetting.');
          }

          console.log('Setting index `' + src + '` to writable');
        })

      .then(function() {
        return es.put('/' + src + '/_settings', {}, {
          "index.blocks.write" : false
        });
      })

      .then(function() {
        console.log('Deleting index `' + dest + '`');
        return es.del('/' + dest).caught(ES_Error, function(e) {
          if (e.status !== 404) {
            throw (e);
          }
        })
      })

      .lastly(function() {
        index.set_extra('');
        return index.set_reindex_status('');
      })

      .caught(handle_error);
  }

  function handle_error(e) {
    index.state.error = e.toString();
    return index.set_reindex_status('error') //
    .then(function() {
      throw (e)
    });

  }

  function reindex() {

    if (index.get_reindex_status() === 'error') {
      return Promise.reject("Cannot reindex `"
        + src
        + "`. First resolve error: "
        + state.error);
    }

    console.log('Reindexing `' + src + '` to `' + dest + '`');

    return create_dest_index() //
    .then(set_src_read_only) //
    .then(start_reindex) //
    .then(monitor_reindex) //
    .then(check_success) //
    .then(finalise_dest) //
    .then(delete_src) //
    .then(add_aliases_to_dest) //
    .then(function() {
      if (index.get_reindex_status() === 'cancelled') {
        console.log('Reindexing cancelled');
        return reset();
      }
      return console.log('Reindexing completed successfully');
    }) //
    .caught(handle_error);
  }

  return {
    reindex : reindex,
    reset : reset
  }

}
function MonitorTask(index, task_id) {

  var monitor_init_time = controller_init_time;
  var node_id = task_id.split(':')[0];

  function get_task(resolve, reject) {

    function _get_task() {
      if (index.get_reindex_status() === 'cancelled') {
        console.log('Cancelling reindex task: ', task_id);
        resolve();
        return es.post('/_tasks/' + task_id + '/_cancel')
      }

      return es.get('/_tasks/' + task_id, {
        detailed : true,
        nodes : node_id
      }) //
      .then(
        function(r) {
          if (monitor_init_time === controller_init_time && r.nodes[node_id]) {
            var status = r.nodes[node_id].tasks[task_id].status;
            index.set_extra((status.created + status.updated)
              + " / "
              + status.total);
            return Promise.delay(1000).then(_get_task);
          }
          index.set_extra('');
          resolve();
        }) //
      .caught(reject);
    }
    _get_task();
  }
  return new Promise(get_task);
};
function MonitorHealth(index, dest) {

  var monitor_init_time = controller_init_time;

  function wait_for_green(resolve, reject) {

    function _wait_for_green() {
      es.get('/_cluster/health/' + dest, {
        level : 'indices'
      }) //
      .then(
        function(r) {
          if (index.get_reindex_status() === 'cancelled'
            || monitor_init_time !== controller_init_time) {
            resolve();
            return;
          }
          if (r.indices[dest].status === 'green') {
            index.set_extra('');
            index.set_reindex_status('green');
            resolve();
            return;
          }
          return Promise.delay(1000).then(_wait_for_green);
        }) //
      .caught(reject);
    }
    _wait_for_green();
  }

  return new Promise(wait_for_green);

}

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