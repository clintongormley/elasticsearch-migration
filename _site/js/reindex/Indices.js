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
