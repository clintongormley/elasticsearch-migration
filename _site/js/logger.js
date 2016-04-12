"use strict";

function Logger(out_id) {

  var groups = [];
  var out;

  function clear() {
    out = jQuery(out_id);
    out.html('<ul></ul>');
    groups = [];
    return out;
  }

  function start_group(title) {
    title = title.replace(/`([^`]+)`/g, "<code>$1</code>");
    var new_out = jQuery('<li class="group">' + title + '<ul></ul></li>');
    out.append(new_out);
    groups.push(out);
    out = new_out.find('ul');
    return out;
  }

  function end_group(group) {
    group = group || out;
    if (group === out) {
      if (groups.length > 0) {
        out = groups.pop();
      }
    } else {
      while (groups.length > 0) {
        out = groups.pop();
        if (out === group) {
          break;
        }
      }
    }
  }

  function _write(level, msg) {
    msg = msg.replace(/`([^`]+)`/g, "<code>$1</code>");
    out.append('<li class="' + level + '">' + msg + "</li>");
  }

  function err(e) {
    var msg;
    if (_.has(e, 'message')) {
      console.log(e.message, e.stack);
      msg = e.message;
    } else {
      console.log(e);
      msg = e.toString();
    }
    _write('err', msg);
  }

  return {
    clear : clear,
    info : function(m) {
      _write('info', m)
    },
    debug : function(m) {
      _write('debug', m)
    },
    warn : function(m) {
      _write('warn', m)
    },
    err : err,
    start_group : start_group,
    end_group : end_group
  };
}
