var vows = require("vows"),
    assert = require("assert"),
    test = require("./test"),
    event = require("../lib/cube/event");

var suite = vows.describe("event");

function build_events(start, stop, put) {
  put = typeof put !== 'undefined' ? put : false;
  events = [];
  if (start < stop) {
    for (var i = start; i <= stop; i++) {
      events.push(build_event(i, put));
    }
  } else {
    for (var i = start; i >= stop; i--) {
      events.push(build_event(i, put));
    }
  }
  return events;
}

function build_event(i, put) {
  put = typeof put !== 'undefined' ? put : false;
  e = {
    id: i,
    time: new Date(Date.UTC(2012, 7, i)),
    data: {i: i}
  };

  if (put) {
    e['type'] = "test";
    e['time'] = e.time.toISOString();
  }
  return e;
}

suite.addBatch(test.batch({
  topic: function(test) {
    var putter = event.putter(test.db),
        getter = event.getter(test.db),
        callback = this.callback;


    for (var i = 0; i < 12; i++) {
      putter(build_event(i, true));
    }

    setTimeout(function() { callback(null, getter); }, 500);
  },

  "there are 12 events": eventTest({
    'expression': 'test(i)',
    'start': new Date(Date.UTC(2012, 7, 0)).toISOString(),
    // don't exclude stop or it will be a streaming GET that doesn't end
    'stop': new Date(Date.UTC(2012, 7, 13)).toISOString(), 
    },
    build_events(11, 0)
  ),

  "get first 5 events": eventTest({
    'expression': 'test(i)',
    'start': new Date(Date.UTC(2012, 7, 0)).toISOString(),
    'stop': new Date(Date.UTC(2012, 7, 5)).toISOString(), 
    },
    build_events(4, 0)
  ),

  "get last 5 events": eventTest({
    'expression': 'test(i)',
    'start': new Date(Date.UTC(2012, 7, 6)).toISOString(),
    'stop': new Date(Date.UTC(2012, 7, 20)).toISOString(), 
    'limit': 5,
    },
    build_events(11, 7)
  ),

  "get 5 events in ascending order": eventTest({
    'expression': 'test(i)',
    'start': new Date(Date.UTC(2012, 7, 6)).toISOString(),
    'stop': new Date(Date.UTC(2012, 7, 20)).toISOString(), 
    'order': 'time',
    'limit': 5,
    },
    build_events(6, 10)
  ),
}));

suite.export(module);

function eventTest(request, expected) {
  var t = get_and_test(expected);
  return t;

  function get_and_test(expected) {
    var start = new Date(request.start),
        stop = new Date(request.stop);

    var test = {
      topic: function() {
        var actual = [],
            timeout = setTimeout(function() { cb("Time's up!"); }, 10000),
            cb = this.callback,
            req = Object.create(request),
            test = arguments[0];
        setTimeout(function() {
          test(req, function(response) {
            if (response == null) {
              clearTimeout(timeout);
              cb(null, actual);
            } else {
              actual.push(response);
            }
          });
        }, 0);
      }
    }
    test['actual'] = function(actual) {
      assert.equal(actual.length, expected.length);

      // each event defines only time, id, and data properties
      actual.forEach(function(d) {
        assert.deepEqual(Object.keys(d), ["id", "time", "data"]);
      });

      for (a in actual) {
        actual_event = actual[a];
        expected_event = expected[a];
        assert.deepEqual(actual_event, expected_event);
      }
    }

    return test;
  }
}
