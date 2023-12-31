var BgBlack, BgBlue, BgCyan, BgGray, BgGreen, BgMagenta, BgRed, BgWhite, BgYellow, Blink, Bright, Dim, FgBlack, FgBlue, FgCyan, FgGray, FgGreen, FgMagenta, FgRed, FgWhite, FgYellow, Hidden, Reset, Reverse, TypeNumber, Underscore, bc, colors, id, types;

import {
  BroadcastChannel,
  isMainThread,
  workerData
} from "worker_threads";

bc = process.log != null ? process.log : process.log = new BroadcastChannel("log");

id = process[isMainThread && "pid" || "ppid"] + ((workerData != null ? workerData.id : void 0) || 0);

colors = [Reset = "\x1b[0m", Bright = "\x1b[1m", Dim = "\x1b[2m", Underscore = "\x1b[4m", Blink = "\x1b[5m", Reverse = "\x1b[7m", Hidden = "\x1b[8m", FgBlack = "\x1b[30m", FgRed = "\x1b[31m", FgGreen = "\x1b[32m", FgYellow = "\x1b[33m", FgBlue = "\x1b[34m", FgMagenta = "\x1b[35m", FgCyan = "\x1b[36m", FgWhite = "\x1b[37m", FgGray = "\x1b[90m", BgBlack = "\x1b[40m", BgRed = "\x1b[41m", BgGreen = "\x1b[42m", BgYellow = "\x1b[43m", BgBlue = "\x1b[44m", BgMagenta = "\x1b[45m", BgCyan = "\x1b[46m", BgWhite = "\x1b[47m", BgGray = "\x1b[100m"];

types = {
  debug: [Reset, Bright, FgGreen, "DEBUG", Reset].join(""),
  event: [Reset, Bright, FgYellow, "EVENT", Reset].join(""),
  error: [Reset, Bright, FgRed, "ERROR", Reset].join(""),
  state: [Reset, Bright, FgCyan, "STATE", Reset].join("")
};

bc.onmessage = function({data}) {
  if (isMainThread && !process.bc) {
    return console.log(new Date().toISOString().substr(-6), `[${types[data[0]]}]`, ...data.slice(1));
  }
};

Object.defineProperties(String.prototype, {
  toNumber: {
    value: function(start = 0, end = this.length, S = 0) {
      while (start < end) {
        S += this.charCodeAt(start++);
      }
      return S;
    }
  }
});

TypeNumber = class TypeNumber extends Number {
  [Symbol.toPrimitive](hint) {
    if (hint !== "string") {
      return this.valueOf();
    }
    return `${this.constructor.name} (${this.valueOf()})`;
  }

};

export default {
  TYPE: global.TYPE = new Proxy({}, {
    get: function(i, type) {
      return eval(`new (class ${type} extends TypeNumber {})(${type.toNumber()})`);
    }
  }),
  log: global.log = new Proxy({}, {
    get: function(i, type) {
      return function() {
        var data;
        try {
          data = [type, id, ...arguments];
        } catch (error) {}
        if (isMainThread) {
          return bc.onmessage({data});
        } else {
          return bc.postMessage(data);
        }
      };
    }
  })
};
