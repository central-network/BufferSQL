var CreateTableEvent, TableStore;

import "./debug.js";

import {
  EventEmitter
} from "events";

CreateTableEvent = class CreateTableEvent extends CustomEvent {
  constructor(header) {
    super(BufferSQLServer.prototype.EVENT_TABLE_CREATE, {
      detail: header
    });
  }

};

TableStore = (function() {
  class TableStore extends Uint8Array {};

  Object.defineProperties(TableStore.prototype, {
    columnNames: {
      get: function() {
        return this.headers.columns.slice().map(function(c) {
          return c.name;
        });
      }
    },
    hasColumn: {
      value: function() {
        return this.columnNames.includes(arguments[0]);
      }
    }
  });

  return TableStore;

}).call(this);

export var BufferSQLServer = (function() {
  class BufferSQLServer extends EventEmitter {
    constructor(options = {}) {
      if (!options.port) {
        log.debug("No listening port defined. SQL server running on isolated mode.");
        options.opMode = BufferSQLServer.defaultOPMode;
      }
      if (!options.byteLength) {
        log.debug("No initial byteLength parameter defined. Default size (100Mb) will be used.");
        options.byteLength = BufferSQLServer.defaultByteLength;
      }
      if (!options.engine) {
        log.debug(`No buffer type defined. Default endine (${BufferSQLServer.defaultBufferType.name}) will be used.`);
        options.bufferType = BufferSQLServer.defaultBufferType;
      }
      super().options = options;
      this.buffer = new this.options.bufferType(this.options.byteLength);
      this.uInt8Array = new Uint8Array(this.buffer);
      this.uInt32Array = new Uint32Array(this.buffer);
      this.headersArray = new Array();
      log.event("Buffer SQL server is running.");
      Atomics.or(this.uInt32Array, this.TINDEX_DATAOFFSET, 12);
      Atomics.or(this.uInt32Array, this.TINDEX_BYTEOFFSET, this.buffer.byteLength);
      Atomics.or(this.uInt32Array, this.TINDEX_TYPEOFFSET, TYPE.BufferSQLDatabase);
    }

    malloc(byteLength = this.MALLOC_BYTELENGTH, type = this.TYPEOF_TABLE) {
      var allocLength, mod, offset, tindex;
      allocLength = byteLength + 12;
      if (mod = allocLength % 4) {
        allocLength = 4 - mod;
      }
      offset = Atomics.add(this.uInt32Array, this.TINDEX_DATAOFFSET, allocLength);
      tindex = offset / 4;
      Atomics.store(this.uInt32Array, tindex, type);
      Atomics.store(this.uInt32Array, tindex + 1, byteLength);
      return offset + 12;
    }

    query(sql) {
      var fn, query, type;
      [type, ...query] = sql.trim().split(" ");
      if (!(fn = this[type.toLowerCase()])) {
        throw [TYPE.NO_DEFINED_METHOD, type];
      }
      return fn.call(this, query.join(" "));
    }

    create(sql) {
      var fn, query, type;
      [type, ...query] = sql.trim().split(" ");
      if (!/table/.test(fn = type.toLowerCase())) {
        throw [TYPE.NO_DEFINED_METHOD, type];
      }
      switch (fn) {
        case "table":
          return this.createTable(query.join(" "));
      }
    }

    select(sql) {
      var column, columns, i, len, split, table;
      split = sql.split(/FROM/i, 2).filter(Boolean);
      table = this.getTable(split[1].trim());
      if (!(columns = split[0].replace(/\s|\*/g, ""))) {
        columns = table.columnNames;
      } else {
        columns = columns.split(/\,/g);
      }
      for (i = 0, len = columns.length; i < len; i++) {
        column = columns[i];
        if (!table.hasColumn(column)) {
          throw [TYPE.TABLE_HAS_NOT_COLUMN, column];
        }
      }
      return {table, columns};
    }

    encodeText(text) {
      return this.textEncoder.encode(text);
    }

    encodeJSON(object) {
      return this.textEncoder.encode(JSON.stringify(object));
    }

    createTable(query, options = {}) {
      var allocIndex, byteLength, byteOffset, column, columns, i, len, name, offset, size, storeIndex, stride, table, tableHeaders, type;
      [table] = query.split(/\s|\(/g).filter(Boolean);
      [...columns] = query.substring(query.indexOf("(") + 1, query.lastIndexOf(")")).trim().split(/\,/g);
      columns = (function() {
        var i, len, results;
        results = [];
        for (i = 0, len = columns.length; i < len; i++) {
          column = columns[i];
          [name, type, ...size] = column.split(/\s|\(|\)/g).filter(Boolean);
          type = type.toLowerCase();
          size = this.getColumnByteLength(type, size.join(" "));
          results.push({name, type, size});
        }
        return results;
      }).call(this);
      stride = columns.slice().map(function(d) {
        return d.size;
      }).reduce(function(d, e) {
        return d + e;
      });
      offset = 0;
      for (i = 0, len = columns.length; i < len; i++) {
        column = columns[i];
        column.begin = offset;
        column.end = offset += column.size;
      }
      options = {...this._tableOptions, ...options};
      byteLength = options.rowLength * stride;
      byteOffset = this.malloc(byteLength);
      storeIndex = byteOffset / 4;
      allocIndex = storeIndex - 1;
      this.headersArray.push(tableHeaders = {table, columns, stride, byteLength, byteOffset, storeIndex, allocIndex, ...options});
      this.emit(this.EVENT_TABLE_CREATE, new CreateTableEvent({
        table: table = this.getTable(table),
        headers: tableHeaders,
        database: this
      }));
      return table;
    }

    getTable(table) {
      var headers, i, len, ref;
      ref = this.headersArray;
      for (i = 0, len = ref.length; i < len; i++) {
        headers = ref[i];
        if (headers.table !== table) {
          continue;
        }
        return Object.assign(new TableStore(this.buffer, headers.byteOffset, headers.byteLength), {headers});
      }
      throw [TYPE.TABLE_CANT_FOUND_ON_THIS_DATABASE, table];
    }

    getColumnByteLength(type, options) {
      switch (type) {
        case "int":
          return parseFloat(options || 4);
        case "varchar":
          return parseFloat(options || 255);
        default:
          throw TYPE.TYPE_IS_UNDEFINED_FOR_FIND_OPTIONS;
      }
    }

  };

  BufferSQLServer.MODE_ISOLATED = TYPE.MODE_ISOLATED;

  BufferSQLServer.defaultByteLength = 1e8;

  BufferSQLServer.defaultBufferType = typeof SharedArrayBuffer !== "undefined" && SharedArrayBuffer !== null ? SharedArrayBuffer : ArrayBuffer;

  BufferSQLServer.defaultOPMode = BufferSQLServer.MODE_ISOLATED;

  BufferSQLServer.prototype._maxListeners = 11;

  BufferSQLServer.prototype._tableOptions = {
    rowLength: 1e5
  };

  BufferSQLServer.prototype.TINDEX_TYPEOFFSET = 0;

  BufferSQLServer.prototype.TINDEX_BYTEOFFSET = 1;

  BufferSQLServer.prototype.TINDEX_DATAOFFSET = 2;

  BufferSQLServer.prototype.MALLOC_BYTELENGTH = 1e6;

  BufferSQLServer.prototype.TYPEOF_TABLE = TYPE.TABLE;

  BufferSQLServer.prototype.EVENT_TABLE_CREATE = TYPE.TABLE_CREATE_EVENT;

  BufferSQLServer.prototype.textEncoder = new TextEncoder();

  return BufferSQLServer;

}).call(this);
