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
    byteStride: {
      get: function() {
        return this.headers.stride;
      }
    },
    index: {
      get: function() {
        return this.headers.storeIndex;
      }
    },
    columnsField: {
      get: function() {
        return `(${this.columnNames})`;
      }
    },
    hasColumn: {
      value: function() {
        return this.columnNames.includes(arguments[0]);
      }
    },
    getColumn: {
      value: function(columnName) {
        var column;
        if (!(column = this.headers.columns.find(function(c) {
          return c.name === columnName;
        }))) {
          throw [TYPE.THERE_IS_NO_COLUMN_NAMED_WITH, columnName];
        }
        return column;
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
      var column, columnName, columnNames, columnValue, columns, condition, j, k, len, len1, limit, matchs, offset, operator, parsedValue, ref, searchOffset, split, stride, table, value;
      split = sql.split(/FROM/i, 2).filter(Boolean);
      table = this.getTable(split[1].split(/where/i, 1)[0].trim());
      matchs = [];
      offset = table.offset();
      stride = table.byteStride;
      if (!(columns = split[0].replace(/\s|\*/g, ""))) {
        columnNames = table.columnNames;
      } else {
        columnNames = columns.split(/\,/g);
      }
      for (j = 0, len = columnNames.length; j < len; j++) {
        columnName = columnNames[j];
        if (!table.hasColumn(columnName)) {
          throw [TYPE.TABLE_HAS_NOT_COLUMN, columnName];
        }
      }
      ref = sql.split(/where/i)[1].split(/and|or/i);
      for (k = 0, len1 = ref.length; k < len1; k++) {
        condition = ref[k];
        [columnName, value] = condition.split(/>=|<=|>|<|=|<>|\!=|=/).map(function(c) {
          return c.trim();
        });
        if (!table.hasColumn(columnName)) {
          throw [TYPE.TABLE_HAS_NOT_COLUMN, columnNames];
        }
        limit = offset;
        column = table.getColumn(columnName);
        operator = condition.match(/(>=|<=|>|<|=|<>|\!=|=)/)[0];
        parsedValue = column.parse(value);
        searchOffset = 0;
        while (searchOffset < limit) {
          columnValue = column.decode(table, searchOffset);
          switch (operator) {
            case ">=":
              if (columnValue >= parsedValue) {
                matchs.push(searchOffset);
              }
          }
          searchOffset += stride;
        }
      }
      return matchs;
    }

    insert(sql) {
      var fn, query, type;
      [type, ...query] = sql.trim().split(" ");
      if (!/into/.test(fn = type.toLowerCase())) {
        throw [TYPE.NO_DEFINED_METHOD, type];
      }
      switch (fn) {
        case "into":
          return this.insertInto(query.join(" "));
      }
    }

    insertInto(query) {
      var column, columnName, columns, i, j, len, offset, table, tableName, value, values;
      [tableName] = query.split(/\s|\(/g, 1).filter(Boolean);
      table = this.getTable(tableName);
      query = query.split(tableName)[1];
      if (!/VALUES/i.test(query)) {
        [columns = table.columnsField, values = query];
      } else {
        [columns, values] = query.split(/values/i);
      }
      columns = columns.substring(columns.indexOf("(") + 1, columns.lastIndexOf(")")).split(/\,/);
      values = values.substring(values.indexOf("(") + 1, values.lastIndexOf(")")).split(/\,/);
      if (columns.length !== values.length) {
        throw [TYPE.COLUMN_COUNT_MUST_EQUAL_TO_VALUE_COUNT];
      }
      offset = table.alloc();
      for (i = j = 0, len = columns.length; j < len; i = ++j) {
        columnName = columns[i];
        column = table.getColumn(columnName);
        value = column.parse(values[i], column.size);
        table.set(column.encode(value), offset + column.begin);
      }
      return offset / table.byteStride;
    }

    encodeText(text) {
      return this.textEncoder.encode(text);
    }

    encodeJSON(object) {
      return this.textEncoder.encode(JSON.stringify(object));
    }

    createTable(query, options = {}) {
      var allocIndex, byteLength, byteOffset, column, columns, j, len, name, offset, size, storeIndex, stride, table, tableHeaders, type;
      [table] = query.split(/\s|\(/g).filter(Boolean);
      [...columns] = query.substring(query.indexOf("(") + 1, query.lastIndexOf(")")).trim().split(/\,/g);
      columns = (function() {
        var j, len, results;
        results = [];
        for (j = 0, len = columns.length; j < len; j++) {
          column = columns[j];
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
      for (j = 0, len = columns.length; j < len; j++) {
        column = columns[j];
        column.begin = offset;
        column.end = offset += column.size;
        column.parse = this.getTypeParser(column);
        column.encode = this.getTypeEncoder(column);
        column.decode = this.getTypeDecoder(column);
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
      var headers, j, len, ref;
      ref = this.headersArray;
      for (j = 0, len = ref.length; j < len; j++) {
        headers = ref[j];
        if (headers.table !== table) {
          continue;
        }
        return Object.assign(new TableStore(this.buffer, headers.byteOffset, headers.byteLength), {
          headers: headers,
          alloc: (byteLength = headers.stride) => {
            return Atomics.add(this.uInt32Array, headers.allocIndex, byteLength);
          },
          offset: () => {
            return Atomics.load(this.uInt32Array, headers.allocIndex);
          }
        });
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

    getTypeParser(column) {
      switch (column.type) {
        case "int":
          return Number;
        case "varchar":
          return function(s) {
            return s.trim().split(/\'|\"/, 2)[1].substr(0, column.size);
          };
      }
    }

    getTypeEncoder(column) {
      switch (column.type) {
        case "int":
          switch (column.size) {
            case 1:
              return function(num) {
                return new Uint8Array([num]);
              };
            case 2:
              return function(num) {
                return new Uint16Array([num]);
              };
            case 4:
              return function(num) {
                return new Uint32Array([num]);
              };
          }
          break;
        case "varchar":
          return (s) => {
            return this.textEncoder.encode(s);
          };
      }
    }

    getTypeDecoder(column) {
      switch (column.type) {
        case "int":
          return (table, offset) => {
            return this.uInt32Array[table.headers.storeIndex + offset / 4];
          };
        case "varchar":
          return (array, offset) => {
            return this.textDecoder.decode(new Uint8Array(column.size).set(this.uInt8Array, array.byteOffset + offset, column.size));
          };
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
