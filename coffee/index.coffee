import "./debug.js"
import { EventEmitter } from "events"

class CreateTableEvent extends CustomEvent
    constructor : ( header ) ->
        super BufferSQLServer::EVENT_TABLE_CREATE, {
            detail : header
        }

class TableStore extends Uint8Array
    Object.defineProperties this::,
        columnNames : get   : ->
            @headers.columns.slice().map (c) -> c.name

        hasColumn   : value : ->
            @columnNames.includes arguments[0]

export class BufferSQLServer extends EventEmitter
    @MODE_ISOLATED = TYPE.MODE_ISOLATED

    @defaultByteLength = 1e8
    @defaultBufferType = SharedArrayBuffer ? ArrayBuffer

    @defaultOPMode = @MODE_ISOLATED
    
    _maxListeners : 11
    _tableOptions :
        rowLength : 1e5

    TINDEX_TYPEOFFSET : 0
    TINDEX_BYTEOFFSET : 1
    TINDEX_DATAOFFSET : 2

    MALLOC_BYTELENGTH : 1e6
    TYPEOF_TABLE : TYPE.TABLE
    
    EVENT_TABLE_CREATE : TYPE.TABLE_CREATE_EVENT

    textEncoder : new TextEncoder()

    constructor : ( options = {} ) ->
        unless options.port
            log.debug "No listening port defined. SQL server running on isolated mode."
            options.opMode = BufferSQLServer.defaultOPMode

        unless options.byteLength
            log.debug "No initial byteLength parameter defined. Default size (100Mb) will be used."
            options.byteLength = BufferSQLServer.defaultByteLength

        unless options.engine
            log.debug "No buffer type defined. Default endine (#{BufferSQLServer.defaultBufferType.name}) will be used."
            options.bufferType = BufferSQLServer.defaultBufferType

        super().options = options

        @buffer = new @options.bufferType @options.byteLength  

        @uInt8Array = new Uint8Array @buffer
        @uInt32Array = new Uint32Array @buffer
        @headersArray = new Array()

        log.event "Buffer SQL server is running."
        Atomics.or @uInt32Array, @TINDEX_DATAOFFSET, 12
        Atomics.or @uInt32Array, @TINDEX_BYTEOFFSET, @buffer.byteLength
        Atomics.or @uInt32Array, @TINDEX_TYPEOFFSET, TYPE.BufferSQLDatabase

    malloc : ( byteLength = @MALLOC_BYTELENGTH, type = @TYPEOF_TABLE ) ->
        allocLength = byteLength + 12

        if  mod = allocLength % 4
            allocLength = 4 - mod

        offset = Atomics.add @uInt32Array, @TINDEX_DATAOFFSET, allocLength
        tindex = offset / 4

        Atomics.store @uInt32Array, tindex, type
        Atomics.store @uInt32Array, tindex + 1, byteLength

        offset + 12

    query : ( sql ) ->
        [ type, ...query ] = sql.trim().split(" ")

        unless fn = @[ type.toLowerCase() ]
            throw [ TYPE.NO_DEFINED_METHOD, type ]

        fn.call this, query.join(" ")

    create : ( sql ) ->
        [ type, ...query ] = sql.trim().split(" ")

        unless /table/.test fn = type.toLowerCase()
            throw [ TYPE.NO_DEFINED_METHOD, type ]

        switch fn
            when "table" then return @createTable query.join(" ")

    select : ( sql ) ->
        split = sql.split(/FROM/i, 2).filter(Boolean)
        table = @getTable split[1].trim()

        unless columns = split[0].replace(/\s|\*/g, "")
            columns = table.columnNames
        else columns = columns.split /\,/g

        for column in columns
            unless table.hasColumn column
                throw [ TYPE.TABLE_HAS_NOT_COLUMN, column ]

        { table, columns }

    encodeText  : ( text ) ->
        @textEncoder.encode text

    encodeJSON  : ( object ) ->
        @textEncoder.encode JSON.stringify object

    createTable : ( query, options = {} ) ->
        [ table ] = query.split(/\s|\(/g).filter Boolean
        [ ...columns ] = query.substring(
            query.indexOf("(")+1,
            query.lastIndexOf(")")
        ).trim().split(/\,/g)

        columns = for column in columns
            [ name, type, ...size ] =
                column.split(/\s|\(|\)/g).filter Boolean

            type = type.toLowerCase()
            size = @getColumnByteLength type, size.join(" ")
            
            { name, type, size }

        stride = columns.slice()
            .map((d) -> d.size)
            .reduce (d, e) -> d + e 

        offset = 0
        for column in columns
            column.begin = offset
            column.end   =
                offset  += column.size

        options = {
            ...this._tableOptions,
            ...options
        }

        byteLength = options.rowLength * stride
        byteOffset = @malloc byteLength
        storeIndex = byteOffset / 4
        allocIndex = storeIndex - 1
        
        @headersArray.push tableHeaders = { 
            table, columns, stride,
            byteLength, byteOffset, 
            storeIndex, allocIndex,
            ...options
        }

        @emit @EVENT_TABLE_CREATE, new CreateTableEvent {
            table : table = @getTable table
            headers : tableHeaders
            database : this
        }

        table
    
    getTable : ( table ) ->
        for headers in @headersArray
            continue unless headers.table is table
            return Object.assign new TableStore(
                @buffer, headers.byteOffset, headers.byteLength
            ), { headers }

        throw [ TYPE.TABLE_CANT_FOUND_ON_THIS_DATABASE, table ]

    getColumnByteLength : ( type, options ) ->
        return switch type
            when "int" then parseFloat options or 4
            when "varchar" then parseFloat options or 255
            else throw TYPE.TYPE_IS_UNDEFINED_FOR_FIND_OPTIONS
        