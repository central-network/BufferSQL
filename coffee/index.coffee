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

        byteStride  : get   : ->
            @headers.stride

        index  : get   : ->
            @headers.storeIndex

        columnsField : get   : ->
            "(#{@columnNames})"

        hasColumn   : value : ->
            @columnNames.includes arguments[0]

        getColumn   : value : ( columnName ) ->
            unless column = @headers.columns.find (c) -> c.name is columnName
                throw [ TYPE.THERE_IS_NO_COLUMN_NAMED_WITH, columnName ]
            column


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
        table = @getTable split[1].split(/where/i,1)[0].trim()
        
        matchs = []
        offset = table.offset()
        stride = table.byteStride

        unless columns = split[0].replace(/\s|\*/g, "")
            columnNames = table.columnNames
        else columnNames = columns.split /\,/g

        for columnName in columnNames
            unless table.hasColumn columnName
                throw [ TYPE.TABLE_HAS_NOT_COLUMN, columnName ]

        for condition in sql.split(/where/i)[1].split(/and|or/i)
            [ columnName, value ] =
                condition.split( />=|<=|>|<|=|<>|\!=|=/ ).map (c) -> c.trim()

            unless table.hasColumn columnName
                throw [ TYPE.TABLE_HAS_NOT_COLUMN, columnNames ]

            limit = offset
            column = table.getColumn columnName
            operator = condition.match( /(>=|<=|>|<|=|<>|\!=|=)/ )[0]
            parsedValue = column.parse value
            searchOffset = 0

            while searchOffset < limit

                columnValue = column.decode(
                    table, searchOffset
                )

                switch operator
                    when ">=" then if columnValue >= parsedValue
                        matchs.push searchOffset

                searchOffset += stride

        matchs


    insert : ( sql ) ->
        [ type, ...query ] = sql.trim().split(" ")

        unless /into/.test fn = type.toLowerCase()
            throw [ TYPE.NO_DEFINED_METHOD, type ]

        switch fn
            when "into" then return @insertInto query.join(" ")

    insertInto  : ( query ) ->
        [ tableName ] = query.split(/\s|\(/g, 1).filter Boolean

        table = @getTable tableName
        query = query.split( tableName )[1]

        unless /VALUES/i.test query
            [ columns = table.columnsField, values = query ]

        else
            [ columns, values ] = query.split /values/i

        columns = columns.substring(
            columns.indexOf("(") + 1,
            columns.lastIndexOf(")")
        ).split /\,/

        values = values.substring(
            values.indexOf("(") + 1,
            values.lastIndexOf(")")
        ).split /\,/

        unless columns.length is values.length
            throw [ TYPE.COLUMN_COUNT_MUST_EQUAL_TO_VALUE_COUNT ]
        
        offset = table.alloc()

        for columnName, i in columns
            column = table.getColumn columnName
            value = column.parse values[i], column.size
            table.set column.encode( value ), offset + column.begin
            
        offset / table.byteStride

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
            
            column.parse  = @getTypeParser column
            column.encode = @getTypeEncoder column
            column.decode = @getTypeDecoder column


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
            ), { 
                headers : headers,
                alloc   : ( byteLength = headers.stride ) => Atomics.add @uInt32Array, headers.allocIndex, byteLength
                offset  : => Atomics.load @uInt32Array, headers.allocIndex
            }

        throw [ TYPE.TABLE_CANT_FOUND_ON_THIS_DATABASE, table ]

    getColumnByteLength : ( type, options ) ->
        return switch type
            when "int" then parseFloat options or 4
            when "varchar" then parseFloat options or 255
            else throw TYPE.TYPE_IS_UNDEFINED_FOR_FIND_OPTIONS
        

    getTypeParser       : ( column ) ->
        switch column.type
            when "int" then Number
            when "varchar" then (s) -> s.trim().split(/\'|\"/,2)[1].substr(0, column.size)

    getTypeEncoder      : ( column ) ->
        switch column.type
            when "int" then switch column.size
                when 1 then ( num ) -> new Uint8Array [ num ]
                when 2 then ( num ) -> new Uint16Array [ num ]
                when 4 then ( num ) -> new Uint32Array [ num ]

            when "varchar" then (s) => @textEncoder.encode s

    getTypeDecoder      : ( column ) ->
        switch column.type

            when "int" then ( table, offset ) =>
                @uInt32Array[ table.headers.storeIndex + offset / 4 ]

            when "varchar" then ( array, offset ) =>
                @textDecoder.decode new Uint8Array( column.size ).set(
                    @uInt8Array, array.byteOffset + offset, column.size
                )
