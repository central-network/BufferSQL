import { BufferSQLServer } from "./index.js"

options = byteLength : 10e8
server = new BufferSQLServer( options )

###
server.on server.EVENT_TABLE_CREATE, ( e ) ->
    console.log e.detail.table
###

server.query("CREATE TABLE Persons (
    PersonID int,
    LastName varchar(255),
    FirstName varchar(255),
    Address varchar(255),
    City varchar(255)
)")

persons = server.getTable "Persons"

console.log "index0:", server.query("
    INSERT INTO Persons (1,'lname','fname','addr', 'city')
")

console.log "index1:", server.query("
    INSERT INTO Persons (LastName,FirstName) VALUES ('2lname','fname3')
")

console.log "select0:", server.query("
    SELECT * FROM Persons WHERE PersonID >= 1
")

parseSelect = ( query ) ->
    forbiddenRanges = [[]]
    index = 0

    loop 
        index = query.indexOf "`", index
        break if index is -1

        if  2 is forbiddenRanges[ forbiddenRanges.length-1 ].length
            forbiddenRanges.push []
    
        forbiddenRanges[ forbiddenRanges.length-1 ].push index++


    if  index = forbiddenRanges.find (e) -> e.length is 1
        throw { error: "QUERY_HAS_AN_UNCLOSED_QUOTE", index, query }

    formPosition = 0
    console.log 2, [ ...query.matchAll /from/gi ].filter (d) ->
        for range in forbiddenRanges
            return if range[0] <= d.index

    #queryUpperCase = queryTrimmed.toUpperCase() 

    console.log { forbiddenRanges }


parseSelect "SELECT *, Persons2.id, `Pers.from`, CONCAT(col1,col2) As col12, `Person.limit` FROM Persons as p WHERE col = 1 AND (col2 > col1) GROUP BY id ORDER BY id ASC, col1 LIMIT 1,10"
#parseSelect "SELECT id, SUM(col2) As `col12` FROM Persons as p WHERE col = 1 AND (`col2` > col1) GROUP BY id ORDER BY id ASC, col1"