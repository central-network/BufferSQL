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

console.log server.query("
    SELECT * FROM Persons
")