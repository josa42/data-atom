mysql = require 'mysql'
DataManager = require './data-manager'

module.exports =
class MysqlManager extends DataManager
  
  execute: (query, onSuccess, onError) ->
    
    connection = mysql.createConnection @url
    connection.connect()
      
    if connection and query
      connection.query query, (err, rows, fields) =>
        
        if err
          console.error 'Query error - ' + err
          onError err unless !onError
          
        else if onSuccess
          command = query.match(/^\s*([a-zA-Z]+)/)[1].toUpperCase()
          
          if typeof rows.length is 'number'
            onSuccess
              message: null,
              command: command,
              fields: fields,
              rowCount: rows.length,
              rows: rows.map (row) -> fields.map (field) -> row[field.name]
          
          else
            onSuccess
              message: @buildMessage(command, rows),
              command: command
        
        connection.end()

  buildMessage: (command, results) ->
    switch command
      when 'INSERT' then results.affectedRows + ' rows inserted.'
      when 'UPDATE' then results.affectedRows + ' rows updated.'
      when 'DELETE' then results.affectedRows + ' rows deleted.'
      else JSON.stringify(results)
