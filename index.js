'use strict';

const async = require( 'async' );
const extend = require( 'extend' );

const Rethink_Helper = module.exports = {};

Rethink_Helper.init = function( options, callback ) {
    const result = {};
    async.series( [
        _verify.bind( null, options, result ),
        _connect.bind( null, options, result ),
        _createDB.bind( null, options, result ),
        _createTables.bind( null, options, result ),
        _ensureIndexes.bind( null, options, result )
    ], error => {
        callback( error, result );
    } );
};

function _verify( options, result, callback ) {
    if ( !( options && options.database ) ) {
        callback( 'You must specify a database.' );
        return;
    }

    callback();
}

function _connect( options, result, callback ) {
    result.r = require( 'rethinkdb' );

    var retries = 0;
    ( function __connect() {
        result.r.connect( {
            host: options.host || 'rethinkdb',
            port: options.port || 28015
        }, function( error, connection ) {
            if ( error && error.name === 'ReqlDriverError' && error.message.indexOf( 'Could not connect' ) === 0 && ++retries < 31 ) {
                setTimeout( __connect, 1000 );
                return;
            }

            result.connection = connection;
            callback( error );
        } );
    } )();
}

function _createDB( options, result, callback ) {
    result.r.dbList().contains( options.database ).do( function( dbExists ) {
        return result.r.branch(
            dbExists, {
                created: 0
            },
            result.r.dbCreate( options.database )
        );
    } ).run( result.connection, callback );
}

function _createTables( options, result, callback ) {
    async.series( [
        // check if we even need to do anything
        function( next ) {
            next( options.tables ? null : 'skip' );
        },

        // iterate over tables, creating them as necessary
        function( next ) {
            async.eachSeries( options.tables, function( _table, _next ) {
                var _tableOptions = typeof _table === 'object' ? _table.options || {} : {};
                var table = typeof _table === 'object' ? _table.name : _table;

                var tableOptions = extend( {
                    shards: 3
                }, _tableOptions );

                result.r.db( options.database ).tableList().contains( table ).do( function( tableExists ) {
                    return result.r.branch(
                        tableExists, {
                            created: 0
                        },
                        result.r.db( options.database ).tableCreate( table, tableOptions )
                    );
                } ).run( result.connection, _next );
            }, next );
        }
    ], function( error ) {
        error = error === 'skip' ? null : error;
        callback( error );
    } );
}

function _ensureIndexes( options, result, callback ) {

    async.series( [
        // check if we need to do anything
        function( next ) {
            next( options.indexes ? null : 'skip' );
        },

        // iterate over indexes, creating them
        function( next ) {
            async.eachSeries( options.indexes, function( indexSpec, _next ) {
                result.r
                    .db( options.database )
                    .table( indexSpec.table )
                    .indexList()
                    .contains( indexSpec.name )
                    .do( function( indexExists ) {
                        return result.r.branch(
                            indexExists, {
                                created: 0
                            },
                            result.r.db( options.database ).table( indexSpec.table ).indexCreate( indexSpec.name, indexSpec.create )
                        );
                    } ).run( result.connection, _next );
            }, next );
        },

        next => {
            async.eachSeries( options.indexes, ( indexSpec, _next ) => {
                if ( indexSpec.wait === false ) {
                    _next();
                    return;
                }

                result.r
                    .db( options.database )
                    .table( indexSpec.table )
                    .indexWait( indexSpec.name )
                    .run( result.connection, _next );
            }, next );
        }
    ], function( error ) {
        error = error === 'skip' ? null : error;
        callback( error );
    } );
}