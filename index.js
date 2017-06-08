'use strict';

const async = require( 'async' );
const extend = require( 'extend' );

const Rethink_Helper = module.exports = {};

Rethink_Helper.init = function( _options, callback ) {
    const options = extend( {
        host: 'localhost',
        port: 28015
    }, _options );

    const result = {};

    async.series( [
        _verify.bind( null, options, result ),
        _connect.bind( null, options, result ),
        _create_database.bind( null, options, result ),
        _create_tables.bind( null, options, result ),
        _ensure_indexes.bind( null, options, result )
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

    let retries = 0;
    ( function __connect() {
        result.r.connect( {
            host: options.host,
            port: options.port
        }, ( error, connection ) => {
            if ( error && error.name === 'ReqlDriverError' && error.message.indexOf( 'Could not connect' ) === 0 && ++retries < 31 ) {
                setTimeout( __connect, 1000 );
                return;
            }

            result.connection = connection;
            callback( error );
        } );
    } )();
}

function _create_database( options, result, callback ) {
    if ( !result || !result.r || !result.connection ) {
        callback( 'Must have a rethink instance and a valid connection!' );
        return;
    }

    result.r
        .dbList()
        .contains( options.database )
        .do( db_exists => {
            return result.r
                .branch(
                    db_exists,
                    { created: 0 },
                    result.r.dbCreate( options.database )
                );
        } ).run( result.connection, callback );
}

function _create_tables( options, result, callback ) {
    if ( !result || !result.r || !result.connection ) {
        callback( 'Must have a rethink instance and a valid connection!' );
        return;
    }

    if ( !options.tables ) {
        callback();
        return;
    }

    async.eachSeries( options.tables, ( _table, next ) => {
        var _table_options = typeof _table === 'object' ? _table.options || {} : {};
        var table = typeof _table === 'object' ? _table.name : _table;

        var table_options = extend( {
            shards: 3
        }, _table_options );

        result.r
            .db( options.database )
            .tableList()
            .contains( table )
            .do( table_exists => {
                return result.r
                    .branch(
                        table_exists,
                        { created: 0 },
                        result.r.db( options.database ).tableCreate( table, table_options ) );
            } ).run( result.connection, next );
    }, callback );
}

function _ensure_indexes( options, result, callback ) {
    if ( !options.indexes ) {
        callback();
        return;
    }

    async.series( [
        // iterate over indexes, creating them
        function( next ) {
            async.eachSeries( options.indexes, ( index_spec, _next ) => {
                result.r
                    .db( options.database )
                    .table( index_spec.table )
                    .indexList()
                    .contains( index_spec.name )
                    .do( index_exists => {
                        return result.r
                            .branch(
                                index_exists,
                                { created: 0 },
                                result.r.db( options.database ).table( index_spec.table ).indexCreate( index_spec.name, index_spec.create ) );
                    } ).run( result.connection, _next );
            }, next );
        },

        next => {
            async.eachSeries( options.indexes, ( index_spec, _next ) => {
                if ( index_spec.wait === false ) {
                    _next();
                    return;
                }

                result.r
                    .db( options.database )
                    .table( index_spec.table )
                    .indexWait( index_spec.name )
                    .run( result.connection, _next );
            }, next );
        }
    ], callback );
}