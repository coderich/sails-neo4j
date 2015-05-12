// Modules
var Q = require('q');
var Request = require('request');

// Services
var UtilService = {
    getDistanceFromLatLonInKm : function(lat1, lon1, lat2, lon2) {
        var R = 6371; // Radius of the earth in km
        var dLat = _util.deg2rad(lat2-lat1);
        var dLon = _util.deg2rad(lon2-lon1);
        var a = Math.sin(dLat/2) * Math.sin(dLat/2) + Math.cos(_util.deg2rad(lat1)) * Math.cos(_util.deg2rad(lat2)) * Math.sin(dLon/2) * Math.sin(dLon/2);
        var c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
        var d = R * c; // Distance in km
        return d;
    },

    merge : function(obj1, obj2) {
        var obj1 = obj1 || {};
        var obj2 = obj2 || {};
        var obj3 = {};
        for (var attrname in obj1) { obj3[attrname] = obj1[attrname]; }
        for (var attrname in obj2) { obj3[attrname] = obj2[attrname]; }
        return obj3;
    }
}

// Private Functions
function normalize(obj) {
    for (var p in obj) if (typeof obj[p] == 'undefined' || obj[p] == null) delete obj[p];
    if (typeof obj.id == 'string' && !isNaN(obj.id)) obj.id = Number(obj.id);
    return obj;
}

function parameterize(obj, options) {
    var options = options || {};
    var prefix = options.prefix || '';
    var alias = options.alias || 'n';
    var ret = {str:'', form:'', json:{}};

    if (!obj) return ret;

    obj = normalize(obj);
    ret.str = '{' + Object.keys(obj).map(function(key) { return key + ':{' + prefix + key + '}'; }).join(',') + '}';
    ret.form = Object.keys(obj).map(function(key) { return alias + '.' + key + '={' + prefix + key + '}'; }).join(',');
    ret.json = JSON.parse(JSON.stringify(obj), function(key, value) { this[prefix + key] = value; return value; }) || {};
    return ret;
}

function hydrate(collection, body, options) {
    var options = options || {};
    if (!body || !body.data) return [];

    var arr = body.data.map(function(el) {
        var data = el[0].data;
        if (data.createdAt) data.createdAt = new Date(data.createdAt);
        if (data.updatedAt) data.updatedAt = new Date(data.updatedAt);
        delete data.bbox;
        delete data.gtype;
        return new sails.models[collection]._model(data);
    });

    return arr;
}

// Adapter API
module.exports = (function () {

    // You'll want to maintain a reference to each connection
    // that gets registered with this adapter.
    var connections = {};

    // You may also want to store additional, private data
    // per-connection (esp. if your data store uses persistent
    // connections).
    //
    // Keep in mind that models can be configured to use different databases
    // within the same app, at the same time.
    //
    // i.e. if you're writing a MariaDB adapter, you should be aware that one
    // model might be configured as `host="localhost"` and another might be using
    // `host="foo.com"` at the same time.  Same thing goes for user, database,
    // password, or any other config.
    //
    // You don't have to support this feature right off the bat in your
    // adapter, but it ought to get done eventually.

    var adapter = {
        // Set to true if this adapter supports (or requires) things like data types, validations, keys, etc.
        // If true, the schema for models using this adapter will be automatically synced when the server starts.
        // Not terribly relevant if your data store is not SQL/schemaful.
        //
        // If setting syncable, you should consider the migrate option,
        // which allows you to set how the sync will be performed.
        // It can be overridden globally in an app (config/adapters.js)
        // and on a per-model basis.
        //
        // IMPORTANT:
        // `migrate` is not a production data migration solution!
        // In production, always use `migrate: safe`
        //
        // drop   => Drop schema and data, then recreate it
        // alter  => Drop/add columns as necessary.
        // safe   => Don't change anything (good for production DBs)
        //
        syncable: true,

        // Default configuration for connections
        defaults: {
            protocol: 'http://',
            port: 7474,
            host: 'localhost',
            base: '/db/data',
            debug: true
        },

        /**
         * This method runs when a model is initially registered
         * at server-start-time.  This is the only required method.
         *
         * @param  {[type]}   connection [description]
         * @param  {[type]}   collection [description]
         * @param  {Function} cb         [description]
         * @return {[type]}              [description]
         */
        registerConnection: function(connection, collections, cb) {
            if(!connection.identity) return cb(new Error('Connection is missing an identity.'));
            if(connections[connection.identity]) return cb(new Error('Connection is already registered.'));

            // Add in logic here to initialize connection
            // e.g. connections[connection.identity] = new Database(connection, collections);
            var endpoint = connection.protocol + connection.host + ':' + connection.port + connection.base;
            Request.get(endpoint, {json:true}, function(err, res, body) {
                if (err) return cb(err);
                if (body.cause) return cb(body.cause.exception);

                connections[connection.identity] = UtilService.merge(connection, body);

                if (typeof connection.geom === 'undefined') return cb();

                // This will create a spatial index in Neo4j to enable geometric calculations on nodes
                Request.post(body.node_index, {
                    json : {
                        name : 'geom',
                        config : {
                            provider : 'spatial',
                            geometry_type : 'point',
                            lat : connection.geom.lat,
                            lon : connection.geom.lon
                        }
                    }
                }, function(err, res, body) {
                    cb(err);
                });
            });
        },

        /**
         * Fired when a model is unregistered, typically when the server
         * is killed. Useful for tearing-down remaining open connections,
         * etc.
         *
         * @param  {Function} cb [description]
         * @return {[type]}      [description]
         */
        // Teardown a Connection
        teardown: function (conn, cb) {
            if (typeof conn == 'function') {
                cb = conn;
                conn = null;
            }

            if (!conn) {
                connections = {};
                return cb();
            }

            if(!connections[conn]) return cb();
            delete connections[conn];
            cb();
        },


        describe: function (connection, collection, cb) {
            // Add in logic here to describe a collection (e.g. DESCRIBE TABLE logic)
            console.log('describe');
            cb();
        },

        define: function (connection, collection, definition, cb) {
            var uri = connections[connection].constraints + '/' + collection;

            // Unique Constraints
            var uniqueKeys = []; for (var p in definition) { if (definition[p].unique) uniqueKeys.push(p); }

            async.forEach(uniqueKeys, function(key, callback) {
                Request.post(uri + '/uniqueness/', {
                    json : { property_keys : [key] }
                }, function(err, res, body) {
                    if (err) return callback(err);
                    // if (body.cause) return callback(body.cause.exception);
                    callback();
                });
            }, function(err) {
                cb(err);
            });
        },

        drop: function (connection, collection, relations, cb) {
            var uri = connections[connection].cypher;

            Request.post(uri, {
                json : {
                    query : 'MATCH (n:' + collection + ') OPTIONAL MATCH (n)-[r]-() DELETE n,r'
                }
            }, function(err, res, body) {
                if (err) return cb(err);
                if (body.cause) return cb(body.cause.exception);
                cb();
            });
        },

        find: function (connection, collection, options, cb) {
            options.where = options.where || {};
            options.where._where = options.where._where || false;
            var _where = options.where._where ? ' WHERE ' + options.where._where : '';
            delete options.where._where;

            var uri = connections[connection].cypher;
            var where = parameterize(options.where);
            var limit = options.limit ? ' LIMIT ' + options.limit : '';
            var skip = options.skip ? ' SKIP ' + options.skip : '';

            Request.post(uri, {
                json : {
                    query : 'MATCH (n:' + collection + where.str + ')' + _where + ' RETURN n' + skip + limit,
                    params : where.json
                }
            }, function(err, res, body) {
                if (err) return cb(err);
                if (body.cause) return cb(body.cause.exception);
                cb(null, hydrate(collection, body));
            });
        },

        create: function (connection, collection, values, cb) {
            var connection = connections[connection];
            var uri = connection.cypher;

            Request.post(uri, {
                json : {
                    query : 'CREATE (n:' + collection + '{props}' + ') SET n.id = id(n) RETURN n',
                    params : {props:normalize(values)}
                }
            }, function(err, res, body) {
                if (err) return cb(err);
                if (body.cause) return cb(body.cause.exception);

                var model = hydrate(collection, body)[0];
                var uri = connection.extensions.SpatialPlugin.addNodeToLayer;
                if (typeof connection.geom === 'undefined') return cb(err, model);
                if (typeof model[connection.geom.lat] === 'undefined' || typeof model[connection.geom.lon] === 'undefined') return cb(err, model);

                // Add node to spatial index
                Request.post(uri, {
                    json : {
                        layer : 'geom',
                        node : connection.node + '/' + model.id
                    }
                }, function(err, res, body) {
                    if (err) return cb(err);
                    if (body.cause) return cb(body.cause.exception);
                    cb(null, model);
                });
            });
        },

        update: function (connection, collection, options, values, cb) {
            var connection = connections[connection];
            var uri = connection.cypher;
            var where = parameterize(options.where, {prefix:'where_'});
            var values = parameterize(values, {prefix:'vals_'});

            Request.post(uri, {
                json : {
                    query : 'MATCH (n:' + collection + where.str + ') SET ' + values.form + ' RETURN n',
                    params : UtilService.merge(where.json, values.json)
                }
            }, function(err, res, body) {
                if (err) return cb(err);
                if (body.cause) return cb(body.cause.exception);

                var model = hydrate(collection, body)[0];
                var uri = connection.extensions.SpatialPlugin.addNodeToLayer;
                if (typeof connection.geom === 'undefined') return cb(err, model);
                if (typeof model[connection.geom.lat] === 'undefined' || typeof model[connection.geom.lon] === 'undefined') return cb(err, model);

                // Add node to spatial index
                Request.post(uri, {
                    json : {
                        layer : 'geom',
                        node : connection.node + '/' + model.id
                    }
                }, function(err, res, body) {
                    if (err) return cb(err);
                    if (body.cause) return cb(body.cause.exception);
                    cb(null, model);
                });
            });
        },

        destroy: function (connection, collection, options, cb) {
            var uri = connections[connection].cypher;
            var where = parameterize(options.where, {prefix:'where_'});

            Request.post(uri, {
                json : {
                    query : 'MATCH (n:' + collection + where.str + ') OPTIONAL MATCH (n)-[r]-() DELETE n,r',
                    params : where.json
                }
            }, function(err, res, body) {
                if (err) return cb(err);
                if (body.cause) return cb(body.cause.exception);
                cb(null, {});
            });
        },

        join : function(connection, collection, criteria, cb) {
            console.log('join');
            console.log(JSON.stringify(criteria));
            cb();
        },

        link: function(connection, collection, params, cb) {
            params.values = params.values || {};
            params.values.createdAt = params.values.createdAt || new Date();
            params.values.updatedAt = params.values.updatedAt || new Date();
            var uri = connections[connection].cypher;
            var label1 = collection;
            var label2 = params.label || label1;
            var label3 = params.relation || 'link';
            var params1 = parameterize(params.start, {prefix: 'start_'});
            var params2 = parameterize(params.end, {prefix:'end_'});
            var params3 = parameterize(params.values, {prefix:'link_'});

            Request.post(uri, {
                json : {
                    query : 'MATCH (n:' + label1 + params1.str + '),(m:' + label2 + params2.str + ') CREATE (n)-[r:' + label3 + params3.str + ']->(m) RETURN r',
                    params : UtilService.merge(params3.json, UtilService.merge(params1.json, params2.json))
                }
            }, function(err, res, body) {
                if (err) return cb(err);
                if (body.cause) return cb(body.cause.exception);
                cb();
            });
        },

        unlink: function(connection, collection, params, cb) {
            var uri = connections[connection].cypher;
            var label1 = collection;
            var label2 = params.label || label1;
            var label3 = params.relation || 'link';
            var params1 = parameterize(params.start, {prefix:'start_'});
            var params2 = parameterize(params.end, {prefix:'end_'});
            var params3 = parameterize(params.values, {prefix:'link_'});
            var direction = params.bidirectional ? '-' : '->';

            Request.post(uri, {
                json : {
                    query : 'MATCH (n:' + label1 + params1.str + '),(m:' + label2 + params2.str + '),(n)-[r:' + label3 + params3.str + ']' + direction + '(m) DELETE r',
                    params : UtilService.merge(params3.json, UtilService.merge(params1.json, params2.json))
                }
            }, function(err, res, body) {
                if (err) return cb(err);
                if (body.cause) return cb(body.cause.exception);
                cb();
            });
        },

        nearby: function(connection, collection, params, cb) {
            var connection = connections[connection];
            var distAttr = params.distAttr || 'distance';
            var modelLat = connection.geom.lat;
            var modelLon = connection.geom.lon;
            var where = parameterize(params.where);
            var uri = connection.cypher;

            Request.post(uri, {
                json : {
                    query : 'START n=node:geom("withinDistance:[' + parseFloat(params.lat).toFixed(13) + ',' + parseFloat(params.lon).toFixed(13) + ',' + parseFloat(params.dist).toFixed(2) + ']") MATCH (n:' + collection + where.str +') RETURN n',
                    params : where.json
                }
            }, function(err, res, body) {
                if (err) return cb(err);
                if (body.cause) return cb(body.cause.exception);
                var models = hydrate(collection, body);
                cb(null, models.map(function(model) { model[distAttr] = UtilService.getDistanceFromLatLonInKm(model[modelLat], model[modelLon], params.lat, params.lon); return model; }));
            });
        },

        graph : function(connection, collection, params, cb) {
            params.relation = params.relation || {};
            var connection = connections[connection];
            var me = parameterize(params.me, {prefix:'me_'});
            var them = parameterize(params.where, {prefix:'them_'});
            var modelLat = connection.geom.lat;
            var modelLon = connection.geom.lon;
            var modelAttr = params.attr || 'graph';
            var uri = connection.cypher;

            // Query parts
            var rcount = 1;
            var rets = ['them', 'me'];
            var start = params.nearby ? 'START them=node:geom("withinDistance:[' + parseFloat(params.nearby.lat).toFixed(13) + ',' + parseFloat(params.nearby.lon).toFixed(13) + ',' + parseFloat(params.nearby.dist).toFixed(2) + ']") ' : '';
            var where = params.me ? 'WHERE them.id <> me.id ' : '';
            var relations = (params.relations && params.relations.length)
                ? params.relations.map(function(relation) {
                    var rel = 'r' + rcount;
                    var hop = 'h' + rcount;
                    var str = (relation.required ? '' : 'OPTIONAL ') + 'MATCH ' + hop + '=shortestPath(me-[' + rel + ':' + relation.type + ']-them)';
                    rcount++;
                    rets.push(rel, hop);
                    return str;
                }).join(' ')
                : ''
            ;
            var req = {
                json : {
                    query : start + 'MATCH (me:' + collection + me.str + '),(them:' + collection + them.str + ') ' + where + relations + ' RETURN DISTINCT ' + rets.join(','),
                    params : UtilService.merge(me.json, them.json)
                }
            };

            Request.post(uri, req, function(err, res, body) {
                if (err) return cb(err);
                if (body.cause) return cb(body.cause.exception);
                cb(null, hydrate(collection, body).map(function(model, index) {
                    var data = body.data[index];
                    var distance = params.nearby ? UtilService.getDistanceFromLatLonInKm(model[modelLat], model[modelLon], params.nearby.lat, params.nearby.lon) : null;
                    try { var r_me = body.data[index][1] || {}; } catch(e) { var r_me = {}; }
                    try { var r_them = body.data[index][0] || {}; } catch(e) { var r_them = {}; }

                    // Graph Attributes
                    model[modelAttr] = {};
                    model[modelAttr].kilometers = (typeof distance == 'number') ? distance : null;
                    model[modelAttr].miles = (typeof distance == 'number') ? (distance / 1.60934) : null;

                    // Optional Relations
                    for (var i=2; i<data.length; i++) {
                        var el = data[i];
                        if (el) {
                            if (i % 2 == 0) {
                                for (var j=0; j<el.length; j++) {
                                    var rel = el[j];
                                    var isMyRel = (rel.start == r_me.self && rel.end == r_them.self);
                                    var isTheirRel = (rel.start == r_them.self && rel.end == r_me.self);
                                    model[modelAttr][rel.type] = model[modelAttr][rel.type] || {data:{}, hops:[], isMine:false};
                                    if (isMyRel || isTheirRel) model[modelAttr][rel.type].data = rel.data;
                                    if (isMyRel) model[modelAttr][rel.type].isMine = true;
                                }
                            } else {
                                model[modelAttr][rel.type].hops = el.nodes.slice(1).map(function(node) { return node.substr(node.lastIndexOf('/') + 1); });
                            }
                        }
                    }

                    return model;
                }));
            });
        },

        count: function(connection, collection, options, cb) {
            var uri = connections[connection].cypher;
            var match = parameterize(options.match);

            Request.post(uri, {
                json : {
                    query : 'MATCH (n:' + collection + match.str + ') RETURN count(n)',
                    params : match.json
                }
            }, function(err, res, body) {
                if (err) return cb(err);
                if (body.cause) return cb(body.cause.exception);
                cb(null, body.data[0][0]);
            });
        },
    };

    // Expose adapter definition
    return adapter;
})();

//var label = collection.charAt(0).toUpperCase() + collection.slice(1);

