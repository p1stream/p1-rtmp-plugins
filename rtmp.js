var util = require('util');
var EventEmitter = require('events');
var chunking = require('./chunking');
var DissolveExt = require('./dissolve-ext');
var ConcentrateExt = require('./concentrate-ext');


function NetConnection(chunkConn) {
    var self = this;

    self.netStreams = Object.create(null);
    self.transactions = Object.create(null);

    self.chunkConn = chunkConn
        .on('readable', function() {
            var msg;
            while ((msg = self.chunkConn.read()))
                self._handleMessage(msg);
        })
        .on('warn', function(s) {
            self.emit('warn', s);
        })
        .on('error', function(err) {
            self.emit('error', err);
        })
        .on('end', function() {
            self.chunkConn = null;
            self.emit('end');
        });

    self.on('end', function() {
        var netStreams = self.netStreams;
        self.netStreams = null;

        var transactions = self.transactions;
        self.transactions = null;

        Object.keys(netStreams).forEach(function(key) {
            var netStream = netStreams[key];
            netStream.emit('end');
        });

        Object.keys(transactions).forEach(function(key) {
            var cb = netStreams[key];
            if (typeof(cb) === 'function')
                cb(new Error("Connection lost"));
        });
    });

    self.mainChunkStream = self.createChunkStream();
}
util.inherits(NetConnection, EventEmitter);

// Send an event message.
NetConnection.prototype.event = function(type, data) {
    var typeBuf = new Buffer(2);
    typeBuf.writeUInt16BE(type, 0);
    return this.chunkConn.control(4, Buffer.concat([typeBuf, data]));
};

// Send a command message.
NetConnection.prototype.call = function(name, args, cb) {
    // Message stream 0 is the NetConnection stream.
    return this._command(0, name, args, cb);
};

// Call the remote connect method.
NetConnection.prototype.connect = function(params, user, cb) {
    if (typeof(user) === 'function') {
        cb = user;
        user = undefined;
    }

    var args = user ? [params, user] : [params];
    return this.call('connect', args, function(err, results) {
        if (err) return cb(err);
        cb(null, results[0], results[1]);
    });
};

// Call the remote createStream method.
NetConnection.prototype.createStream = function(params, cb) {
    var self = this;

    if (typeof(params) === 'function') {
        cb = params;
        params = undefined;
    }

    var args = params ? [params] : [];
    return this.call('createStream', args, function(err, results) {
        if (err) return cb(err);

        var netStream = self._ensureStream(results[1]);
        cb(null, results[0], netStream);
    });
};

// Create a ChunkStream
NetConnection.prototype.createChunkStream = function(options) {
    return this.chunkConn.createStream(options);
};

// End te connection.
NetConnection.prototype.end = function() {
    if (this.chunkConn)
        this.chunkConn.end();
};

// Internal command helper.
NetConnection.prototype._command = function(msgStrmId, name, args, cb) {
    var trId = 0;
    if (cb) {
        while (true) {
            if (!this.transactions[trId = 1 + ~~(Math.random() * 65535)]) {
                this.transactions[trId] = cb;
                break;
            }
        }
    }

    var builder = ConcentrateExt()
        .amf0(name)
        .amf0(trId);
    args.forEach(function(arg) {
        builder.amf0(arg);
    });

    // For convenience, keep the chunk stream ID the same as the
    // message stream ID, plus 3 to keep away from reserved IDs.
    return this.mainChunkStream.write({
        msgTs: 0,
        msgTypeId: 20,  // AMF0
        msgStrmId: msgStrmId,
        data: builder.result()
    });
};

// Handle a received message.
NetConnection.prototype._handleMessage = function(msg) {
    var self = this;

    if (msg.chkStrmId === 2) {
        if (msg.msgStrmId !== 0)
            self.emit('warn', "Got control message on message stream " + msg.msgStrmId);

        self._handleControl(msg.msgTypeId, msg.data);
        return;
    }

    var target = msg.msgStrmId === 0 ? self : self.netStreams[msg.msgStrmId];
    if (!target) {
        self.emit('warn', "Message for missing stream " + msg.msgStrmId);
        return;
    }

    switch (msg.msgTypeId) {
        case 20:  // AMF0 command
            var args = [];
            function pushTmp() {
                args.push(this.vars._tmp);
            }
            DissolveExt()
                .loop(function() {
                    this.amf0('_tmp').tap(pushTmp);
                })
                .on('data', function() {})
                .on('end', function() {
                    var name = args.shift();
                    var trId = args.shift();
                    if (typeof(name) !== 'string' || typeof(trId) !== 'number')
                        return self.emit('warn', "Invalid parameters in command message");

                    // Special handling for transaction responses.
                    if (name !== '_result' && name !== '_error')
                        return target._handleCommand(name, args);

                    var cb = trId && self.transactions[trId];
                    delete self.transactions[trId];

                    if (typeof(cb) === 'function') {
                        if (name === '_result')
                            cb(null, args);
                        else
                            cb(args);
                    }
                    else if (cb !== true) {
                        self.emit('warn', "Response to invalid transaction " + trId);
                    }
                })
                .end(msg.data);
            break;

        default:
            self.emit('warn', "Unknown message type " + msg.msgTypeId);
            break;
    }
};

// Handle a received control message.
NetConnection.prototype._handleControl = function(msgTypeId, data) {
    if (msgTypeId === 4) {
        var evtTypeId = data.readUInt16BE(0);
        this._handleEvent(evtTypeId, data.slice(2));
    }
    else {
        this.emit('warn', "Unknown control message type " + msgTypeId);
    }
};

// Handle a received event message.
NetConnection.prototype._handleEvent = function(evtTypeId, data) {
    var msgStrmId, netStream;
    switch (evtTypeId) {
        // Stream Begin
        case 0:
            if (data.length !== 4)
                return this.emit('warn', "Invalid Stream Begin payload");

            this._ensureStream(data.readUInt32BE(0));
            break;

        // Stream EOF
        case 1:
            if (data.length !== 4)
                return this.emit('warn', "Invalid Stream EOF payload");

            msgStrmId = data.readUInt32BE(0);
            netStream = this.netStreams[msgStrmId];
            if (!netStream)
                return this.emit('warn', "Stream EOF on unknown stream " + msgStrmId);

            delete this.netStreams[msgStrmId];
            netStream.emit('end');
            break;

        // Stream Dry
        case 2:
            if (data.length !== 4)
                return this.emit('warn', "Invalid Stream Dry payload");

            msgStrmId = data.readUInt32BE(0);
            netStream = this.netStreams[msgStrmId];
            if (!netStream)
                return this.emit('warn', "Stream Dry on unknown stream " + msgStrmId);

            netStream.emit('dry');
            break;

        // Stream Is Recorded
        case 4:
            if (data.length !== 4)
                return this.emit('warn', "Invalid Stream Is Recorded payload");

            msgStrmId = data.readUInt32BE(0);
            netStream = this.netStreams[msgStrmId];
            if (!netStream)
                return this.emit('warn', "Stream Is Recorded on unknown stream " + msgStrmId);

            netStream.emit('recorded');
            break;

        // Ping request
        case 6:
            if (data.length !== 4)
                return this.emit('warn', "Invalid Ping Request payload");

            this.event(7, data);
            break;

        default:
            this.emit('warn', "Unknown event type " + evtTypeId);
            break;
    }
};

// Handle a received command message.
NetConnection.prototype._handleCommand = function(name, args) {
    switch (name) {
        default:
            this.emit('warn', "Unknown NetConnection command " + name);
            break;
    }
};

// Craete or get a named NetStream.
NetConnection.prototype._ensureStream = function(msgStrmId) {
    var netStream = this.netStreams[msgStrmId];
    if (!netStream) {
        netStream = new NetStream(this, msgStrmId);
        this.netStreams[msgStrmId] = netStream;
    }
    return netStream;
};


function NetStream(netConn, msgStrmId) {
    this.netConn = netConn;
    this.msgStrmId = msgStrmId;
}
util.inherits(NetStream, EventEmitter);

NetStream.prototype._handleCommand = function(name, args) {
    switch (name) {
        case 'onStatus':
            this.status = args[1].code;
            this.emit('status', this.status);
            break;
        default:
            this.emit('warn', "Unknown NetStream command " + name);
            break;
    }
};

// Send a command message.
NetStream.prototype.call = function(name, args, cb) {
    return this.netConn._command(this.msgStrmId, name, args, cb);
};

// Call the remote play method.
NetStream.prototype.play = function(name, start, dur, reset) {
    var args = [null, name];
    [start, dur, reset].some(function(val) {
        if (val === undefined)
            return true;
        else
            args.push(val);
    });
    return this.call('play', args);
};

// Call the remote play2 method.
NetStream.prototype.play2 = function(opts) {
    return this.call('play2', [null, opts]);
};

// Call the remote receiveAudio method.
NetStream.prototype.receiveAudio = function(bool) {
    return this.call('receiveAudio', [null, !!bool]);
};

// Call the remote receiveVideo method.
NetStream.prototype.receiveVideo = function(bool) {
    return this.call('receiveVideo', [null, !!bool]);
};

// Call the remote publish method.
NetStream.prototype.publish = function(name, type) {
    return this.call('publish', [null, name, type]);
};

// Call the remote seek method.
NetStream.prototype.seek = function(ts) {
    return this.call('seek', [null, ts]);
};

// Call the remote pause method.
NetStream.prototype.pause = function(bool, ts) {
    return this.call('pause', [null, !!bool, ts]);
};

// Write an audio packet. The buffer should contain a partial FLVTAG starting
// after the StreamID field. The timestamp should be in milliseconds. The
// chunkStream parameter is optional but recommended, and used to specify a
// dedicated chunk stream.
NetStream.prototype.writeAudio = function(buf, ts, chunkStream) {
    return (chunkStream || this.netConn.mainChunkStream).write({
        msgTs: ts,
        msgTypeId: 8,
        msgStrmId: this.msgStrmId,
        data: buf
    });
};

// Write a video packet. The buffer should contain a partial FLVTAG starting
// after the StreamID field. The timestamp should be in milliseconds. The
// chunkStream parameter is optional but recommended, and used to specify a
// dedicated chunk stream.
NetStream.prototype.writeVideo = function(buf, ts, chunkStream) {
    return (chunkStream || this.netConn.mainChunkStream).write({
        msgTs: ts,
        msgTypeId: 9,
        msgStrmId: this.msgStrmId,
        data: buf
    });
};


exports.NetConnection = NetConnection;
exports.NetStream = NetStream;

exports.connect = function(port, host, options) {
    var chunkConn = chunking.connect(port, host, options);
    return new NetConnection(chunkConn);
};

// audioCodecs bits
exports.SUPPORT_SND_NONE    = 0x0001;
exports.SUPPORT_SND_ADPCM   = 0x0002;
exports.SUPPORT_SND_MP3     = 0x0004;
exports.SUPPORT_SND_INTEL   = 0x0008;
exports.SUPPORT_SND_UNUSED  = 0x0010;
exports.SUPPORT_SND_NELLY8  = 0x0020;
exports.SUPPORT_SND_NELLY   = 0x0040;
exports.SUPPORT_SND_G711A   = 0x0080;
exports.SUPPORT_SND_G711U   = 0x0100;
exports.SUPPORT_SND_NELLY16 = 0x0200;
exports.SUPPORT_SND_AAC     = 0x0400;
exports.SUPPORT_SND_SPEEX   = 0x0800;
exports.SUPPORT_SND_ALL     = 0x0FFF;

// videoCodecs bits
exports.SUPPORT_VID_UNUSED    = 0x0001;
exports.SUPPORT_VID_JPEG      = 0x0002;
exports.SUPPORT_VID_SORENSON  = 0x0004;
exports.SUPPORT_VID_HOMEBREW  = 0x0008;
exports.SUPPORT_VID_VP6       = 0x0010;
exports.SUPPORT_VID_VP6ALPHA  = 0x0020;
exports.SUPPORT_VID_HOMEBREWV = 0x0040;
exports.SUPPORT_VID_H264      = 0x0080;
exports.SUPPORT_VID_ALL       = 0x00FF;

// videoFunction bits
exports.SUPPORT_VID_CLIENT_SEEK = 1;

// objectEncoding values
exports.AMF0 = 0;
exports.AMF3 = 3;
