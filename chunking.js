var net = require('net');
var util = require('util');
var crypto = require('crypto');
var stream = require('stream');
var DissolveExt = require('./dissolve-ext');
var ConcentrateExt = require('./concentrate-ext');


// Represents a connection that multiplexes several chunk streams. This is a
// readable stream which emits demultiplexed messages as they arrive.
function ChunkConnection(readable, writable, options) {
    var self = this;

    if (!options) options = {};
    options.objectMode = true;

    stream.Readable.call(self, options);

    self.readable = readable;
    self.readable.on('error', onError);

    self.writable = writable || readable;
    if (self.writable !== self.readable)
        self.writeable.on('error', onError);

    function onError(err) {
        self.emit('error', err);
    }

    self.parser = DissolveExt();
    self.parser.on('readable', function() {
        self._readFlush();
    });
    self.parser.on('end', function() {
        self.push(null);
        self.parser = self.readable = null;
    });
    self.readable.pipe(self.parser);

    self.builder = ConcentrateExt();
    self.on('finish', function() {
        self.builder.end();
        self.builder = self.writable = null;

        Object.keys(self.streams).forEach(function(chkStrmId) {
            self.streams[chkStrmId].end();
        });
    });
    self.builder.pipe(self.writable);

    self._readPending = false;
    self.handshakeComplete = false;
    self.ended = false;

    self.epoch = options.epoch || 0;
    self.streams = Object.create(null);
    self.peerEpoch = 0;
    self.peerChunkSize = 128;
    self.peerStreams = Object.create(null);

    self._handshake();
    self._loop();
}
util.inherits(ChunkConnection, stream.Readable);

// Send a control message.
ChunkConnection.prototype.control = function(msgTypeId, data) {
    return this._write({
        chkStrmId: 2,
        msgTs: 0,
        msgTypeId: msgTypeId,
        msgStrmId: 0,
        data: data
    });
};

// Create a chunk stream.
ChunkConnection.prototype.createStream = function(options) {
    var streams = this.streams;

    var id = 3;
    while (streams[id])
        id++;

    var stream = new ChunkStream(this, id, options);
    streams[id] = stream;
    return stream;
};

// End the connection.
ChunkConnection.prototype.end = function() {
    if (!this.ended) {
        this.ended = true;
        this.emit('finish');
    }
};

// Readable implementation.
ChunkConnection.prototype._read = function() {
    this._readPending = true;
    this._readFlush();
};

ChunkConnection.prototype._readFlush = function() {
    while (this._readPending) {
        var msg = this.parser.read();
        if (!msg)
            break;
        if (!this._handleControl(msg))
            this._readPending = this.push(msg);
    }
};

// Internal write methods.
ChunkConnection.prototype._write = function(msg) {
    var self = this;

    if (self.handshakeComplete)
        return self._actualWrite(msg);

    self.once('handshake', function() {
        self._actualWrite(msg);
    });
};

ChunkConnection.prototype._actualWrite = function(msg) {
    var self = this;

    // Basic header
    // FIXME: We always send a Type 0 header now.
    var id = msg.chkStrmId;
    if (id < 64) {
        self.builder
            .uint8(id);
    }
    else if (id < 320) {
        self.builder
            .uint8(0)
            .uint8(id - 64);
    }
    else {
        id -= 64;
        self.builder
            .uint8(1)
            .uint8(id % 256)
            .uint8(~~(id / 256));
    }

    // Message header
    var needExtTs = msg.msgTs > 0xffffff;
    self.builder
        .uint24be(needExtTs ? 0xffffff : msg.msgTs)
        .uint24be(msg.data.length)
        .uint8(msg.msgTypeId)
        .uint32le(msg.msgStrmId);

    // Extended timestamp
    if (needExtTs)
        self.builder.uint32be(msg.msgTs);

    // Chunk data
    // FIXME: Implement chunking. We now just maximize chunk size directly
    // after the handshake, so we can get away with this.
    self.builder.buffer(msg.data);

    // Send
    self.builder.flush();
};

// Helper for bailing on the connection.
ChunkConnection.prototype._bail = function(reason) {
    this.parser.jobs.length = 0;
    this.builder.jobs.length = 0;

    this.emit('error', new Error(reason));
    this.end();

    this.parser = this.builder = this.readable = this.writable = null;
};

// Handshake with the peer.
ChunkConnection.prototype._handshake = function() {
    var self = this;

    // This doesn't really need to be very random, so this is what we do.
    var random = crypto.randomBytes(191);
    random = Buffer.concat([
        random, random, random, random,
        random, random, random, random
    ], 1528);

    // The comment terminology here is client/server, but the protocol is
    // symmetrical. So var names talk about a generic 'peer'.
    self.builder
        // C0.
        .uint8(3)  // Version
        // C1.
        .uint32be(this.epoch)  // Time
        .uint32be(0)  // Zero
        .buffer(random)  // Random
        // Send
        .flush();

    self.parser
        // S0
        .uint8('version')
        // S1
        .uint32be('time')
        .uint32be('zero')
        .buffer('random', 1528)
        // Handle
        .tap(function() {
            if (this.vars.version !== 3)
                return self._bail("Unknown peer version");

            self.peerEpoch = this.vars.time;

            self.builder
                // C2
                .uint32be(self.peerEpoch)  // Time
                .uint32be(0)  // Time2
                .buffer(this.vars.random)  // Random echo
                // Send
                .flush();

            this.vars = {};
        })
        // S2
        .uint32be('time')
        .uint32be('time2')
        .buffer('echo', 1528)
        // Handle
        .tap(function() {
            if (this.vars.time !== self.epoch)
                return self._bail("Invalid peer epoch echo");
            if (this.vars.echo.compare(random) !== 0)
                return self._bail("Invalid peer random echo");

            this.vars = {};

            // Because we don't do proper bandwidth management, we might
            // as well maximize the chunk size. FIXME
            self._actualWrite({
                chkStrmId: 2,
                msgTs: 0,
                msgTypeId: 1,
                msgStrmId: 0,
                data: new Buffer([0x00, 0xff, 0xff, 0xff])
            });

            self.handshakeComplete = true;
            self.emit('handshake');
        });
};

// Parser loop, after the handshake.
ChunkConnection.prototype._loop = function() {
    var self = this;

    self.parser.loop(function() {
        this
            // Basic header
            .uint8('basic1')
            .tap(function() {
                this.vars.msgHdrFmt = (this.vars.basic1 & 0xc) >> 6;
                this.vars.chkStrmIdFmt = (this.vars.basic1 & 0x3f);
                if (this.vars.chkStrmIdFmt < 2) {
                    this.uint8('basic2');
                    if (this.vars.chkStrmIdFmt === 1)
                        this.uint8('basic3');
                }
            })
            .tap(function() {
                if (this.vars.chkStrmIdFmt < 2) {
                    var id = this.vars.basic2 + 64;
                    if (this.vars.chkStrmIdFmt === 1)
                        id += this.vars.basic3 * 256;
                    this.vars.chkStrmId = id;
                }
                else {
                    this.vars.chkStrmId = this.vars.chkStrmIdFmt;
                }

                var cs = self.peerStreams[this.vars.chkStrmId];
                if (!cs) {
                    if (this.vars.msgHdrFmt !== 0)
                        return self._bail("Expected Type 0 message header at start of chunk stream");
                    cs = self.peerStreams[this.vars.chkStrmId] = { curMsgChunks: [] };
                }
                this.vars.cs = cs;
            })
            // Message header
            .tap(function() {
                if (this.vars.msgHdrFmt === 0) {
                    this
                        .uint24be('msgTs')
                        .uint24be('msgLen')
                        .uint8('msgTypeId')
                        .uint32le('msgStrmId');
                }
                else if (this.vars.msgHdrFmt === 1) {
                    this
                        .uint24be('msgTsDiff')
                        .uint24be('msgLen')
                        .uint8('msgTypeId');
                }
                else if (this.vars.msgHdrFmt === 2) {
                    this
                        .uint24be('msgTsDiff');
                }
            })
            // Extended timestamp
            .tap(function() {
                if (this.vars.msgTs !== undefined) {
                    if (this.vars.msgTs === 0xffffff)
                        this.uint32be('msgTs');
                    return;
                }

                var val = this.vars.msgTsDiff;
                if (val === undefined)
                    val = this.vars.cs.lastMsgTsVal;
                if (val === 0xffffff)
                    this.uint32be('msgTsDiff');
            })
            // Interpolate missing values
            .tap(function() {
                var cs = this.vars.cs;

                if (this.vars.msgTs === undefined) {
                    if (this.vars.msgTsDiff === undefined)
                        this.vars.msgTsDiff = cs.lastMsgTsVal;
                    else
                        cs.lastMsgTsVal = this.vars.msgTsDiff;
                    this.vars.msgTs = cs.lastMsgTs + this.vars.msgTsDiff;
                }
                else {
                    cs.lastMsgTsVal = this.vars.msgTs;
                }
                cs.lastMsgTs = this.vars.msgTs;

                if (cs.curMsgPending) {
                    this.vars.msgLen = cs.curMsgLen;
                    this.vars.msgTypeId = cs.curMsgTypeId;
                    this.vars.msgStrmId = cs.curMsgStrmId;
                }
                else {
                    cs.curMsgLen = cs.curMsgPending = this.vars.msgLen;
                    cs.curMsgTypeId = this.vars.msgTypeId;
                    cs.curMsgStrmId = this.vars.msgStrmId || cs.curMsgStrmId;
                }

                this.vars.chunkSize = self.peerChunkSize;
                if (this.vars.chunkSize > cs.curMsgPending)
                    this.vars.chunkSize = cs.curMsgPending;
                cs.curMsgPending -= this.vars.chunkSize;

                // Chunk data
                this.buffer('data', this.vars.chunkSize);
            })
            // Handle chunk
            .tap(function() {
                var cs = this.vars.cs;
                cs.curMsgChunks.push(this.vars.data);
                if (!cs.curMsgPending) {
                    var msg = {
                        chkStrmId: this.vars.chkStrmId,
                        msgTs: this.vars.msgTs,
                        msgStrmId: this.vars.msgStrmId,
                        msgTypeId: this.vars.msgTypeId,
                        data: Buffer.concat(cs.curMsgChunks, cs.curMsgLen)
                    };
                    cs.curMsgChunks.length = 0;
                    this.push(msg);
                }
            });
    });
};

// Try to handle a control message. If not one of the protocol control
// messages, returns false to handle at the upper layer.
ChunkConnection.prototype._handleControl = function(msg) {
    if (msg.chkStrmId !== 2 || msg.msgStrmId !== 0)
        return false;

    switch (msg.msgTypeId) {
        // Set Chunk Size
        case 1:
            if (msg.data.length !== 4 || (msg.data[0] & 0x80))
                return this._bail("Invalid Set Chunk Size control payload");
            this.peerChunkSize = msg.data.readUInt32BE(0);
            if (this.peerChunkSize > 0xffffff)
                this.peerChunkSize = 0xffffff;
            return true;

        // Abort Message
        case 2:
            if (msg.data.length !== 4)
                return this._bail("Invalid Abort Message control payload");
            var csId = msg.data.readUInt32BE(0);
            var cs = this.peerStreams[csId];
            if (cs) {
                cs.curMsgPending = 0;
                cs.curMsgChunks.length = 0;
            }
            return true;

        // Acknowledgement
        case 3:
            this.emit('warn', "Not implemented: Acknowledgement");  // FIXME
            return true;

        // Window Acknowledgement Size
        case 5:
            this.emit('warn', "Not implemented: Window Acknowledgement Size");  // FIXME
            return true;

        // Set Peer Bandwidth
        case 6:
            this.emit('warn', "Not implemented: Set Peer Bandwidth");  // FIXME
            return true;

        default:
            return false;
    }
};


// A writable stream that represents a single chunk stream on a connection.
function ChunkStream(chkConn, chkStrmId, options) {
    if (!options) options = {};
    options.objectMode = true;

    stream.Writable.call(this, options);

    this.chkConn = chkConn;
    this.chkStrmId = chkStrmId;

    this.on('finish', function() {
        delete chkConn.streams[chkStrmId];
    });
}
util.inherits(ChunkStream, stream.Writable);

// Writable implementation.
ChunkStream.prototype._write = function(msg, unused, callback) {
    this.chkConn._write({
        chkStrmId: this.chkStrmId,
        msgTs: msg.msgTs,
        msgTypeId: msg.msgTypeId,
        msgStrmId: msg.msgStrmId,
        data: msg.data
    });
    // FIXME: Ideally, each stream gets a priority, the window is filled with
    // high priority chunks, and this callback only gets called when all
    // chunks are sent out.
    callback();
};


exports.ChunkConnection = ChunkConnection;
exports.ChunkStream = ChunkStream;

exports.connect = function(port, host, options) {
    var s = net.createConnection(port, host);
    return new ChunkConnection(s, s, options);
};
