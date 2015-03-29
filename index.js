var _ = require('underscore');
var int24 = require('int24');
var urllib = require('url');
var rtmp = require('./rtmp');

module.exports = function(app) {
    // Set config defaults for `root:p1-rtmp-plugins` before init.
    app.on('preInit', function() {
        var settings = app.cfg['root:p1-rtmp-plugins'] ||
            (app.cfg['root:p1-rtmp-plugins'] = {});
        _.defaults(settings, {
            type: 'root:p1-rtmp-plugins',
            streamIds: [],
        });
    });

    // Define the plugin root type.
    app.store.onCreate('root:p1-rtmp-plugins', function(obj) {
        obj.resolveAll('streams');
    });

    // Implement RTMP streamtype.
    app.store.onCreate('stream:p1-rtmp-plugins:rtmp', function(obj) {
        var netConn, netStream, audioChkStrm, videoChkStrm, listener;
        var startTs = 0;
        var eventHandlers = {
            audioHeaders: onAudioHeaders,
            audioFrame: onAudioFrame,
            videoHeaders: onVideoHeaders,
            videoFrame: onVideoFrame
        };
        var listenerOptions = {
            emitInitHeaders: true
        };

        obj.activation('connection', {
            start: function() {
                var url = urllib.parse(obj.cfg.url);
                if (url.protocol !== 'rtmp:')
                    return obj.fatal("Protocol '%s' is not supported", url.protocol);

                obj._log.info('Connecting to %s', url.hostname);
                netConn = rtmp.connect(url.port || 1935, url.hostname);
                netConn.on('warn', onConnWarn);
                netConn.on('error', onConnError);
                netConn.on('end', onConnEnd);
                netConn.connect({
                    app: url.pathname.slice(1)
                }, function(err, props, info) {
                    if (err)
                        return obj.fatal(err, "`connect` command failed");

                    obj._log.info('Connected, sending publish request');
                    netConn.createStream(function(err, info, stream) {
                        if (err)
                            return obj.fatal(err, "`createStream` command failed");

                        netStream = stream;
                        audioChkStrm = netConn.createChunkStream();
                        videoChkStrm = netConn.createChunkStream();

                        stream.on('status', function(stat) {
                            var started = stat === 'NetStream.Publish.Start';
                            if (started && !listener) {
                                obj._log.info('Stream started publishing');
                                listener = obj._mixer.addFrameListener(
                                    eventHandlers, listenerOptions);
                            }
                            else {
                                obj._log.info('Stream stopped publishing');
                                listener();
                                listener = null;
                            }
                        });
                        stream.publish(obj.cfg.streamName || '', 'live');
                    });
                });
            },
            stop: function() {
                if (listener) {
                    listener();
                    listener = null;
                }

                netConn.end();
                netConn = netStream = audioChkStrm = videoChkStrm = null;

                startTs = 0;
            }
        });

        function onConnWarn(s) {
            obj._log.warn(s);
        }

        function onConnError(err) {
            obj.fatal(err, "Connection failure");
        }

        function onConnEnd() {
            if (netConn)
                obj.fatal("Unexpectedly lost connection");
        }

        function onAudioHeaders(headers) {
            var b = new Buffer(2);
            // Audio Tag Header
            b[0] = 0xaf;  // AAC, 44.1kHz, 16-bit, Stereo
            b[1] = 0;  // AAC sequence header follows
            // Send the message.
            b = Buffer.concat([b, headers.buf]);
            netStream.writeAudio(b, 0, audioChkStrm);
        }

        function onAudioFrame(frame) {
            var b = new Buffer(2);
            // Audio Tag Header
            b[0] = 0xaf;  // AAC, 44.1kHz, 16-bit, Stereo
            b[1] = 1;  // AAC raw data follows
            // Send the message.
            b = Buffer.concat([b, frame.buf]);
            var t = getRtmpTime(frame.pts);
            netStream.writeAudio(b, t, audioChkStrm);
        }

        function onVideoHeaders(headers) {
            var b = new Buffer(5);
            // Video Tag Header
            b[0] = 0x17;  // Keyframe, AVC
            b[1] = 0;  // AVC sequence header follows
            int24.writeInt24BE(b, 2, 0);  // Composition time
            // Send the message.
            b = Buffer.concat([b, headers.avc]);
            netStream.writeVideo(b, 0, videoChkStrm);
        }

        function onVideoFrame(frame) {
            var b = new Buffer(5);
            // Video Tag Header
            b[0] = (frame.keyframe ? 0x10 : 0x20) | 0x07;  // Keyframe / IDR, AVC
            b[1] = 1;  // AVC NALUs follow
            var ct = ~~((frame.pts - frame.dts) / 1000000);
            int24.writeInt24BE(b, 2, ct);  // Composition time
            // Send the message.
            b = Buffer.concat([b, frame.buf]);
            var t = getRtmpTime(frame.dts);
            netStream.writeVideo(b, t, videoChkStrm);
        }

        function getRtmpTime(ts) {
            ts = ~~(ts / 1000000);
            if (!startTs)
                startTs = ts - 5000;  // Some room for variations
            return ts - startTs;
        }
    });
};
