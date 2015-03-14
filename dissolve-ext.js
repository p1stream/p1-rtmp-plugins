var util = require('util');
var int24 = require('int24');
var Dissolve = require('dissolve');

function DissolveExt(options) {
    if (!(this instanceof DissolveExt))
        return new DissolveExt(options);

    Dissolve.call(this, options);
    this._done = null;
}
util.inherits(DissolveExt, Dissolve);

// This wonderful hack allows us to throw errors during decoding.
DissolveExt.prototype._transform = function(input, encoding, done) {
    this._done = done;
    Dissolve.prototype._transform.call(this, input, encoding, function(err, data) {
        this._done = null;
        done(err, data);
    });
};

DissolveExt.prototype._error = function(err) {
    this.jobs.length = 0;
    this._done(err);
};

DissolveExt.prototype.uint24be = function(name) {
    return this.buffer('_tmp', 3).tap(function() {
        this.vars[name] = int24.readUInt24BE(this.vars._tmp, 0);
    });
};

DissolveExt.prototype._amf0Utf8 = function(name) {
    return this.uint16be('_tmp').tap(function() {
        this.string(name, this.vars._tmp);
    });
};

DissolveExt.prototype._amf0Utf8Long = function(name) {
    return this.uint32be('_tmp').tap(function() {
        this.string(name, this.vars._tmp);
    });
};

DissolveExt.prototype.amf0 = function(name, type) {
    if (type === undefined)
        this.uint8('_tmp');
    else
        this.vars._tmp = type;

    // We take extra care to set the actual var at the very end, because
    // we need to be reentrant and reuse _tmp a lot.
    return this.tap(function() {
        switch (this.vars._tmp) {
            case 0x00:  // Number
                this.doublebe(name);
                break;
            case 0x01:  // Boolean
                this.uint8(name).tap(function() {
                    this.vars[name] = !!this.vars[name];
                });
                break;
            case 0x02:  // String
                this._amf0Utf8(name);
                break;
            case 0x0c:  // Long string
                this._amf0Utf8Long(name);
                break;
            case 0x05:  // Null
                this.vars[name] = null;
                break;
            case 0x0a:  // Array
                this.uin32be('_tmp').tap(function() {
                    var num = this.vars._tmp;
                    var arr = [];
                    function pushTmp() {
                        arr.push(this.vars._tmp);
                    }
                    while (num--)
                        this.amf0('_tmp').tap(pushTmp);
                    this.tap(function() {
                        this.vars[name] = arr;
                    });
                });
                break;
            case 0x0b:  // Date
                this.doublebe('_tmp').uint16be('_tmp2').tap(function() {
                    this.vars[name] = new Date(this.vars._tmp);
                });
                break;
            case 0x03:  // Object
                var obj = {};
                this.loop(function(end) {
                    this._amf0Utf8('_tmp').uint8('_tmp2').tap(function() {
                        if (this.vars._tmp2 == 0x09) {
                            this.vars[name] = obj;
                            return end();
                        }
                        var key = this.vars._tmp;
                        this.amf0('_tmp', this.vars._tmp2).tap(function() {
                            obj[key] = this.vars._tmp;
                        });
                    });
                });
                break;
            case 0x06:  // Undefined
                this.vars[name] = undefined;
                break;
            default:
                this._error(new TypeError("Unsupported AMF0 type " + this.vars._tmp));
        }
    });
};

module.exports = DissolveExt;
