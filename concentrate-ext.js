var util = require('util');
var int24 = require('int24');
var Concentrate = require('concentrate');

function ConcentrateExt(options) {
    if (!(this instanceof ConcentrateExt))
        return new ConcentrateExt(options);

    Concentrate.call(this, options);
}
util.inherits(ConcentrateExt, Concentrate);

ConcentrateExt.prototype.uint24be = function(val) {
    var b = new Buffer(3);
    int24.writeUInt24BE(b, 0, val);
    return this.buffer(b);
};

ConcentrateExt.prototype._amf0Utf8 = function(str) {
    return this.uint16be(Buffer.byteLength(str)).string(str);
};

ConcentrateExt.prototype._amf0Utf8Long = function(str) {
    return this.uint32be(Buffer.byteLength(str)).string(str);
};

ConcentrateExt.prototype.amf0 = function(val) {
    switch (typeof(val)) {
        case 'number':
            this.uint8(0x00).doublebe(val);
            break;
        case 'boolean':
            this.uint8(0x01).uint8(val ? 1 : 0);
            break;
        case 'string':
            if (val.length > 0xffff)
                this.uint8(0x0c)._amf0Utf8Long(val);
            else
                this.uint8(0x02)._amf0Utf8(val);
            break;
        case 'object':
            if (val === null) {
                this.uint8(0x05);
            }
            else if (Array.isArray(val)) {
                this.uint8(0x0a).uint32be(val.length);
                val.forEach(function(el) {
                    this.amf0(el);
                }, this);
            }
            else if (val instanceof Date) {
                this.uint8(0x0b).doublebe(+val).uint16be(0);
            }
            else {
                this.uint8(0x03);
                Object.keys(val).forEach(function(key) {
                    this._amf0Utf8(key).amf0(val[key]);
                }, this);
                this._amf0Utf8('').uint8(0x09);
            }
            break;
        case 'undefined':
            this.uint8(0x06);
            break;
    }
    return this;
};

module.exports = ConcentrateExt;
