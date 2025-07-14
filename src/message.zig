const std = @import("std");
const print = std.debug.print;

const MsgError = error{
    UnkownMsgType,
    InvalidInputCommand,
};

const MsgType = enum {
    subscribe,
    unsubscribe,
    publish,
};

const Message = union(MsgType) { subscribe: struct {
    topic: []const u8,
}, unsubscribe: struct {
    topic: []const u8,
}, publish: struct {
    topic: []const u8,
    message: []const u8,
} };

pub fn parseMessage(message: []const u8) !Message {
    var it = std.mem.tokenizeAny(u8, message, " ");
    var buf: [13]u8 = undefined;
    const raw = it.next() orelse return MsgError.InvalidInputCommand;
    const lowered = std.ascii.lowerString(&buf, raw);
    const command = std.meta.stringToEnum(MsgType, lowered) orelse return MsgError.UnkownMsgType;
    switch (command) {
        .publish => {
            const msg = Message{ .publish = .{
                .topic = it.next() orelse return MsgError.InvalidInputCommand,
                .message = it.rest(),
            } };
            return msg;
        },
        .subscribe => {
            const msg = Message{ .subscribe = .{
                .topic = it.rest(),
            } };
            return msg;
        },
        .unsubscribe => {
            const msg = Message{ .unsubscribe = .{
                .topic = it.rest(),
            } };
            return msg;
        },
    }
}
