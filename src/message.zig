const std = @import("std");
const Print = std.debug.print;

const ErrorCommand = error{
    NotFound,
    Invalid,
};

const CommandType = enum {
    subscribe,
    unsubscribe,
};

const Command = union(CommandType) {
    subscribe: struct {
        topic: []const u8,
        subscriber: []const u8,
    },
    unsubscribe: struct {
        topic: []const u8,
        subscriber: []const u8,
    },
};

pub fn parseCommand(line: []const u8) anyerror!Command {
    var it = std.mem.tokenizeAny(u8, line, " ");
    const command = it.next() orelse return ErrorCommand.Invalid;
    const cmdType = std.meta.stringToEnum(CommandType, command) orelse return ErrorCommand.NotFound;
    switch (cmdType) {
        .subscribe => {
            return Command{ .subscribe = .{
                .topic = it.next() orelse return ErrorCommand.Invalid,
                .subscriber = it.next() orelse return ErrorCommand.Invalid,
            } };
        },
        .unsubscribe => {
            return Command{ .unsubscribe = .{
                .topic = it.next() orelse return ErrorCommand.Invalid,
                .subscriber = it.next() orelse return ErrorCommand.Invalid,
            } };
        },
    }
}
